from vmcall.vmcall import *
from vmcall.vmserve import *
from vmcall.qemu import *
import tempfile
import os
from os.path import join as pjoin
from os.path import exists as pexists
import shutil
import subprocess
import pytest
import unittest


class TestDataImage(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def _prepare_data(self):
        data = pjoin(self.tmpdir, 'data')
        os.mkdir(data)
        with open(pjoin(data, 'testfile'), 'w') as f:
            f.write('hi')
        return data

    def test_exists(self):
        image = pjoin(self.tmpdir, 'image.img')
        data = self._prepare_data()
        prepare_data_image(image, data, size="20M")
        assert pexists(image)

    def test_invalid_type(self):
        image = pjoin(self.tmpdir, 'image.img')
        data = self._prepare_data()
        with pytest.raises(subprocess.CalledProcessError):
            prepare_data_image(image, data, type="no_such_fs_type")

    def test_invalid_size(self):
        image = pjoin(self.tmpdir, 'image.img')
        data = self._prepare_data()
        with pytest.raises(subprocess.CalledProcessError):
            prepare_data_image(image, data, size="5gb")

    def test_read_dir(self):
        image = pjoin(self.tmpdir, 'image.img')
        data = self._prepare_data()
        prepare_data_image(image, data, size="20M")
        outdir = pjoin(self.tmpdir, 'outdir')
        os.mkdir(outdir)
        extract_from_image(image, outdir, use_tar=False)
        with open(pjoin(outdir, 'testfile')) as f:
            assert f.read() == 'hi'

    def test_read_file(self):
        image = pjoin(self.tmpdir, 'image.img')
        data = self._prepare_data()
        prepare_data_image(image, data, size="20M")
        outdir = pjoin(self.tmpdir, 'outdir')
        os.mkdir(outdir)
        extract_from_image(image, outdir, path='/testfile', use_tar=False)
        with open(pjoin(outdir, 'testfile')) as f:
            assert f.read() == 'hi'

    def test_tar_out(self):
        image = pjoin(self.tmpdir, 'image.img')
        data = self._prepare_data()
        prepare_data_image(image, data, size="20M")
        outdir = pjoin(self.tmpdir, 'outdir')
        os.mkdir(outdir)
        outfile = pjoin(outdir, "out.tar")
        extract_from_image(image, outfile, path='/', use_tar=True)
        assert pexists(outfile)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)


class TestCreateOverlay(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.base_image = pjoin(self.tmpdir, 'base_image.img')
        self.image = pjoin(self.tmpdir, 'image.ovl')

    def _prepare_base_image(self):
        data = pjoin(self.tmpdir, 'data')
        os.mkdir(data)
        with open(pjoin(data, 'testfile'), 'w') as f:
            f.write('hi')
        image = prepare_data_image(self.base_image, data, size="50M")
        return image

    def test_exists(self):
        base_image = self._prepare_base_image()
        create_overlay(self.image, base_image)
        assert pexists(base_image)
        assert pexists(self.image)


class TestCommandSendServer(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.request_socket = os.path.join(self.tmpdir, 'request')
        self.response_socket = os.path.join(self.tmpdir, "response")
        self._backend_alive = True
        self.server = CommandSendingServer(
            "ipc://" + self.request_socket,
            "ipc://" + self.response_socket,
            lambda: self._backend_alive,
            2,
        )

        def handle():
            request_socket = self.server._context.socket(zmq.PULL)
            response_socket = self.server._context.socket(zmq.PUSH)
            response_socket.set(zmq.SNDTIMEO, 2000)
            request_socket.set(zmq.RCVTIMEO, 100)
            request_socket.connect('ipc://' + self.request_socket)
            response_socket.connect('ipc://' + self.response_socket)
            while True:
                if not self._backend_alive:
                    return
                try:
                    request = request_socket.recv_json()
                except zmq.error.Again:
                    pass
                else:
                    if request['type'] == 'setup':
                        continue
                    self.last_request = request
                    response_socket.send_json(
                        {
                            'requestID': request['requestID'],
                            'returncode': request['requestID'],
                            'out': '',
                            'err': '',
                            'type': 'commandFinished'
                        }
                    )

        self.handler = threading.Thread(target=handle)

    def tearDown(self):
        self.server.shutdown()
        self._backend_alive = False
        self.handler.join()
        shutil.rmtree(self.tmpdir)

    def test_send_command(self):
        self.handler.start()
        self.server.start()
        future = self.server.send_command(['ls'])
        assert future.result() == (0, '', '')
        future = self.server.send_command(['blubb'])
        assert future.result() == (1, '', '')
        self._backend_alive = False
        with pytest.raises(RuntimeError):
            future = self.server.send_command(['bla'])


class TestCommandReceive(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.req = "ipc://%s/req" % self.tmp
        self.res = "ipc://%s/res" % self.tmp
        self.slave = VMSlave(self.req, self.res)
        self.slave_thread = threading.Thread(
            target=self.slave.serve_till_shutdown
        )
        self.slave_thread.start()
        self.master = CommandSendingServer(
            self.req, self.res, lambda: self.slave_thread.is_alive(), 2
        )
        self.master.start()

    def tearDown(self):
        self.master.shutdown()
        self.slave.shutdown()
        self.master.wait()
        self.slave_thread.join()

    def test_ls(self):
        future = self.master.send_command(['ls'])
        ret, out, err = future.result()
        assert "setup.py" in out
        assert not ret
