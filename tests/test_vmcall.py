from vmcall.vmcall import VMExecutor, CommandSendingServer
from vmcall.vmserve import VMSlave
import tempfile
import shutil
import unittest
import threading
from unittest import mock
import pytest


@mock.patch('vmcall.vmcall.CommandSendingServer')
@mock.patch('subprocess.Popen')
def test_vm_executor(popen, server):
    popen.return_value.communicate.return_value = (b"out", b"err")
    popen.return_value.returncode = 1
    with pytest.raises(RuntimeError):
        with VMExecutor(['qemu'], "req", "res"):
            pass

    popen.return_value.returncode = 0
    with VMExecutor(['qemu'], "req", "res") as executor:
        assert popen.called
        executor.submit(['ls'])
        server().send_command.assert_called_with(["ls"])


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
        self.slave_thread.join()
        shutil.rmtree(self.tmp)

    def test_ls(self):
        future = self.master.send_command(['ls'])
        ret, out, err = future.result()
        assert "setup.py" in out
        assert not ret
