from vmcall.qemu import (
    prepare_data_image, extract_from_image, create_overlay, VMBuilder
)
from os.path import join as pjoin
from os.path import exists as pexists
import os
import pytest
import tempfile
import unittest
import subprocess
import shutil


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


class TestVMBuilder(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.tmp):
            shutil.rmtree(self.tmp)

    def test_init(self):
        with pytest.raises(ValueError):
            VMBuilder("qemu-system-x86_64", "root.raw", self.tmp)
        image = os.path.join(self.tmp, "root.raw")
        with open(image, 'w'):
            pass
        with VMBuilder("qemu-system-x86_64", image, self.tmp) as vm:
            command = vm._build_command()
            assert command[0] == "qemu-system-x86_64"
            print(command)
            assert any(self.tmp in s for s in command[1:])
            assert '-drive' in command
            vm.add_diskimg("input", size="5M")
            assert any("input.raw" in s for s in vm._build_command())
            vm.add_option('cpu', 'host')
            vm._add_command_sockets()
            print(vm._build_command())
            assert any("guestfwd" in s for s in vm._build_command())


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
