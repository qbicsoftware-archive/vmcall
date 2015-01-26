from __future__ import print_function

import os
import logging
import subprocess
from os.path import join as pjoin
import tempfile
import shutil
from . import vmcall

logger = logging.getLogger(__name__)


def prepare_data_image(dest, data_dir, type='ntfs', format='raw', size='+1G'):
    """ Create a disk image containing the data inside data_dir.

    Parameters
    ----------
    dest: str
        path of the image that should be created
    data_dir: str
        directory containing the data that should be copied to
        the image.
    type: str, optional
        filesystem to use in the image. Could be ntfs or vfat
    format: ['qcow2', 'raw'], optional
        format of the image file
    size: str
        either the size of the whole image or if preceded by a `+`,
        the amount of free space in the final image. The size can
        be specified by appending T, G, M or K.

    Example
    -------
    >>> image = prepare_data_image('image.raw', 'path/to/data/dir', size='+5G')

    Notes
    -----
    This uses `virt-make-fs` from guestfs. See the documentation of this
    package for further information.
    """

    if not os.path.isdir(data_dir):
        raise ValueError('Not a directory: %s' % data_dir)
    if os.path.exists(dest):
        raise ValueError('Destination file exists: %s' % dest)

    logger.info("Copying data from %s to image", data_dir)
    subprocess.check_call(
        [
            'virt-make-fs',
            '--partition',
            '--type', type,
            '--format', format,
            '--size', size,
            '--',
            data_dir,
            dest,
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    return dest


def extract_from_image(image, dest, path='/', use_tar=True):
    """ Write the contents of disk image `image` to `dest`.

    This can be used while the virtual machine is running.

    Parameters
    ----------
    image: str
        Path to the image from which the files should be extracted
    dest: str
        Path to the destination directory
    path: str, optional
        Path to a directory or file inside the image that should be
        extracted
    use_tar: bool, optional
        If True, do not copy the contents to dest, but create a
        tar file at dest, that contains the contents of path inside
        the image. This only works if path is a directory.
        In this case dest must be a filename, not a directory.
    """
    if not os.path.isfile(image):
        raise ValueError("Image does not exist: %s" % image)
    if not os.path.isdir(dest) and not use_tar:
        raise ValueError("Destination must be a directory: %s" % dest)
    if os.path.exists(dest) and use_tar:
        raise ValueError("Destination exists but output type is tar: %s" % dest)

    if use_tar:
        command = 'tar-out'
    else:
        command = 'copy-out'

    logger.info("Extracting data from image %s to %s", image, dest)
    subprocess.check_call(
        [
            'guestfish',
            '--ro',
            '-a', image,
            '-m', '/dev/sda1',
            command,
            path,
            dest,
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )


def create_overlay(dest_path, base_image):
    if os.path.exists(dest_path):
        raise ValueError("Destination path exists: %s" % dest_path)
    if not os.path.isfile(base_image):
        raise ValueError("Base image file not found: %s" % base_image)

    subprocess.check_call(
        [
            'qemu-img',
            'create',
            '-b', base_image,
            '-f', 'qcow2',
            dest_path
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return dest_path


class VMBuilder:
    """ Configure and start a virtual machine.

    Parameters
    ----------
    qemu_bin: str
        Name or path of the qemu binary. Usually this would be
        'qemu-system-x86_64'
    root_base_image: str
        Path to a read-only image of a boot partitition
    workdir: str
        Directory where to store temporary images and temporary files
    keep_images: bool, optional
        Keep all temporary images after shutdown.

    Examples
    --------
    >>> with VMBuilder('qemu-system-x86_64', 'root.raw', '/tmp') as vm:
    >>>     vm.add_diskimg('input', ['file.raw'], size="+10G")
    >>>     vm.add_diskimg('output', size="30G")
    >>>     vm.add_option('cpu', 'host')
    >>>     vm.add_option('smp', sockets=1, cores=10, threads=2)
    >>>     with vm.executor() as executor:
    >>>         future = executor.submit(["msconvert", "{input}/file.RAW]",
    >>>                                   "-o", "{output}"])
    >>>         retcode, out, err = future.result()
    >>>         executor.submit(['shutdown', '/t', '3']).wait()
    >>>     vm.copy_out('output', 'output.tar')
    """
    def __init__(self, qemu_bin, root_base_image, workdir, keep_images=False):
        if not os.path.isdir(workdir):
            raise ValueError("Workdir does not exist: %s" % workdir)
        self._workdir = workdir

        image_path = pjoin(workdir, 'root_image.ovl')
        self._root_image = create_overlay(image_path, root_base_image)
        self._options = [('drive', [], {'file': self._root_image})]
        self._images = {}

        self._qemu_bin = qemu_bin
        self._interrupted = False

        self._keep_images = keep_images

    def add_diskimg(self, name, data_files=None, data_path=None,
                    type='ntfs', format='raw', size='+500M', *args, **options):
        """ Create a new disk image and attach it to the VM.

        Parameters
        ----------
        name: str
            Name of the disk image
        data_files: list of file paths
            The list of files to copy to the disk image
        data_path: str
            Path to a directory. The content will be copied to the disk.
            You can use only one of `data_path` and `data_files`.
        type: str
            Filesystem format
        format: ['raw', 'qcow2']
            Format of the disk image
        size: str
            Size of the image
        options: dict
            List of options that are passed to qemu about disk. See section
            `drive` in qemu documentation.
        """
        if data_files is not None and data_path is not None:
            raise ValueError(
                "You can use only one of 'data_path' and 'data_files'"
            )

        path = pjoin(self._workdir, "%s.%s" % (name, format))

        if data_files is None and data_path is None:
            data_files = []

        # the root image is the first one and is not in this list
        if len(self._images) > 3:
            raise ValueError("qemu does not support more than 4 disks")

        if name in self._images:
            raise ValueError("Name of image is not unique: %s" % name)

        if data_path is not None:
            image = prepare_data_image(path, data_path, type, format, size)
        else:
            if data_files is None:
                data_files = []

            datadir = tempfile.mkdtemp()
            try:
                for file in data_files:
                    shutil.copy(file, pjoin(datadir, os.path.split(file)[1]))

                image = prepare_data_image(path, datadir, type, format, size)
            finally:
                shutil.rmtree(datadir)

        if options is None:
            options = {}

        options['file'] = image
        self._options.append(('drive', args, options))
        self._images[name] = image

    def add_option(self, name, *args, **kwargs):
        """ Add a command line argument to the qemu command line.

        For a list of options, see `man qemu`. Underscores in
        the options will be ignored, so that you can use `if_` instead
        of `if` to specify an interface.
        """
        self._options.append((name, args, kwargs))

    def _add_socket(self, socket_file, host_ip, port, id_):
        """ Add socket forwarding to the command line.

        This socket is used to send commands to the vm and to get back
        status codes.
        """

        self.add_option('chardev', 'socket', path=socket_file, id=id_)
        guestfwd = "tcp:{}:{}-chardev:{}".format(host_ip, port, id_)
        self.add_option('net', 'user', guestfwd=guestfwd, restrict='on')

    def _add_command_sockets(self):
        self.add_option('net', 'nic')
        self._request_path = pjoin(self._workdir, 'request-socket')
        self._response_path = pjoin(self._workdir, 'response-socket')
        self._add_socket(self._request_path, "10.0.2.2", 8000, 'req-socket')
        self._add_socket(self._response_path, "10.0.2.2", 8001, 'res-socket')

    def copy_out(self, image, dest, path='/'):
        """ Extract files from image. """
        if image not in self._images:
            raise ValueError("Could not find disk image: %s" % image)
        extract_from_image(self._images[image], dest, path)

    def _build_command(self):
        """ Build the qemu command from previous `add_diskimg` and co calls."""

        cmd = [self._qemu_bin]
        for name, args, kwargs in self._options:
            cmd.append('-' + name)
            # remove underscores in options so that we can pass the option
            # 'if' as 'if_'. Otherwise this would be a syntax error
            args = ','.join([s.rstrip('_') for s in args])
            for key, val in kwargs.items():
                left = str(key).rstrip('_').replace('_', '-')
                right = str(val).rstrip('_')
                if args:
                    args = ','.join([args, '='.join([left, right])])
                else:
                    args = '='.join([left, right])
            if args:
                cmd.append(args)

        return [item.rstrip('_') for item in cmd]

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if not self._keep_images:
            shutil.rmtree(self._workdir)
        return False

    def executor(self):
        """ Start the machine and return a `VMExecutor`. """
        self._add_command_sockets()
        return vmcall.VMExecutor(self._build_command(), self._request_path,
                                 self._response_path)
