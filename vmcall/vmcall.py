from __future__ import print_function

import subprocess
import logging
import threading
import zmq
import time

logger = logging.getLogger('vmcall-host')


class VMExecutor:
    """ Start the virtual machine and forward jobs to it.

    `VMExecutor` loosly follows the interface of `concurrent.futures`.

    Parameters
    ----------
    qemu_command: list
        The command that is used to start the vm, as used by subprocess
    request_path: str
        A zeromq path (e.g. "ipc:///tmp/request"). A socket will be created
        before the vm starts and jobs will be submitted to it.
    request_path: str
        A zeromq path. Like `request_path` but for the responses and logging
        of the vm.
    """
    def __init__(self, qemu_command, request_path, response_path):
        self._request_path = request_path
        self._response_path = response_path
        self._command = qemu_command
        self._interrupted = False

    def _start(self):
        """ Start the virtual machine.

        If the VMExecutor._interrupted flag is set, the vm will be killed.
        """

        logger.info("Starting the command server")

        def backend_alive():
            # do not stop the command server before the vm is started
            if not hasattr(self, '_vm_popen'):
                return True
            return self._vm_popen.poll() is None

        self._command_server = CommandSendingServer(
            self._request_path,
            self._response_path,
            backend_alive,
            2,
        )
        logger.info("Starting virtual machine")
        self._vm_popen = subprocess.Popen(
            self._command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        time.sleep(1)
        if self._vm_popen.poll() is not None:
            out, err = self._vm_popen.communicate()
            print(out.decode())
            print(err.decode())
            raise RuntimeError("unexpected qemu exit")

        self._command_server.send_setup()
        self._command_server.start()

    def shutdown(self, force=False):
        if not hasattr(self, '_vm_popen'):
            raise ValueError(
                "VMExecutor.shutdown was called, but vm is not running"
            )

        self._command_server.shutdown()

        if force:
            logger.warn("Interrupting the virtual machine")
            self._vm_popen.kill()

        out, err = self._vm_popen.communicate()

        if self._vm_popen.returncode:
            logger.error("qemu retured non-zero exit code %s",
                         self._vm_popen.returncode)
            if out:
                logger.warn('Output of qemu was: %s', out)
            if err:
                logger.warn('Stderr of qemu was: %s', err)
            raise RuntimeError("qemu failed")

    def submit(self, command):
        """ Submit the command for execution on the vm.

        `command` can contain the name of disks in a format string::

            "msconvert {input}/file.RAW" -o {output} --mzML"
        """
        return self._command_server.send_command(command)

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, *args, **kwargs):
        self.shutdown()
        return False


class VMFuture:
    def __init__(self, command_server, command):
        self._server = command_server
        self.command = command
        self._out = None

    def done(self):
        return self._out is not None

    def result(self):
        self.wait()
        exception = self._out.get('exception', None)
        if exception:
            raise RuntimeError("Remote exception raised: %s" % exception)
        return self._out['returncode'], self._out['out'], self._out['err']

    def wait(self):
        while True:
            if self._out is not None:
                return
            if not self._server.is_alive() and self._out is None:
                raise RuntimeError("command server died")
            time.sleep(.1)


class CommandSendingServer(threading.Thread):
    """ Connect to a socket, send commands and collect returncodes.

    Parameters
    ----------
    request_path: str
        A zmq path to a socket where commands will be send
    response_path: str
        A zmq path to a socket where we listen for the responses
    backend_running: callable
        A callable that return whether the backend (the vm) is still alive
    ctx: zmq.Context, optional
        A zmq context
    """
    def __init__(self, request_path, response_path, backend_alive, num_workers,
                 ctx=None):
        super().__init__()
        self._context = ctx or zmq.Context()
        self._num_workers = num_workers
        self._request_socket = self._context.socket(zmq.PUSH)
        self._response_socket = self._context.socket(zmq.PULL)
        self._request_socket.set(zmq.SNDTIMEO, 60000)

        self._request_socket.setsockopt(zmq.LINGER, -1)
        self._response_socket.setsockopt(zmq.LINGER, -1)

        self._request_socket.hwm = 10
        self._response_socket.hwm = 10

        self._request_socket.bind(request_path)
        self._response_socket.bind(response_path)
        self._backend_alive = backend_alive

        self._future_lock = threading.Lock()

        self._shutdown = False

        self._remote_logger = logging.getLogger('remote')
        self._request_counter = 0
        self._futures = {}

    def shutdown(self):
        """
        Do not accept new commands and wait until all commands are finished.
        """
        self._shutdown = True
        while True:
            with self._future_lock:
                if self._futures and (self.is_alive() or self._backend_alive()):
                    raise ValueError("Could not finish all tasks")
                if not self._futures:
                    break
            time.sleep(.1)

    def __del__(self):
        self._request_socket.close()
        self._response_socket.close()
        self._context.term()

    def send_setup(self):
        self._request_socket.send_json(
            {
                'type': 'setup',
                'numWorkers': self._num_workers
            }
        )

    def run(self):
        try:
            poller = zmq.Poller()
            poller.register(self._response_socket, zmq.POLLIN)
            while True:
                socks = dict(poller.poll(100))
                if socks:
                    data = self._response_socket.recv_json(zmq.NOBLOCK)
                    if data['type'] == 'logging':
                        self._remote_logger.log(data['priority'],
                                                data['message'])
                    elif data['type'] == 'commandFinished':
                        with self._future_lock:
                            future = self._futures[data['requestID']]
                            future._out = data
                            del self._futures[data['requestID']]
                elif not self._backend_alive():
                    logger.info(
                        "VM is dead. Shutting down CommandSendingServer")
                    return
        except Exception as e:
            logger.critical("Exception in server thread: %s" % e)
            raise

    def send_command(self, command):
        if not self.is_alive() or not self._backend_alive():
            raise RuntimeError("Tried to send message to dead server")

        with self._future_lock:
            if self._shutdown:
                raise RuntimeError("No new tasks are accepted after `finish`")
            future = VMFuture(self, command)
            assert self._request_counter not in self._futures
            self._futures[self._request_counter] = future
            request = {
                'type': 'command',
                'command': command,
                'requestID': self._request_counter,
            }
            self._request_counter += 1
        self._request_socket.send_json(request)
        return future


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--qemu", help="qemu executable",
                        default="qemu-system-x86_64")
    parser.add_argument("-i", "--input", help="Input files",
                        nargs="+", default=[])
    parser.add_argument("image", help="vm image")
    parser.add_argument("output", help="Path to output tar")
    parser.add_argument("--cores", default="2")
    parser.add_argument("--workdir", default="/tmp")
    parser.add_argument("--insize", default="+500M")
    parser.add_argument("--outsize", default="10G")

    return parser.parse_args()


def main():
    import cmd
    from .qemu import VMBuilder
    args = parse_args()

    def parse(arg):
        return arg.split()

    class RemoteShell(cmd.Cmd):
        prompt = '$ '

        def __init__(self, vm):
            super().__init__()
            self._vm = vm

        def do_exec(self, arg):
            future = self._vm.submit(parse(arg))
            ret, out, err = future.result()
            print("Exit code:", ret)
            print("Stdout:", out)
            print("Stderr:", err)

        def do_exit(self, arg):
            return True

    with VMBuilder(args.qemu, args.image, args.workdir) as vm:
        vm.add_diskimg('input', args.input, size=args.insize)
        vm.add_diskimg('output', size=args.outsize)
        vm.add_option('cpu', 'host')
        vm.add_option('enable-kvm')
        vm.add_option('display', 'sdl')
        vm.add_option("m", "2G")
        vm.add_option('smp', sockets=1, cores=args.cores, threads=2)
        with vm.executor() as executor:
            RemoteShell(executor).cmdloop()
            executor.submit(['shutdown', '/t', '3']).wait()
        vm.copy_out('output', args.output)


if __name__ == '__main__':
    main()
