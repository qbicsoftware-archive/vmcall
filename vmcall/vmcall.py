from __future__ import print_function

import subprocess
import logging
import threading
import zmq
import time

logger = logging.getLogger('vmrun-host')


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
            if self._interrupted:
                return False
            if not hasattr(self, '_vm_popen'):
                return True
            return self._vm_popen.poll() is None

        self._command_server = CommandSendingServer(
            self._request_path,
            self._response_path,
            backend_alive,
        )
        self._command_server.start()

        logger.info("Starting virtual machine")
        self._vm_popen = subprocess.Popen(
            self._command, stdin=subprocess.PIPE, stderr=subprocess.PIPE
        )

    def shutdown(self):
        if not hasattr(self, '_vm_popen'):
            raise ValueError(
                "VMExecutor.shutdown was called, but vm is not running"
            )
        logger.warn("Interrupting the virtual machine")
        self._interrupted = True
        self._vm_popen.kill()

        out, err = self._vm_popen.communicate()
        if out:
            logger.warn('Output of qemu was "%s"', out.decode())
        if err:
            logger.warn('Stderr of qemu was "%s"', err.decode())

        self._command_server.wait()

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
        self._command_server.wait()
        self.interrupt()
        if self._vm_popen.returncode:
            logger.error("qemu retured non-zero exit code %s",
                         self._vm_popen.returncode)
            raise ValueError("qemu failed")
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
        self._request_socket = self._context.socket(zmq.PUSH)
        self._response_socket = self._context.socket(zmq.PULL)
        self._request_socket.set(zmq.SNDTIMEO, 1000)
        self._request_socket.bind(request_path)
        self._response_socket.bind(response_path)
        self._backend_alive = backend_alive

        self._send_lock = threading.Lock()

        self._interrupted = False

        self._remote_logger = logging.getLogger('remote')
        self._request_counter = 0
        self._futures = {}

        self._request_socket.send_json({'type': 'setup',
                                        'numWorkers': num_workers})

    def shutdown(self):
        self._interrupted = True

    def run(self):
        poller = zmq.Poller()
        poller.register(self._response_socket, zmq.POLLIN)
        while True:
            if not self._backend_alive() or self._interrupted:
                logger.info("Shutting down CommandSendingServer")
                return
            socks = dict(poller.poll(100))
            if socks:
                data = self._response_socket.recv_json(zmq.NOBLOCK)
                if data['type'] == 'logging':
                    self._remote_logger.log(data['priority'], data['message'])
                elif data['type'] == 'commandFinished':
                    future = self._futures[data['requestID']]
                    future._out = data
                    del self._futures[data['requestID']]

    def send_command(self, command):
        if not self.is_alive() or not self._backend_alive():
            raise RuntimeError("Tried to send message to dead server")

        with self._send_lock:
            future = VMFuture(self, command)
            self._futures[self._request_counter] = future
            request = {
                'type': 'command',
                'command': command,
                'requestID': self._request_counter,
            }
            self._request_counter += 1
            self._request_socket.send_json(request)
            return future

    def wait(self):
        while (self.is_alive() and
               self._backend_alive() and
               self._futures):
            time.sleep(.1)
        return len(self._futures) == 0
