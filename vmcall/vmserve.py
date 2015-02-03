""" A windows service that receives commands via tcp and executes them.

Install this service inside the VM::

    $ vmserve install
    $ vmserve start

The tcp sockets *must* not be exposed to loopback on the host or the
internet. Forward them to UNIX domain sockets on the host with
`vmcall.VMBuilder.add_command_sockets`
"""
import zmq
import logging
import time
from concurrent import futures
import subprocess
import io
import os
import csv


def win_label_to_path():
    if os.name != 'nt':
        raise NotImplementedError()
    table_str = subprocess.check_output(
        ['wmic', 'volume', 'get', 'DeviceID,Label', '/format:csv']
    ).decode()
    # first line in empty
    table = list(csv.reader(io.StringIO(table_str)))[1:]
    lines = [dict(zip(table[0], line)) for line in table[1:]]
    return dict((line['Label'], line['DeviceID']) for line in lines)


class VMSlave:
    def __init__(self, request_path, response_path, ctx=None):
        self._context = ctx or zmq.Context()
        self._request = self._context.socket(zmq.PULL)
        self._response = self._context.socket(zmq.PUSH)

        self._request.hwm = 10
        self._response.hwm = 10
        self._request.set(zmq.LINGER, -1)
        self._response.set(zmq.LINGER, -1)

        self._request.connect(request_path)
        self._response.connect(response_path)

        self._running_tasks = {}

        self._executor = None

        self._exit = False

        self._poller = zmq.Poller()
        self._poller.register(self._request, zmq.POLLIN)

        if os.name == 'nt':
            self._labels = win_label_to_path()
        else:
            self._labels = None

    def shutdown(self):
        """ Finish pending tasks and shutdown. """
        self._exit = True

    def __del__(self):
        self._request.close()
        self._response.close()
        self._context.term()

    def _recv_setup(self):
        """ Handle an incoming setup request.

        The first request a `VMSlave` receives must be a setup request like::

            {'type': setup, 'numWorkers': 2}

        This is used to set the number of workers the `VMSlave` will use.

        This function blocks until it receives a request.
        """
        self.info("Waiting for setup data")
        try:
            setup = self._request.recv_json()
            assert setup['type'] == 'setup'
            num_workers = setup['numWorkers']
            self.info("Got setup data: %s" % setup)
            return dict(num_workers=num_workers)
        except Exception as e:
            self.critical("Failed to interpret setup data: %s" % e)
            self.shutdown()
            raise

    def _handle_request(self, request):
        """ Execute a request containing a command.

        Adds the created future to `self._running_tasks`. Invalid requests will
        be logged and ignored.
        """
        if request['type'] == 'command':
            if 'requestID' not in request:
                self.critical("missing requestID in request: %s" % request)
                return
            request_id = request['requestID']
            if request_id in self._running_tasks:
                self.critical("request id is not unique: %s" % request_id)
                return
            self.debug("Submitting new task: %s" % request)
            self._running_tasks[request_id] = self._executor.submit(
                self._call, request
            )
        else:
            self.critical("Invalid request type %s"
                          % request['type'])

    def serve_till_shutdown(self):
        """ Handle requests until `self.shutdown()` is called.

        Remaining tasks will be finished even after `shutdown` is called.
        """
        setup = self._recv_setup()
        with futures.ThreadPoolExecutor(setup['num_workers']) as executor:
            self._executor = executor
            while True:
                if self._exit:
                    self._finish_remaining()
                    return
                request = self._recv_request()
                if request:
                    self._handle_request(request)
                self._send_finished()

    def _send_finished(self):
        done = []
        for task_id, task in self._running_tasks.items():
            if task.done():
                done.append(task_id)
                self._send_response(task_id, task)
        if done:
            self.info("Sending responses for %s of %s tasks" %
                      (len(done), len(self._running_tasks)))
        for key in done:
            del self._running_tasks[key]

    def _finish_remaining(self):
        while self._running_tasks:
            self._send_finished()
            time.sleep(.01)

    def _recv_request(self, timeout=50):
        """ Wait for `timeout` ms and return the request or `None`. """
        socks = dict(self._poller.poll(timeout))
        if socks:
            return self._request.recv_json(zmq.NOBLOCK)
        else:
            return None

    def _call(self, request):
        if 'command' not in request:
            self.critical("Invalid command request: %s" % request)
            return
        command = request['command']
        if self._labels:
            command = [s.format(**self._labels) for s in command]
        popen = subprocess.Popen(command, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        out, err = popen.communicate(timeout=request.get('timeout', None))
        return popen.returncode, out.decode(), err.decode()

    def _send_response(self, request_id, task):
        """ Send the result of a computation to the host.

        Arguments
        ---------
        request_id: str or int
            The id of the request. The client must make sure this id is unique.
        task: futures.Future
            A future that describes the result of the computation. Either
            `task.exception` must be set or `task.result()` must return
            the returncode of the executed command, the stdout and stderr.

        If the request is invalid, this will be logged to the host and the
        request will be ignored.
        """
        self.debug("Sending response for request %s" % request_id)
        response = {'requestID': request_id,
                    'type': 'commandFinished'}
        if task.exception():
            response['exception'] = str(task.exception())
            response['returncode'] = None
        else:
            retcode, out, err = task.result()
            response['returncode'] = retcode
            response['out'] = out
            response['err'] = err
        self._response.send_json(response)

    def log(self, priority, message):
        """ Send a logging message to the host. """
        self._response.send_json({'type': 'logging', 'message': message,
                                  'priority': priority})

    def debug(self, message):
        self.log(logging.DEBUG, message)

    def info(self, message):
        self.log(logging.INFO, message)

    def warn(self, message):
        self.log(logging.WARN, message)

    def error(self, message):
        self.log(logging.ERROR, message)

    def critical(self, message):
        self.log(logging.CRITICAL, message)


if os.name == 'nt':
    import win32serviceutil
    import win32api

    class VMCallService(win32serviceutil.ServiceFramework):
        _svc_name_ = 'VMCallService'
        _svc_display_name_ = "vmcall service"
        _svc_description_ = 'vmcall service'

        def __init__(self, args):
            super().__init__(args)

        def SvcDoRun(self):
            slave = VMSlave("tcp://10.0.2.2:8000", "tcp://10.0.2.2:8001")
            slave.serve_till_shutdown()

    def ctrlHandler(ctrlType):
        return True


def register_win_service():
    if os.name != 'nt':
        raise ValueError("Windows service is only available on windows")
    win32api.SetConsoleCtrlHandler(ctrlHandler, True)
    win32serviceutil.HandleCommandLine(VMCallService)

if __name__ == '__main__':
    register_win_service()
