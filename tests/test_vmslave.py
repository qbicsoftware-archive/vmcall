import pytest
from vmcall import vmserve
import zmq
import tempfile
import threading
import logging
import time


@pytest.yield_fixture
def slave_ctx():
    context = zmq.Context()
    remote_req = context.socket(zmq.PUSH)
    remote_res = context.socket(zmq.PULL)

    with tempfile.TemporaryDirectory() as tmp:
        req_path = 'ipc://' + tmp + '/req'
        res_path = 'ipc://' + tmp + '/res'
        remote_req.bind(req_path)
        remote_res.bind(res_path)
        slave = vmserve.VMSlave(req_path, res_path)
        thread = threading.Thread(target=slave.serve_till_shutdown)
        yield slave, remote_req, remote_res, thread

        slave.shutdown()
        thread.join()
        collect_responses(remote_res)


def collect_responses(socket):
    responses = []
    while True:
        try:
            responses.append(socket.recv_json(zmq.NOBLOCK))
        except zmq.error.Again:
            break
    return responses


def test_setup_exit(slave_ctx):
    slave, remote_req, remote_res, thread = slave_ctx
    thread.start()
    remote_req.send_json({'type': 'setup', 'numWorkers': 2})


def test_no_setup(slave_ctx):
    slave, remote_req, remote_res, thread = slave_ctx
    thread.start()
    remote_req.send_json({'type': 'blubb'})
    assert remote_res.recv_json()['type'] == 'logging'
    assert remote_res.recv_json()['priority'] == logging.CRITICAL


def test_request(slave_ctx):
    slave, remote_req, remote_res, thread = slave_ctx
    thread.start()
    remote_req.send_json({'type': 'setup', 'numWorkers': 2})
    remote_req.send_json({'type': 'command', 'command': ['ls'],
                          'requestID': 0})
    time.sleep(.2)
    res = collect_responses(remote_res)
    assert any('requestID' in s and s['requestID'] == 0 for s in res)


def test_timeout(slave_ctx):
    slave, remote_req, remote_res, thread = slave_ctx
    thread.start()
    remote_req.send_json({'type': 'setup', 'numWorkers': 2})
    remote_req.send_json({'type': 'command', 'command': ['sleep', '5'],
                          'timeout': .01, 'requestID': 0})
    time.sleep(.2)
    res = collect_responses(remote_res)
    assert any('exception' in s and s['requestID'] == 0 for s in res)
