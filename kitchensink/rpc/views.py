import logging
import traceback

from flask import request, current_app, jsonify
from rq.job import Status

from .app import rpcblueprint

logger = logging.getLogger(__name__)

# we assume that you set rpc.rpc to some instance of an RPC object
def make_json(jsonstring, status_code=200, headers={}):
    """like jsonify, except accepts string, so we can do our own custom
    json serialization.  should move this to continuumweb later
    """
    return current_app.response_class(response=jsonstring,
                                      status=status_code,
                                      headers=headers,
                                      mimetype='application/json')

@rpcblueprint.route("/call/<rpcname>/")
def call(rpcname):
    func_string = request.values['func_string']
    args_string = request.values.get('args_string')
    kwargs_string = request.values.get('kwargs_string')
    fmt = request.values.get('fmt')
    queue_name = request.values.get('queue_name', 'default')
    auth_string = request.values.get('auth_string')
    async = request.values.get('async').lower() == 'true'
    serialized_function = request.values.get('serialized_function').lower() == 'true'
    rpc = rpcblueprint.rpcs[rpcname]
    result = rpc.call(func_string, args_string, kwargs_string, fmt,
                      serialized_function=serialized_function,
                      auth_string=auth_string, async=async,
                      queue_name=queue_name)
    if async:
        return result
    else:
        status, metadata, result = result
        return jsonify(status=status, result=result, metadata=metadata)

@rpcblueprint.route("/status/<job_id>/")
def status(job_id):
    timeout = request.values.get('timeout')
    if timeout:
        timeout = float(timeout)
    status, metadata, value = rpc.rpc.status(job_id, timeout=timeout)
    return jsonify(status=stauts, result=result)

@rpcblueprint.route("/helloworld/")
def hello():
    return "hello"
