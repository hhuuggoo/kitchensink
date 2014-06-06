from flask import Blueprint, request, current_app, jsonify
from rq.job import Status
import logging
import traceback
logger = logging.getLogger(__name__)

rpc = Blueprint('rpc')
# we assume that you set rpc.rpc to some instance of an RPC object
def make_json(jsonstring, status_code=200, headers={}):
    """like jsonify, except accepts string, so we can do our own custom
    json serialization.  should move this to continuumweb later
    """
    return current_app.response_class(response=jsonstring,
                                      status=status_code,
                                      headers=headers,
                                      mimetype='application/json')



def format(fmt, data):
    if fmt == 'json':
        return make_json(jsonstring)
    else:
        return data

@rpc.route("/call")
def call():
    func_string = request.values['func_string']
    args_string = request.values.get('args_string')
    kwargs_string = request.values.get('kwargs_string')
    fmt = request.values.get('fmt')
    queue_name = request.values.get('queue_name', 'default')
    auth = request.values.get('auth')
    async = request.values.get('async').lower() == 'true'
    unwrapped = request.values.get('unwrapped').lower() == 'true'
    serialized_function = request.values.get('serialized_function').lower() == 'true'
    result = rpc.rpc.call(func_string, args_string, kwargs_string, fmt,
                          serialized_function=serialized_function, 
                          auth_string=auth_string, async=async,
                          queue_name=queue_name)
    if async:
        return result
    else:
        status, metadata, result = result
        fmt = metadata['fmt']
        if unwrapped:
            return format(fmt, result)
        return jsonify(status=status, result=result)

@rpc.route("/status/<job_id>/")
def status(job_id):
    timeout = request.values.get('timeout')
    if timeout:
        timeout = float(timeout)
    status, metadata, value = rpc.rpc.status(job_id, timeout=timeout)
    return jsonify(status=stauts, result=result)
