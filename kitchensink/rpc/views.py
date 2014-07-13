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

@rpcblueprint.route("/call/<rpcname>/", methods=['POST'])
def call(rpcname):
    msg = request.data
    rpc = rpcblueprint.rpcs[rpcname]
    result = rpc.call(msg)
    return current_app.response_class(response=result,
                                      status=200,
                                      mimetype='application/octet-sream')

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
