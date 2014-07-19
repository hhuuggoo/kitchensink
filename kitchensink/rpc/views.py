import logging
import traceback

from flask import request, current_app, jsonify, send_file
from rq.job import Status

from .app import rpcblueprint
from ..serialization import pack_result
from .. import settings

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
    metadata, value = rpcblueprint.task_queue.status(job_id, timeout=timeout)
    result = pack_result(metadata, value, fmt=metadata['result_fmt'])
    return current_app.response_class(response=result,
                                      status=200,
                                      mimetype='application/octet-sream')

@rpcblueprint.route("/data/<path>/", methods=['GET'])
def get_data(path):
    #check auth here if we're doing auth
    local_path = settings.catalog.local_path(path)
    return send_file(local_path)

@rpcblueprint.route("/data/<path>/", methods=['POST'])
def put_data(path):
    #check auth here if we're doing auth
    fstorage = request.files['data']
    storage.catalog.write(fstorage, path, is_new=True)
    return jsonify(success=True)
