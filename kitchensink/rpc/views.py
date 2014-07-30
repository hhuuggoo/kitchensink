import logging
from os.path import exists
import traceback

from flask import request, current_app, jsonify
from rq.job import Status

from .app import rpcblueprint
from ..serialization import pack_result, pack_results, unpack_msg, pack_msg
from .. import settings
from ..utils import send_file

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

### job endpoints
@rpcblueprint.route("/call/<rpcname>/", methods=['POST'])
def call(rpcname):
    logger.info("POST, call")
    msg = request.data
    rpc = rpcblueprint.rpcs[rpcname]
    result = rpc.call(msg)
    logger.info("POST, done call")
    return current_app.response_class(response=result,
                                      status=200,
                                      mimetype='application/octet-sream')

@rpcblueprint.route("/bulkcall/", methods=['POST'])
def bulkcall():
    logger.info("BULKPOST, call")
    msg = request.data
    msg_format, messages = unpack_msg(msg, override_fmt='raw') # strip of format
    calls = [(messages[i], messages[i+1]) for i in range(0, len(messages), 2)]
    results = []
    for rpcname, msg in calls:
        rpc = rpcblueprint.rpcs[rpcname]
        result = rpc.call(msg)
        results.append(result)
    result = pack_msg(*results, fmt=['raw' for x in range(len(results))])
    logger.info("DONE BULKPOST, call")
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
                                      mimetype='application/octet-stream')

@rpcblueprint.route("/cancel/<job_id>/")
def cancel(job_id):
    rpcblueprint.task_queue.cancel(job_id)
    return "success"

@rpcblueprint.route("/bulkstatus/", methods=['POST', 'GET'])
def bulk_status():
    timeout = request.values.get('timeout', 1)
    job_ids = request.values.get('job_ids').split(",")
    if timeout:
        timeout = int(timeout)
    metadata_data_pairs = rpcblueprint.task_queue.bulkstatus(job_ids, timeout=timeout)
    # hack, with actual results, result_fmt should be present
    # otherwise for status we just use json
    fmt = [x[0].get('result_fmt', 'json') for x in metadata_data_pairs]
    result = pack_results(metadata_data_pairs, fmt=fmt)
    return current_app.response_class(response=result,
                                      status=200,
                                      mimetype='application/octet-stream')

### Data endpoints

@rpcblueprint.route("/data/<path:path>/", methods=['GET'])
def get_data(path):
    #check auth here if we're doing auth
    offset = request.values.get('offset')
    length = request.values.get('length')
    if offset is not None and length is not None:
        local_path = settings.catalog.get_file_path(path, unfinished=True)
        offset = int(offset)
        length = int(length)
        if not exists(local_path):
            data = b""
        else:
            with open(local_path, "rb") as f:
                f.seek(offset)
                data = f.read(length)
        logger.info("sending %s of %s", len(data), path)
        return current_app.response_class(response=data,
                                          status=200,
                                          mimetype='application/octet-stream')
    else:
        local_path = settings.catalog.get_file_path(path)
        logger.info("sending %s", path)
        return send_file(local_path)

@rpcblueprint.route("/data/<path:path>/", methods=['POST'])
def put_data(path):
    #check auth here if we're doing auth
    fstorage = request.files['data']
    try:
        settings.catalog.write(fstorage, path, is_new=True)
        return jsonify(success=True)
    except Exception as e:
        exc_info = traceback.format_exc()
        return jsonify(error=exc_info)

@rpcblueprint.route("/chunkeddata/<path:path>/", methods=['GET'])
def get_chunked_data(path):
    #check auth here if we're doing auth
    offset = int(request.values['offset'])
    length = int(request.values['offset'])
    local_path = settings.catalog.get_file_path(path)
    with open(local_path, "rb") as f:
        f.seek(offset)
        data = f.read(local_path, settings.chunk_size)
    return current_app.response_class(response=data,
                                      status=200,
                                      mimetype='application/octet-sream')
