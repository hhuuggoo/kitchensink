import traceback
import sys
import cStringIO
import threading
import logging
import time
import hashlib
import datetime as dt

from rq.job import Status
from rq import Queue, Connection
from six import string_types

from ..serialization import (pack_result,
                             unpack_rpc_metadata,
                             unpack_msg_format,
                             unpack_rpc_call,
                             append_rpc_data,
                             serializer,
                             deserializer
)

from ..taskqueue.objs import current_job_id, KitchenSinkJob
from ..errors import UnauthorizedAccess, UnknownFunction, WrappedError, KitchenSinkError
from .. import settings
from ..data import du, do

logger = logging.getLogger(__name__)
"""
Aspects:
- async or not
- serialized/deserialized
- currently, the serialization format stops and ends with the head node
- however in the future, serialization will be done on the worker nodes

NOTE: for security reasons, we should probably refactor this to
unpack the message on the work queue, since dill/pickle allow for arbitrary code
execution
"""
class RPC(object):
    def __init__(self,
                 task_queue=None,
                 allow_arbitrary=False,
                 default_serialization_config=True,
                 auth=None):
        """
        Args
            allow_arbitrary : allow calls of arbitrary
                functions that are not registered
            default_serialization_config : use default
                serialization configuration
        """
        self.allow_arbitrary = allow_arbitrary
        self.functions = {}
        self.task_queue = task_queue
        self.auth = auth
        self.queues = {}

    def setup_queue(self, queue):
        self.task_queue = queue

    def bulk_call(self, msgs):
        """TODO: refactor this so it's less complicated.
        what's going on is we're going through call1, call2, call3
        and then routing call1, call2, call3 to the first priority host
        and then call1, call2, call3 to the second priority host, etc..
        """
        async_jobs = {}
        async_job_metadata = {}
        async_queues = {}
        results = {}
        for idx, msg in enumerate(msgs):
            metadata = unpack_rpc_metadata(msg)
            result_fmt = metadata.get('result_fmt', 'cloudpickle')
            queue_names = metadata.get('queue_names', ['default'])
            async = metadata.get('async', True)
            auth = metadata.get('auth', '')
            if auth and self.auth:
                if not self.auth(auth):
                    raise UnauthorizedException
            try:
                if async:
                    fmt = metadata['result_fmt']
                    func_string = metadata.get('func_string')
                    if func_string is not None:
                        func = self.resolve_function(func_string)
                        if func is None:
                            raise KitchenSinkError("Unknownn function %s" % func_string)
                        msg = append_rpc_data(msg, {'func' : func}, fmt='cloudpickle')
                    args = [msg]
                    kwargs = {'intermediate_results' : metadata['intermediate_results']}
                    job = self.task_queue.make_job(execute_msg,
                                             args, kwargs,
                                             metadata=metadata)
                    async_jobs[idx] = job
                    async_job_metadata[idx] = {'result_fmt' : fmt, 'job_id' : job.id}
                    async_queues[idx] = queue_names
                else:
                    metadata, result =  self.call_instant(msg, metadata)
                    results[idx] = pack_result(metadata, result, fmt=result_fmt)
            except Exception as e:
                exc_info = traceback.format_exc()
                metadata = {'result_fmt' : result_fmt,
                            'status' : Status.FAILED,
                            'error' : exc_info}
                result = None
                results[idx] = pack_result(metadata, result, fmt=result_fmt)
        while async_queues:
            for idx in range(len(msgs)):
                queues = async_queues.get(idx)
                if not queues:
                    continue
                job = async_jobs.get(idx, None)
                queue = queues.pop(0)
                self.task_queue.get_queue(queue).enqueue_job(job)
                if not queues:
                    del async_queues[idx]
        for k,v in async_jobs.iteritems():
            metadata = async_job_metadata[k]
            metadata['status'] = v.get_status()
            if metadata['status'] == Status.FAILED:
                v.refresh()
                metadata['error'] = v.exc_info
            results[k] = pack_result(metadata, None, fmt=metadata['result_fmt'])
        results = [results[x] for x in range(len(msgs))]
        return results

    def call(self, msg):
        logger.debug("CALL")
        metadata = unpack_rpc_metadata(msg)
        result_fmt = metadata.get('result_fmt', 'cloudpickle')
        queue_names = metadata.get('queue_names', ['default'])
        async = metadata.get('async', True)
        auth = metadata.get('auth', '')
        if auth and self.auth:
            if not self.auth(auth):
                raise UnauthorizedException
        try:
            if async:
                metadata, result = self.call_async(msg, metadata, queue_names)
            else:
                metadata, result =  self.call_instant(msg, metadata)
        except Exception as e:
            exc_info = traceback.format_exc()
            metadata = {'result_fmt' : result_fmt,
                        'status' : Status.FAILED,
                        'error' : exc_info}
            result = None
        return pack_result(metadata, result, fmt=result_fmt)

    def call_instant(self, msg, metadata):
        func_string = metadata.get('func_string')
        fmt = metadata['result_fmt']
        if func_string is not None:
            func = self.resolve_function(func_string)
            msg = append_rpc_data(msg, {'func' : func}, fmt='cloudpickle')
        result = execute_msg(msg)
        metadata = {'result_fmt' : fmt, 'status' : Status.FINISHED}
        return metadata, result

    def call_async(self, msg, metadata, queue_names):
        ## at some point, we might pass strings
        ## directly to the backend task queue, but for now
        ## we parse them into python objects, and then
        ## re-serialize them via python-rq(which uses pickle)
        fmt = metadata['result_fmt']
        func_string = metadata.get('func_string')
        if func_string is not None:
            func = self.resolve_function(func_string)
            if func is None:
                raise KitchenSinkError("Unknownn function %s" % func_string)
            msg = append_rpc_data(msg, {'func' : func}, fmt='cloudpickle')
        job_id, status = self.task_queue.enqueue(
            queue_names,
            execute_msg,
            [msg],
            {'intermediate_results' : metadata['intermediate_results']},
            metadata=metadata
        )
        metadata = {'result_fmt' : fmt,
                    'job_id' : job_id,
                    'status' : status
        }
        return metadata, None

    def resolve_function(self, func_string):
        """turn a func_string into a function
        """
        if func_string in self.functions:
            return self.functions[func_string]
        if self.allow_arbitrary:
            return self.get_arbitrary_function(func_sring)

    def get_arbitrary_function(self, func_string):
        raise NotImplementedError

    def register_function(self, func, name=None):
        if name is None:
            name = func.__name__
        self.functions[name] = func


import tempfile
class OutputThread(threading.Thread):
    interval = 0.001
    def run(self):
        with open(self.filename, "r") as toread:
            self.buf = ""
            while not self.kill:
                self.output(toread)
            self.output(toread)
    def output(self, toread):
        read = toread.read()
        if not read:
            time.sleep(self.interval)
        self.buf += read
        msgs = self.buf.split("\n")[:-1]
        for msg in msgs:
            self.job.push_stdout(msg)
        self.buf = self.buf.rsplit("\n", 1)[-1]


def _execute_msg(msg):
    st = time.time()
    msg_format, metadata, data = unpack_rpc_call(msg)
    ed = time.time()
    func = data['func']
    memoize_url = None
    ## some code in place for memoization decorator(experimental)
    if hasattr(func, "ks_memoize") and func.ks_memoize and settings.catalog:
        m = hashlib.md5()
        m.update(msg)
        key = m.hexdigest()
        memoize_url = "memoize/%s" % key
        if settings.catalog.url_exists(memoize_url):
            logger.debug("retrieving memoized")
            return du(memoize_url).obj()

    args = data.get('args', [])
    kwargs = data.get('kwargs', {})
    result = func(*args, **kwargs)

    ## some code in place for @remote decorator(experimental)
    if hasattr(func, 'ks_remote') and func.ks_remote:
        logger.debug("METADATA %s", metadata)
        result.save(prefix=metadata.get('prefix', ''))

    ## some code in place for memoization decorator(experimental)
    if memoize_url:
        logger.debug("saving memoized")
        do(result).save(url=memoize_url)
    logger.debug("DONE EXECUTING")
    return result

def patch_loggers(output):
    patched = []
    to_patch = logging.Logger.manager.loggerDict.values()
    to_patch.append(logging.Logger.manager.root)
    for logger in to_patch:
        if not hasattr(logger, 'handlers'):
            continue
        for handler in logger.handlers:
            if isinstance(handler , logging.StreamHandler):
                patched.append((handler, handler.stream))
                handler.stream = output
    return patched

def unpatch_loggers(patched):
    for handler, stream in patched:
        handler.stream = stream

def execute_msg(msg, intermediate_results=False):
    print('START')
    if not current_job_id() or not intermediate_results:
        logger.debug("**jobid %s", current_job_id())
        return _execute_msg(msg)
    output = tempfile.NamedTemporaryFile(prefix="ks-").name
    output_thread = OutputThread()
    output_thread.filename = output
    output_thread.job = KitchenSinkJob(current_job_id(),
                                       connection=settings.redis_conn)
    output_thread.kill = False
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    with open(output, "w+", 0) as towrite:
        sys.stdout = towrite
        sys.stderr = towrite
        patched = patch_loggers(towrite)
        try:
            output_thread.start()
            result = _execute_msg(msg)
            return result
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            unpatch_loggers(patched)
            output_thread.kill = True
            output_thread.join()

## fmt is the format that the consumer wants, and is sending args in
## for now we use our own internal serialization (probably pickle or dill)
## we can sort out the internal serialization later
## same goes for storage of server side data into files
