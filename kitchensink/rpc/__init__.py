import traceback
import sys
import cStringIO
import threading

from rq.job import Status
from rq import Queue, Connection
from six import string_types

from ..serialization import (json_serialization,
                             dill_serialization,
                             pickle_serialization,
                             pack_result,
                             unpack_rpc_metadata,
                             unpack_msg_format,
                             unpack_rpc_call,
                             append_rpc_data
)

from ..taskqueue.objs import get_current_job
from ..errors import UnauthorizedAccess, UnknownFunction, WrappedError
from ..settings import serializer, deserializer
from .. import settings
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

    def call(self, msg):
        metadata = unpack_rpc_metadata(msg)

        #metadata
        result_fmt = metadata.get('result_fmt', 'dill')
        queue_name = metadata.get('queue_name', 'default')
        async = metadata.get('async', True)
        auth = metadata.get('auth', '')
        if auth and self.auth:
            if not self.auth(auth):
                raise UnauthorizedException
        if async:
            metadata, result = self.call_async(msg, metadata, queue_name)
        else:
            metadata, result =  self.call_instant(msg, metadata)
        return pack_result(metadata, result, fmt=result_fmt)

    def call_instant(self, msg, metadata):
        func_string = metadata.get('func_string')
        fmt = metadata['result_fmt']
        if func_string is not None:
            func = self.resolve_function(func_string)
        else:
            func = None
        msg = append_rpc_data(msg, {'func' : func}, fmt='dill')
        try:
            result = execute_msg(msg)
            metadata = {'result_fmt' : fmt, 'status' : Status.FINISHED}
            return metadata, result

        except Exception as e:
            exc_info = traceback.format_exc()
            """
            how to do return errors?
            """
            metadata = {'result_fmt' : fmt, 'status' : Status.FAILED, 'error' : exc_info}
            return metadata, None

    def call_async(self, msg, metadata, queue_name):
        ## at some point, we might pass strings
        ## directly to the backend task queue, but for now
        ## we parse them into python objects, and then
        ## re-serialize them via python-rq(which uses pickle)
        fmt = metadata['result_fmt']
        func_string = metadata.get('func_string')
        if func_string is not None:
            func = self.resolve_function(func_string)
        else:
            func = None
        msg = append_rpc_data(msg, {'func' : func}, fmt='dill')
        job_id, status = self.task_queue.enqueue(
            queue_name,
            execute_msg,
            [msg],
            {}, metadata=metadata
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


# def wrap(func):
#     def wrapper(msg):
#         metadata, data = unpack_msg(msg)
#         args = data.get('args', [])
#         kwargs = data.get('kwargs', {})
#         result = func(*args, **kwargs)
#         #postprocess results here
#         return result
#     wrapper.__name__ = func.__name__
#     wrapper.__module__ = func.__module__
#     wrapper.__name__ = func.__name__
#     return wrapper
import tempfile
class OutputThread(threading.Thread):
    interval = 1
    def run(self):
        with open(self.filename, "r") as toread:
            buf = ""
            while not self.kill:
                buf += toread.read()
                msgs = buf.split("\n")[:-1]
                for msg in msgs:
                    self.job.push_stdout(msg)
                buf = buf.rsplit("\n", 1)[-1]

def _execute_msg(msg):
    msg_format, metadata, data = unpack_rpc_call(msg)
    func = data['func']
    args = data.get('args', [])
    kwargs = data.get('kwargs', {})
    result = func(*args, **kwargs)
    return result

def execute_msg(msg):
    with Connection(settings.redis_conn):
        job = get_current_job()
    if not job or not job.meta.get('intermediate_results', True):
        return _execute_msg(msg)
    output = tempfile.NamedTemporaryFile(prefix="ks-").name
    output_thread = OutputThread()
    output_thread.filename = output
    output_thread.job = job
    output_thread.kill = False
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    with open(output, "w+", 0) as towrite:
        sys.stdout = towrite
        sys.stderr = towrite
        try:
            output_thread.start()
            result = _execute_msg(msg)
            return result
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            output_thread.kill = True
            output_thread.join()

## fmt is the format that the consumer wants, and is sending args in
## for now we use our own internal serialization (probably pickle or dill)
## we can sort out the internal serialization later
## same goes for storage of server side data into files
