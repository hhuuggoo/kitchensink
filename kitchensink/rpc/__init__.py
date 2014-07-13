import traceback

from rq.job import Status
from six import string_types

from ..serialization import (json_serialization,
                             dill_serialization,
                             pickle_serialization,
                             pack_msg,
                             unpack_msg)

from ..errors import UnauthorizedAccess, UnknownFunction, WrappedError
from ..settings import serializer, deserializer

"""
Aspects:
- async or not
- serialized/deserialized
- currently, the serialization format stops and ends with the head node
- however in the future, serialization will be done on the worker nodes
"""
class RPC(object):
    def __init__(self,
                 allow_arbitrary=False,
                 default_serialization_config=True,
                 task_queue=None,
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

    def call(self, msg):
        metadata, data = unpack_msg(msg)

        #metadata
        fmt = metadata.get('fmt', 'dill')
        queue_name = metadata.get('queue_name', 'default')
        async = metadata.get('async', True)
        auth = metadata.get('auth', '')

        #data
        func = data['func']
        args = data.get('args', [])
        kwargs = data.get('kwargs', {})
        if auth and self.auth:
            if not self.auth(auth):
                raise UnauthorizedException
        if async:
            return self.call_async(func, args, kwargs,
                                   fmt, queue_name)
        else:
            return self.call_instant(func, args, kwargs,
                                     fmt)

    def call_instant(self, func, args, kwargs, fmt):
        try:
            if isinstance(func, string_types):
                func = self.resolve_function(func)
            result = wrap(func)(*args, **kwargs)
            metadata = {'fmt' : fmt, 'status' : Status.FINISHED}
            return pack_msg(metadata, result)
        except Exception as e:
            exc_info = traceback.format_exc()
            """
            how to do return errors?
            """
            metadata = {'fmt' : fmt, 'status' : Status.FAILED, 'error' : exc_info}
            return pack_msg(metadata, None)

    def resolve_function(self, func_string):
        """turn a func_string into a function
        """
        if func_string in self.functions:
            return self.functions[func_string]
        if self.allow_arbitrary:
            return self.get_arbitrary_function(func_sring)

    def get_arbitrary_function(self, func_string):
        raise NotImplementedError

    def call_async(self, func, args, kwargs, fmt, queue_name):
        ## at some point, we might pass strings
        ## directly to the backend task queue, but for now
        ## we parse them into python objects, and then
        ## re-serialize them via python-rq(which uses pickle)
        if isinstance(func, string_types):
            func = self.resolve_function(func)
        func = wrap(func)
        job_id = self.task_queue.enqueue(queue_name, func, args, kwargs,
                                         metadata={'fmt' : fmt})
        return job_id

    def status(self, job_id, timeout=None):
        return self.task_queue.status(job_id, timeout=timeout)

    def register_function(self, func, name=None):
        if name is None:
            name = func.__name__
        self.functions[name] = func


def wrap(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        #postprocess results here
        return result
    wrapper.__name__ = func.__name__
    return wrapper

## fmt is the format that the consumer wants, and is sending args in
## for now we use our own internal serialization (probably pickle or dill)
## we can sort out the internal serialization later
## same goes for storage of server side data into files
