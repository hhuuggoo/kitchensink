import traceback

from rq.job import Status

from ..serialization import json_serialization, dill_serialization, pickle_serialization
from ..errors import UnauthorizedAccess, UnknownFunction, WrappedError

"""
Aspects:
- async or not
- serialized/deserialized
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
        self.formats = {}
        self.task_queue = task_queue
        self.auth = auth
        self.register_default_serialization()

    def register_default_serialization(self):
        self.register_serialization('json',
                                    json_serialization.serialize,
                                    json_serialization.deserialize)
        self.register_serialization('dill',
                                    dill_serialization.serialize,
                                    dill_serialization.deserialize)
        self.register_serialization('pickle',
                                    pickle_serialization.serialize,
                                    pickle_serialization.deserialize)

    def call(self, func_string, args_string, kwargs_string, fmt,
             serialized_function=False, auth_string=None, async=True,
             queue_name="default"):
        """
        Args
            func_string : name of function, OR module:func
            args_string : args un-serialized for function
            kwargs : kwargs un-serialized for function
            fmt : serialization mechanism (dill, pickle, or json)
        """
        if auth_string and self.auth:
            auth = self.deserializer(fmt)(auth_string)
            if not self.auth(auth):
                raise UnauthorizedException
        if async:
            return self.call_async(func_string, args_string, kwargs_string, fmt)
        else:
            return self.call_instant(func_string, args_string, kwargs_string, fmt)

    def call_instant(self, func_string, args_string, kwargs_string, fmt):
        try:
            args = self.deserializer(fmt)(args_string)
            kwargs = self.deserializer(fmt)(kwargs_string)
            func = self.resolve_function(func_string)
            result = wrap(func)(*args, **kwargs)
            return Status.FINISHED, {'fmt' : fmt}, self.serializer(fmt)(result)
        except Exception as e:
            exc_info = traceback.format_exc()
            """
            how to do return errors?
            """
            e = WrappedError()
            e.message = exc_info
            return Status.FAILED, {'fmt' : fmt}, e

    def function_default_format(self, func_string):
        """Default format associated with a registered function
        """
        if func_string in self.functions:
            return self.functions[func_string][1]


    def resolve_function(self, func_string, serialized_function=False):
        """turn a func_string into a function
        """
        if serialized_function:
            func = dill_serialization.deserialize(func_string)
            ## instance methods are serialized as tuples of
            ## the instance and func name
            if isinstance(func, tuple):
                return getattr(func[0], func[1])
            else:
                return func
        if func_string in self.functions:
            return self.functions[func_string][0]
        if self.allow_arbitrary:
            return self.get_arbitrary_function(func_sring)

    def serializer(self, fmt):
        return self.formats[fmt][0]

    def deserializer(self, fmt):
        return self.formats[fmt][1]

    def get_arbitrary_function(self, func_string):
        raise NotImplementedError

    def call_async(self, func_string, args_string, kwargs_string, fmt, queue_name):
        ## at some point, we might pass strings
        ## directly to the backend task queue, but for now
        ## we parse them into python objects, and then
        ## re-serialize them via python-rq(which uses pickle)
        func = self.resolve_function(func_string)
        args = self.deserializer(fmt)(args_string)
        kwargs = self.deserializer(fmt)(kwargs_string)
        func = wrap(func)
        job_id = self.task_queue.enqueue(queue_name, func, args, kwargs,
                                         metadata={'fmt' : fmt})
        return job_id

    def status(self, job_id, timeout=None):
        return self.task_queue.status(job_id, timeout=timeout)

    def register_function(self, func, name=None, default_fmt="pickle"):
        if name is None:
            name = func.__name__
        self.functions[name] = (func, default_fmt)

    def register_serialization(self, name, serializer, deserializer):
        self.formats[name] = (serializer, deserializer)

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
