import six
import requests
import dill
from rq.job import Status

from ..settings import serializer, deserializer
from ..utils import make_query_url


class Client(object):
    def __init__(self, url, rpc_name='default',
                 queue_name=None,
                 fmt='dill'):
        self.url = url
        self.rpc_name = rpc_name
        self.fmt = fmt
        self.queue_name = queue_name

    def call(self, func, *args, **kwargs):
        #TODO: check for serialized function
        #TODO: handle instance methods
        if isinstance(func, six.string_types):
            func_string = func
        else:
            func_string = func.__module__ + "." + func.__name__
        args_string = serializer(self.fmt)(args)
        kwargs_string = serializer(self.fmt)(kwargs)
        queue_name = self.queue_name
        fmt = self.fmt
        #auth not supported yet
        auth_string = ""
        async = kwargs.pop('_async', True)
        serialized_function = False
        data = dict(func_string=func_string,
                    args_string=args_string,
                    kwargs_sring=kwargs_string,
                    fmt=fmt,
                    queue_name=queue_name,
                    auth_string=auth_string,
                    async=async,
                    serialized_function=serialized_function)

        url = self.url + "rpc/call/%s/" % self.rpc_name
        print url
        url = make_query_url(url, data)
        result = requests.get(url)
        result = result.json()
        if result['status'] == Status.FAILED:
            raise Exception, result['result']
        else:
            return deserializer(self.fmt)(result['result'])
