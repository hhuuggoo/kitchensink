import six
import requests
import dill
from rq.job import Status

from ..serialization import serializer, deserializer, pack_msg, unpack_msg
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
        if not isinstance(func, six.string_types):
            func = func.__module__ + "." + func.__name__
        queue_name = self.queue_name
        fmt = self.fmt
        auth_string = ""
        async = kwargs.pop('_async', True)
        metadata = dict(fmt=fmt,
                        queue_name=queue_name,
                        auth_string=auth_string,
                        async=async)
        data = dict(func=func,
                    args=args,
                    kwargs_sring=kwargs)

        url = self.url + "rpc/call/%s/" % self.rpc_name
        msg = pack_msg(metadata, data)
        result = requests.post(url, data=msg,
                               headers={'content-type' : 'application/octet-stream'})
        metadata, data = unpack_msg(result.content)
        if metadata['status'] == Status.FAILED:
            raise Exception, metadata['error']
        else:
            return data
