import time

import six
import requests
import dill
from rq.job import Status

from ..serialization import (serializer, deserializer, pack_msg, unpack_msg,
                             pack_rpc_call, unpack_rpc_call)
from ..utils import make_query_url


class Client(object):
    def __init__(self, url, rpc_name='default',
                 queue_name="default",
                 fmt='dill'):
        self.url = url
        self.rpc_name = rpc_name
        self.fmt = fmt
        self.queue_name = queue_name

    def call(self, func, *args, **kwargs):
        #TODO: check for serialized function
        #TODO: handle instance methods
        func_string = None
        if not isinstance(func, six.string_types):
            func_string = func.__module__ + "." + func.__name__
        else:
            func_string = func
        func = None
        #pass func in to data later, when we support that kind of stuff

        queue_name = self.queue_name
        fmt = self.fmt
        auth_string = ""
        async = kwargs.pop('_async', True)
        metadata = dict(
            func_string=func_string,
            fmt=fmt,
            queue_name=queue_name,
            auth_string=auth_string,
            async=async)

        data = dict(func=func,
                    args=args,
                    kwargs=kwargs)

        url = self.url + "rpc/call/%s/" % self.rpc_name
        msg = pack_rpc_call(metadata, data)
        result = requests.post(url, data=msg,
                               headers={'content-type' : 'application/octet-stream'})
        metadata, data = unpack_msg(result.content)
        if metadata['status'] == Status.FAILED:
            raise Exception, metadata['error']
        elif metadata['status'] == Status.FINISHED:
            return data
        elif async:
            return self.async_result(metadata['job_id'])
    def async_result(self, jobid, interval=0.1, retries=10):
        for c in range(retries):
            url = self.url + "rpc/status/%s" % jobid
            time.sleep(interval)
            result = requests.get(url,
                                  headers={'content-type' : 'application/octet-stream'})
            metadata, data = unpack_msg(result.content)
            if metadata['status'] == Status.FAILED:
                raise Exception(data)
            elif metadata['status'] == Status.FINISHED:
                return data
            else:
                print(metadata['status'])
