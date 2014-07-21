import time

import six
import requests
import dill
from rq.job import Status

from ..serialization import (serializer, deserializer, unpack_result,
                             pack_rpc_call)
from ..utils import make_query_url

class Client(object):
    def __init__(self, url, rpc_name='default',
                 queue_name="default",
                 fmt='cloudpickle'):
        self.url = url
        self.rpc_name = rpc_name
        self.fmt = fmt
        self.queue_name = queue_name

    def call(self, func, *args, **kwargs):
        #TODO: check for serialized function
        #TODO: handle instance methods
        func_string = None
        if isinstance(func, six.string_types):
            func_string = func
        else:
            func = func
        #pass func in to data later, when we support that kind of stuff

        queue_name = kwargs.pop('_queue_name', self.queue_name)
        fmt = self.fmt
        auth_string = ""
        async = kwargs.pop('_async', True)
        rpc_name = kwargs.pop('_rpc_name', self.rpc_name)

        metadata = dict(
            func_string=func_string,
            result_fmt=fmt,
            queue_name=queue_name,
            auth_string=auth_string,
            async=async)

        data = dict(func=func,
                    args=args,
                    kwargs=kwargs)

        url = self.url + "rpc/call/%s/" % rpc_name
        msg = pack_rpc_call(metadata, data, fmt=self.fmt)
        result = requests.post(url, data=msg,
                               headers={'content-type' : 'application/octet-stream'})
        msg_format, [metadata, data] = unpack_result(result.content)
        if metadata['status'] == Status.FAILED:
            raise Exception, metadata['error']
        elif metadata['status'] == Status.FINISHED:
            return data
        elif async:
            return AsyncResult(self, metadata['job_id'])

    def async_result(self, jobid, interval=0.1, retries=10):
        for c in range(retries):
            url = self.url + "rpc/status/%s" % jobid
            time.sleep(interval)
            result = requests.get(url,
                                  headers={'content-type' : 'application/octet-stream'})
            msg_format, [metadata, data] = unpack_result(result.content)
            for msg in metadata.get('msgs', []):
                if msg['type'] == 'status':
                    print (msg)
                else:
                    print (msg['msg'])
            if metadata['status'] == Status.FAILED:
                raise Exception(data)
            elif metadata['status'] == Status.FINISHED:
                return data
            elif metadata['status'] == Status.STARTED:
                pass

    def _get_data(self, path, offset=None, length=None):
        url = self.url + "rpc/data/%s/" % path
        if offset is not None and length is not None:
            url = make_query_url(url, {'offset' : offset, 'length' : length})
        result = requests.get(url, stream=True)
        return result

    def _put_data(self, path, f):
        url = self.url + "rpc/data/%s/" % path
        result = requests.post(url, files={'data' : f})
        return result

    def pick_host(self, data_url):
        host_info, data_info = self.call('get_info', data_url, _async=False)
        if settings.host_url and settings.host_url in host_info:
            return host_info[settings.host_url]
        host = host_info.keys()[0]
        return host

    def path_search(self, pattern):
        return self.call('search_path', pattern, _async=True, _rpc_name='data').result()

class AsyncResult(object):
    def __init__(self, rpc, *jobid):
        self.rpc = rpc
        self.jobid = jobid

    def result(self):
        return self.rpc.async_result(self.jobid)
