from __future__ import print_function
import time

import six
import requests
import dill
from rq.job import Status

from ..serialization import (serializer, deserializer, unpack_result,
                             pack_rpc_call, unpack_results)
from ..utils import make_query_url
from ..errors import KitchenSinkError
from .. import settings

class Client(object):
    def __init__(self, url, rpc_name='default',
                 queue_name="default",
                 fmt='cloudpickle'):
        self.url = url

        self.fmt = fmt
        self.rpc_name = rpc_name
        self.queue_name = queue_name
        self.data_threshold = 200000000 #200 megs

    def data_info(self, urls):
        results = {}
        active_hosts = self.call('hosts', _async=False, _rpc_name="data",
                                 _no_route_data=True)
        for u in urls:
            host_info, data_info = self.call('get_info', u,
                                             _no_route_data=True,
                                             _rpc_name="data",
                                             _async=False)
            for host in host_info.keys():
                if host not in active_hosts:
                    host_info.pop(host)
            results[u] = host_info, data_info
        return active_hosts, results

    def call(self, func, *args, **kwargs):
        #TODO: check for serialized function
        #TODO: handle instance methods
        func_string = None
        if isinstance(func, six.string_types):
            func_string = func
        else:
            func = func
        #pass func in to data later, when we support that kind of stuff
        if "_queue_name" in kwargs:
            queue_names = [kwargs.pop("_queue_name")]
        elif kwargs.pop('_no_route_data', False):
            queue_names = [self.queue_name]
        else:
            #fixme circular import
            from ..data.routing  import inspect, route
            data_urls = inspect(args, kwargs)
            if data_urls:
                active_hosts, infos = self.data_info(data_urls)
                queue_names = route(data_urls, active_hosts, infos, self.data_threshold)
                #strip off size information
                queue_names = [x[0] for x in queue_names]
                print (queue_names)
            else:
                queue_names = [self.queue_name]
        fmt = self.fmt
        auth_string = ""
        async = kwargs.pop('_async', True)
        rpc_name = kwargs.pop('_rpc_name', self.rpc_name)

        metadata = dict(
            func_string=func_string,
            result_fmt=fmt,
            queue_names=queue_names,
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
            return metadata['job_id']

    def async_result(self, jobid, retries=10):
        for c in range(retries):
            url = self.url + "rpc/status/%s" % jobid
            result = requests.get(url,
                                  headers={'content-type' : 'application/octet-stream'})
            msg_format, [metadata, data] = unpack_result(result.content)
            for msg in metadata.get('msgs', []):
                if msg['type'] == 'status':
                    print (msg['status'])
                else:
                    print (msg['msg'])
            if metadata['status'] == Status.FAILED:
                raise Exception(data)
            elif metadata['status'] == Status.FINISHED:
                return data
            elif metadata['status'] == Status.STARTED:
                pass

    def bulk_async_result(self, job_ids, timeout=600.0):
        to_query = job_ids
        raw_url = self.url + "rpc/bulkstatus/"
        results = {}
        st = time.time()
        while True:
            to_query = list(set(to_query).difference(set(results.keys())))
            if time.time() - st > timeout:
                break
            if len(to_query) == 0:
                break
            result = requests.post(raw_url,
                                   data={'job_ids' : ",".join(to_query)},
            )
            metadata_data_pairs = unpack_results(result.content)
            for job_id, (metadata, data) in zip(to_query, metadata_data_pairs):
                for msg in metadata.get('msgs', []):
                    if msg['type'] == 'status':
                        print (job_id, msg['status'])
                    else:
                        print (msg['msg'])
                if metadata['status'] == Status.FAILED:
                    for job_id in to_query:
                        self.cancel(job_id)
                    raise Exception(data)
                elif metadata['status'] == Status.FINISHED:
                    results[job_id] = data
                else:
                    pass
        return [results[x] for x in job_ids]

    def cancel(self, jobid):
        """cannot currently cancel running jobs
        """
        url = self.url + "rpc/cancel/%s" % jobid
        resp = requests.get(url)
        if resp.status_code == 200:
            return
        else:
            raise KitchenSinkError("Failed to cancel %s" % jobid)

    def _get_data(self, path, offset=None, length=None):
        url = self.url + "rpc/data/%s/" % path
        if offset is not None and length is not None:
            url = make_query_url(url, {'offset' : offset, 'length' : length})
        result = requests.get(url, stream=True)
        return result

    def _put_data(self, path, f):
        url = self.url + "rpc/data/%s/" % path
        result = requests.post(url, files={'data' : f}).json()
        if result.get('error'):
            raise Exception(result.get('error'))
        return result

    def pick_host(self, data_url):
        host_info, data_info = self.call('get_info', data_url,
                                         _rpc_name='data',
                                         _async=False)
        if settings.host_url and settings.host_url in host_info:
            return host_info[settings.host_url]
        host = host_info.keys()[0]
        return host

    def path_search(self, pattern):
        return self.async_result(self.call('search_path', pattern,
                                           _async=True,
                                           _rpc_name='data'))
    def reduce_data_hosts(self, url, number=0):
        host_info, data_info = self.call('get_info', url,
                                         _rpc_name='data',
                                         _async=False)
        active_hosts = self.call('hosts', _async=False,
                                 _rpc_name="data",
                                 _no_route_data=True)
        hosts = set(active_hosts).intersection(set(host_info.keys()))
        jobs = []
        if not len(hosts) > number:
            print("%s hosts have data.  Not reducing" % len(hosts))
            return
        to_keep = set(list(hosts)[:number])
        to_delete = hosts.difference(to_keep)
        print ("**DELETE", to_delete)
        for host in to_delete:
            result = self.call('delete', url, _queue_name=host, _rpc_name='data')
            jobs.append(result)
        return self.bulk_async_result(jobs)
