from __future__ import print_function
import time
import random
import datetime as dt

import six
import requests
import dill
from rq.job import Status

from ..serialization import (serializer, deserializer, pack_msg,
                             unpack_result, unpack_msg,
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
        self.calls = []
        self.local = False
        self.prefix = ""

    def bulk_call(self, func, *args, **kwargs):
        if self.prefix and '_prefix' not in kwargs:
            kwargs['_prefix'] = self.prefix
        self.calls.append((func, args, kwargs))

    bc = bulk_call
    def execute_local(self):
        self.results = []
        for c in self.calls:
            func, args, kwargs = c
            r = func(*args, **kwargs)
            self.results.append(r)

    def bulk_results_local(self):
        return self.results

    def execute(self):
        if self.local:
            return self.execute_local()
        urls = set()
        from ..data.routing  import inspect, route
        for func, args, kwargs in self.calls:
            urls.update(inspect(args, kwargs))
        if urls:
            active_hosts, data_info = self.data_info(urls)
        else:
            active_hosts, data_info = None, None
        calls = self.calls
        self.calls = []
        self.jids = self._bulk_call(calls, active_hosts, data_info)

    def bulk_results(self):
        if self.local:
            return self.bulk_results_local()
        try:
            return self.bulk_async_result(self.jids)
        except KeyboardInterrupt as e:
            self.bulk_cancel(self.jids)
    br = bulk_results
    def bulk_cancel(self, jids=None):
        if jids is None:
            jids = self.jids
        raw_url = self.url + "rpc/bulkcancel/"
        result = requests.post(raw_url,
                               data={'job_ids' : ",".join(jids)}
        )


    def data_info(self, urls):
        active_hosts, results = self.call('get_info_bulk', urls,
                                          _async=False, _rpc_name="data",
                                          _no_route_data=True)
        return active_hosts, results

    def rpc_message(self, func, *args, **kwargs):
        #TODO: check for serialized function
        #TODO: handle instance methods
        async = kwargs.pop('_async', True)
        active_hosts = kwargs.pop('_active_hosts', None)
        data_info = kwargs.pop('_data_info', None)
        no_route_data = kwargs.pop('_no_route_data', False)
        intermediate_results = kwargs.pop('_intermediate_results', True)
        prefix = kwargs.pop('_prefix', "")

        func_string = None
        if isinstance(func, six.string_types):
            func_string = func
        else:
            func = func
        #pass func in to data later, when we support that kind of stuff
        if "_queue_name" in kwargs:
            queue_names = [kwargs.pop("_queue_name")]
        elif no_route_data:
            queue_names = [self.queue_name]
        else:
            #fixme circular import
            from ..data.routing  import inspect, route
            data_urls = inspect(args, kwargs)
            if data_urls:
                if active_hosts is None and data_info is None:
                    active_hosts, data_info = self.data_info(data_urls)
                queue_names = route(data_urls, active_hosts, data_info,
                                    self.data_threshold)
                #strip off size information
                queue_names = [x[0] for x in queue_names]
                print (queue_names)
                print ("finished routing", time.time())
            else:
                queue_names = [self.queue_name]
        fmt = self.fmt
        auth_string = ""

        metadata = dict(
            intermediate_results=intermediate_results,
            func_string=func_string,
            result_fmt=fmt,
            queue_names=queue_names,
            auth_string=auth_string,
            async=async,
            prefix=prefix
        )

        data = dict(func=func,
                    args=args,
                    kwargs=kwargs)
        msg = pack_rpc_call(metadata, data, fmt=self.fmt)
        return msg

    def _bulk_call(self, calls, active_hosts, data_info):
        """only works async
        """
        data = []
        for func, args, kwargs in calls:
            rpc_name = kwargs.pop('_rpc_name', self.rpc_name)
            kwargs['_data_info'] = data_info
            kwargs['_active_hosts'] = active_hosts
            msg = self.rpc_message(func, *args, **kwargs)
            data.append(rpc_name),
            data.append(msg)
        msg = pack_msg(*data, fmt=["raw" for x in range(len(data))])
        url = self.url + "rpc/bulkcall/"
        print ("POST",dt.datetime.now().isoformat())
        result = requests.post(url, data=msg,
                               headers={'content-type' : 'application/octet-stream'})
        msg_format, messages = unpack_msg(result.content, override_fmt='raw')
        jids = []
        for msg in messages:
            msg_format, [metadata, data] = unpack_result(msg)
            if metadata['status'] == Status.FAILED:
                raise Exception, metadata['error']
            else:
                jids.append(metadata['job_id'])
        return jids

    def call(self, func, *args, **kwargs):
        async = kwargs.get('_async', True)
        rpc_name = kwargs.pop('_rpc_name', self.rpc_name)
        msg = self.rpc_message(func, *args, **kwargs)
        url = self.url + "rpc/call/%s/" % rpc_name
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

    def bulk_async_result(self, job_ids, timeout=6000.0):
        to_query = job_ids
        raw_url = self.url + "rpc/bulkstatus/"
        results = {}
        st = time.time()
        while True:
            to_query = list(set(to_query).difference(set(results.keys())))
            print ("WAITING ON %s" % len(to_query))
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

    def _put_data(self, path, f, data_type='object', fmt="cloudpickle"):
        url = self.url + "rpc/data/%s/" % path
        result = requests.post(url,
                               data={'data_type' : data_type,
                                     'fmt' : fmt
                                 },
                               files={'data' : f}).json()
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
    def reduce_data_hosts(self, *urls, **kwargs):
        number = kwargs.pop('number', 0)
        remove_host = kwargs.pop('remove_host', None)
        active_hosts, infos = self.data_info(urls)
        for url in urls:
            host_info, data_info = infos[url]
            hosts = set(active_hosts).intersection(set(host_info.keys()))
            if not len(hosts) > number:
                print("%s hosts have data for %s.  Not reducing" % (len(hosts), url))
                continue
            if remove_host and remove_host in hosts:
                hosts = [x for x in hosts if x != remove_host]
                hosts.append(remove_host)
            to_keep = set(list(hosts)[:number])
            assert len(to_keep) == number
            to_delete = set(hosts).difference(to_keep)
            for host in to_delete:
                self.bulk_call('delete', url, _queue_name=host,
                               _rpc_name='data')
        self.execute()
        self.bulk_results()

    def move_data(self, url, length, from_host, to_host=None):
        active_hosts = self.call('hosts', _async=False,
                                 _rpc_name="data",
                                 _no_route_data=True)
        if to_host is None:
            active_hosts = [x for x in active_hosts if x != from_host]
            to_host = random.choice(active_hosts)
        result = self.call('chunked_copy', url, length, from_host,
                        _queue_name=to_host,
        )
        result = self.async_result(result)
        print (result)
        result = self.call('delete', url, _queue_name=from_host, _rpc_name='data')
        result = self.async_result(result)
        print (result)

    def reducetree(self, pattern, number=0, remove_host=None):
        matching = self.path_search(pattern)
        self.reduce_data_hosts(*matching, number=number, remove_host=remove_host)
