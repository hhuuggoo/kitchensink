from __future__ import print_function
import time
import random
import datetime as dt
import logging
import urllib

import six
import requests
from rq.job import Status

from ..serialization import (serializer, deserializer, pack_msg,
                             unpack_result, unpack_msg,
                             pack_rpc_call, unpack_results)
from ..utils import make_query_url
from ..errors import KitchenSinkError
from .. import settings

logger = logging.getLogger(__name__)

class Client(object):
    """This class contains a variety of methods for calling functions
    The convention for kwargs that are used by the
    kitchensink framework (and which do not get passed to the
    underlying function) is to preface them with a "_"

    _queue_name - route the call to a sepcific queue
    _async - execute the task asynchronously
    _data_info - previoulsy retrieved metadata about remote data sources
    _active-hosts - previously retrieved information about which
      hosts are active
    _rpc_name - which rpc namespace the task should be executed in
    _no_route_data - don't try to use data locality based routing
    _intermediate_results - whether or not to pass stdout back to the client
    _prefix - prefix used in saving remote data (used by experimental decorators)

    For ease of interactive use, we store a decent amount of state on the client itself
    self.bulk_call, will store a function call specification (func, args, kwargs)
    in self.calls
    self.execute will execute all calls in self.calls, and store the resulting
    job ids in self.jids
    self.bulk_results will retrieving the results in self.jids, and store
    the result in self.results (And return them)

    We could make it so you didn't have to use the client in such a way that
    all the state is stored on the client, but there really is no downside to this
    clients are cheap to construct, so, I think the mode of operation should just
    be to clone this one if we need a new one
    """
    def __init__(self, url, rpc_name='default',
                 queue_name="default",
                 fmt='cloudpickle'):
        # client settings
        if url is None:
            raise Exception('url cannot be None')
        self.url = url
        self.fmt = fmt
        self.rpc_name = rpc_name
        self.queue_name = queue_name
        self.data_threshold = 200000000 #200 megs
        self.local = False
        self.prefix = settings.prefix

        # client state, used in the bulk call pipeline
        self.calls = []
        self.jids = []
        self.results = []

    def queue(self, name, host=None):
        if host:
            return "%s|%s" % (name, host)
        else:
            return name

    def has_host(self, queue_name):
        return len(queue_name.split("|")) == 2

    def clone(self):
        """constructs a client with the same settings as this one, but
        without job state (self.calls, self.jids, self.results)
        """
        return self.__class__(self.url, rpc_name=self.rpc_name,
                              queue_name=self.queue_name,
                              fmt=self.fmt)

    """Functions for executing many calls on the server
    """
    def bulk_call(self, func, *args, **kwargs):
        """bulk_call will store a function call on this instance
        to be executed by calling execute
        """
        if self.prefix and '_prefix' not in kwargs:
            kwargs['_prefix'] = self.prefix
        self.calls.append((func, args, kwargs))

    bc = bulk_call

    def _bulk_execute(self, calls, active_hosts, data_info):
        """execute a large number of async calls on the server
        calls - tuple of (func, args, kwargs)
        active_hosts - set of active hosts
        data_info - dict of url -> metadata about the data
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
        result = requests.post(url, data=msg,
                               headers={'content-type' : 'application/octet-stream'})
        msg_format, messages = unpack_msg(result.content, override_fmt='raw')
        jids = []
        errors = []
        for msg in messages:
            msg_format, [metadata, data] = unpack_result(msg)
            if metadata['status'] == Status.FAILED:
                errors.append(metadata.get('error', ''))
            else:
                jids.append(metadata['job_id'])
        if errors:
            self.bulk_cancel(jids)
            raise Exception(str(errors))
        return jids

    def execute(self):
        """execute all calls queued up with bulk_call
        """
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
        self.jids = self._bulk_execute(calls, active_hosts, data_info)

    def bulk_async_result(self, job_ids, timeout=6000.0):
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
            print ('RESULT', "%.3f" % time.time())
            metadata_data_pairs = unpack_results(result.content)
            print ('UNPACKED', "%.3f" % time.time())
            for job_id, (metadata, data) in zip(to_query, metadata_data_pairs):
                for msg in metadata.get('msgs', []):
                    if msg['type'] == 'status':
                        pass
                    else:
                        print (msg['msg'])
                if metadata['status'] == Status.FAILED:
                    self.bulk_cancel(to_query)
                    raise Exception(data)
                elif metadata['status'] == Status.FINISHED:
                    results[job_id] = data
                else:
                    pass
        return [results[x] for x in job_ids]

    def bulk_cancel(self, jids=None):
        if jids is None:
            jids = self.jids
        raw_url = self.url + "rpc/bulkcancel/"
        result = requests.post(raw_url,
                               data={'job_ids' : ",".join(jids)}
        )

    def cancel_all(self):
        return self.call('cancel_all', _rpc_name='admin', _async=False)

    """functions for executing a call locally (on this machine)
    these are mostly experimental
    """
    def execute_local(self):
        self.results = []
        for c in self.calls:
            func, args, kwargs = c
            r = func(*args, **kwargs)
            self.results.append(r)

    def bulk_results_local(self):
        return self.results

    def bulk_results(self, profile=False):
        st = time.time()
        if self.local:
            return self.bulk_results_local()
        try:
            retval = self.bulk_async_result(self.jids)
        except KeyboardInterrupt as e:
            self.bulk_cancel(self.jids)
        ed = time.time()
        if profile:
            if isinstance(profile, basestring):
                print ("%s took %s" % (profile, ed-st))
            else:
                print ("%s took %s" % ("profile", ed-st))
            components = self.call('retrieve_profile',
                                   self.jids, _rpc_name='admin',
                                   _async=False)
            last_finish = components.pop('last_finish')
            total_runtimes = components.pop('total_runtimes')
            print (components)
            components.pop('start_spread')
            components.pop('end_spread')
            overhead = (ed-st) - (components.sum() / len(self.jids))
            overhead2 = (ed-st) - total_runtimes / len(self.jids)
            print ('%s overhead %s' % (profile, overhead))
            print ('%s overhead2 %s' % (profile, overhead2))
            print ('%s result delay %s' % (profile, ed - last_finish))
            print ('%s complete %s' % (profile, ed))
        return retval

    br = bulk_results
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
        queue_names = None
        queue_name = kwargs.pop('_queue_name', self.queue_name)
        if (queue_name and self.has_host(queue_name)) or no_route_data:
            queue_names = [queue_name]
        else:
            #fixme circular import
            from ..data.routing  import inspect, route
            data_urls = inspect(args, kwargs)
            if data_urls:
                if active_hosts is None and data_info is None:
                    active_hosts, data_info = self.data_info(data_urls)
                hosts = route(data_urls, active_hosts, data_info,
                              self.data_threshold)
                #strip off size information
                queue_names = [self.queue(queue_name, host=x[0]) for x in hosts]
                #logger.debug("routing to %s", queue_names)
            else:
                queue_names = [queue_name]
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

    """
    functions for executing one function, or retrieving one result,
    one at a time
    """

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


    def cancel(self, jobid):
        """cannot currently cancel running jobs
        """
        url = self.url + "rpc/cancel/%s" % jobid
        resp = requests.get(url)
        if resp.status_code == 200:
            return
        else:
            raise KitchenSinkError("Failed to cancel %s" % jobid)

    """remote data functions
    """
    def data_info(self, urls):
        active_hosts, results = self.call('get_info_bulk', urls,
                                          _async=False, _rpc_name="data",
                                          _no_route_data=True)
        return active_hosts, results

    def _get_data(self, path, offset=None, length=None):
        url = self.url + "rpc/data/%s/" % urllib.quote(path)
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

    def hosts(self, to_write=False):
        return self.call('hosts', to_write=to_write, _rpc_name='data', _async=False)

    def pick_host(self, data_url):
        active_hosts, results = self.call('get_info_bulk', [data_url],
                                          _rpc_name='data',
                                          _async=False)
        location_info, data_info = results[data_url]
        if settings.host_name and settings.host_name in location_info:
            return (settings.host_name, active_hosts[settings.host_name])
        #FIXME: TEST THIS
        host = random.choice(list(location_info))
        return (host, active_hosts[host])

    def path_search(self, pattern):
        return self.async_result(self.call('search_path', pattern,
                                           _async=True,
                                           _rpc_name='data'))

    def reducetree(self, pattern, number=0, remove_host=None):
        matching = self.path_search(pattern)
        self.reducedata(*matching, number=number, remove_host=remove_host)

    def reducedata(self, *urls, **kwargs):
        c = self.clone()
        number = kwargs.pop('number', 0)
        remove_host = kwargs.pop('remove_host', None)
        active_hosts, infos = self.data_info(urls)
        active_host_names = set(active_hosts.keys())
        for url in urls:
            host_info, data_info = infos[url]
            hosts = set(active_host_names).intersection(host_info)
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
                queue = self.queue('data', host=host)
                c.bulk_call('delete', url, _queue_name=queue,
                            _rpc_name='data')
        c.execute()
        return c.bulk_results()

    """
    experimental data management operations - might not work
    use at your own peril
    """

    def move_data_pattern(self, pattern, from_host, to_host=None):
        c = self.clone()
        urls = self.path_search(pattern)
        info = self.data_info(urls)
        active_hosts, data_info = info
        active_host_names = set(active_hosts.keys())
        candidates = [x for x in active_host_names if x != from_host]
        for url, (location_info, data_info) in data_info.items():
            if to_host is None:
                target = random.choice(candidates)
            else:
                target = to_host
            if from_host in location_info and len(location_info) == 1:
                queue = self.queue('data', host=from_host)
                c.bc('chunked_copy', url, metadata.get('size'),
                     from_host, _queue_name=queue)
        c.execute()
        c.br()
        info = self.data_info(urls)
        active_hosts, data_info = info
        active_host_names = set(active_hosts.keys())
        for url, (location_info, data_info) in data_info.items():
            if from_host in location_info and len(location_info) >= 2:
                queue_name = self.queue('data', host=from_host)
                self.bc('delete', url, _queue_name=queue_name, _rpc_name='data')
        self.execute()
        self.br()

    def md5(self, url):
        from kitchensink.data import du
        infos = self.data_info([url])
        active_hosts = infos[0]
        info, metadata = infos[-1][url]
        hosts = set(active_hosts).intersection(set(info.keys()))
        obj = du(url)
        for h in hosts:
            queue = self.queue('data', host=h)
            self.bc(md5sum, obj, _queue_name=queue)
        self.execute()
        results = self.br()
        return zip(hosts, results)

    def ping(self):
        url = self.url + "rpc/ping/"
        try:
            return requests.get(url).content
        except requests.ConnectionError:
            return None

import hashlib
def md5sum(obj):
    path = obj.local_path()
    m = hashlib.md5()
    with open(path) as f:
        m.update(f.read())
    return m.hexdigest()
