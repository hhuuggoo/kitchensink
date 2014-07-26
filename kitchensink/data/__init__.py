from os.path import join, exists, isdir, relpath, abspath, dirname
import datetime as dt
import posixpath
import logging
import tempfile
from os import stat, makedirs
import random
import uuid
import cStringIO
import time

from six import string_types

from ..clients.http import Client
from .. import settings
from ..serialization import deserializer, serializer
from ..errors import KitchenSinkError
from ..utils.pathutils import urlsplit, dirsplit, urljoin

logger = logging.getLogger(__name__)

def hosts():
    if settings.prefix:
        host_key = settings.prefix + ":" + "hosts"
        hostinfo_key = settings.prefix + ":" + "hostinfo"
    else:
        host_key = "hosts"
        hostinfo_key = "hostinfo"
    possible_hosts = settings.redis_conn.smembers(host_key)
    hosts = settings.redis_conn.mget(*possible_hosts)
    hosts = [x for x in hosts if x is not None]
    return hosts

class Catalog(object):
    def __init__(self, connection, datadir, host_url, prefix=None):
        """connection - redis connection
        host_url - url of current controller/host
        """
        if prefix is None:
            prefix = settings.prefix
        self.prefix = ""
        self.conn = connection
        self.datadir = datadir
        self.host_url = host_url

    def path_key(self, key):
        if self.prefix:
            return self.prefix + ":data:path:" + key
        else:
            return "data:path:" + key

    def starting_key(self, key):
        if self.prefix:
            return self.prefix + ":datastarting:path:" + key
        else:
            return "datastarting:path:" + key

    def data_key(self, key):
        if self.prefix:
            return self.prefix + ":data:info:" + key
        else:
            return "data:info:" + key

    def search(self, pattern):
        if self.prefix:
            prefix = self.prefix + ":data:info:"
        else:
            prefix = self.prefix + "data:info:"
        pattern = prefix + pattern
        ### implement this with scan later
        keys = self.conn.keys(pattern)
        return [x[len(prefix):] for x in keys]

    def _raw_write(self, finput, f):
        if not isinstance(finput, string_types):
            while True:
                data = finput.read(settings.chunk_size)
                if data:
                    f.write(data)
                else:
                    break
        else:
            f.write(finput)
    def setup_file_path_from_url(self, url):
        splits = urlsplit(url, "")
        file_path = abspath(join(self.datadir, *splits))
        if not file_path.startswith(self.datadir):
            raise KitchenSinkError, "Must be inside datadir"
        if not exists(dirname(file_path)):
            makedirs(dirname(file_path))
        return file_path

    def write(self, finput, url, is_new=True):
        file_path = self.setup_file_path_from_url(url)
        if is_new:
            if self.url_exists(url):
                raise KitchenSinkError("path already being used")
            else:
                self.init_addition(file_path, url)

        with open(file_path, "wb+") as f:
            self._raw_write(finput, f)
        if is_new:
            size = stat(file_path).st_size
            self.set_metadata(url, size)
        self.add(file_path, url)
        return file_path

    def write_chunked(self, iterator, url, is_new=True):
        file_path = self.setup_file_path_from_url(url)
        if is_new:
            if self.url_exists(url):
                raise KitchenSinkError("path already being used")
            else:
                self.init_addition(file_path, url)
        with open(file_path, "wb+") as f:
            for chunk in iterator:
                f.write(chunk)
        if is_new:
            size = stat(file_path).st_size
            self.set_metadata(url, size)
        self.add(file_path, url)
        return file_path

    def url_exists(self, url):
        start_key = self.starting_key(url)
        path_key = self.path_key(url)
        data_key = self.data_key(url)
        return self.conn.exists(start_key) or \
            self.conn.exists(path_key) or \
            self.conn.exists(data_key)

    def add(self, file_path, url):
        path_key = self.path_key(url)
        self.conn.hset(path_key, self.host_url, file_path)
        return file_path

    def init_addition(self, file_path, url):
        start_key = self.starting_key(url)
        self.conn.hset(start_key, self.host_url, file_path)

    def set_metadata(self, url, size):
        data_key = self.data_key(url)
        self.conn.hset(data_key, 'size', str(size))

    def get_chunked_iterator(self, url, length, host_url=None):
        if host_url is None:
            host_url = self.host_url
        data_read = 0
        c = Client(host_url, rpc_name='data', queue_name='data')
        while True:
            if data_read == length:
                break
            #logger.info("%s, %s, %s", url, data_read, settings.chunk_size)
            data = c._get_data(url, data_read, settings.chunk_size)
            if data.status_code != 200:
                raise KitchenSinkError("http error")
            data = data.raw.read()
            data_read += len(data)
            logger.info ("read %s of %s from %s to %s" % (data_read, length,
                                                          host_url, settings.host_url))
            if len(data) == 0:
                time.sleep(1.0)
            yield data

    def get(self, url, host=None, chunked_read=False):
        hosts_info, data_info = self.get_info(url)
        if self.host_url in hosts_info:
            return hosts_info[self.host_url]
        else:
            if not chunked_read:
                host = hosts_info.keys()[0]
                c = Client(host, rpc_name='data', queue_name='data')
                stream = c._get_data(url).raw
            return self.write(stream, url, is_new=False)

    def get_info(self, url):

        hosts_info = self.conn.hgetall(self.path_key(url))
        data_info = self.conn.hgetall(self.data_key(url))
        if 'size' in data_info:
            data_info['size'] = int(data_info['size'])
        return (hosts_info, data_info)

    def get_file_path(self, url, unfinished=False):
        if unfinished:
            return self.setup_file_path_from_url(url)
        hosts_info, data_info = self.get_info(url)
        file_path = hosts_info[self.host_url]
        if not file_path.startswith(self.datadir):
            raise KitchenSinkError, "Must be inside datadir"
        return file_path

## should factor this out into a separate module
class RemoteData(object):
    def __init__(self, obj=None, local_path=None, data_url=None, rpc_url=None,
                 fmt="cloudpickle"):
        if rpc_url is None:
            rpc_url = settings.data_rpc_url
        self.rpc_url = rpc_url
        self.data_url = data_url
        self.fmt = fmt
        self._obj = obj
        self._local_path = local_path
        self._raw = None

    def __setstate__(self, obj):
        self.data_url = obj['data_url']
        self.fmt = obj['fmt']
        self.rpc_url = settings.data_rpc_url
        self._obj = None
        self._local_path = None
        self._raw = None

    def __getstate__(self):
        return dict(data_url=self.data_url,
                    fmt=self.fmt)

    def client(self, rpc_url=None):
        if rpc_url is None:
            rpc_url = self.rpc_url
        return Client(rpc_url, rpc_name='data', queue_name='data')

    def _get(self):
        if settings.catalog:
            return settings.catalog.get(self.data_url)
        c = self.client(self.rpc_url)
        url = c.pick_host(self.data_url)
        resp = self.client(url)._get_data(self.data_url)
        return resp.raw

    def _put(self, f):
        logger.info("posting %s to %s", self.data_url, self.rpc_url)
        f.seek(0)
        c = self.client(self.rpc_url)
        return c._put_data(self.data_url, f)

    def local_path(self, path=None):
        """provides a path to a local file that contains the
        contents of this remote data source (downloads if necessary)
        """
        if self._local_path:
            return self._local_path
        name = tempfile.NamedTemporaryFile(prefix="ks-data-").name
        if self._raw:
            with open(name, "w+") as f:
                f.write(self._raw)
        else:
            stream = self._get()
            #not great, but this means we're getting back the file path
            if isinstance(stream, string_types):
                name = stream
                return name
            name = tempfile.NamedTemporaryFile(prefix="ks-data-").name
            with open(name, "w+") as f:
                while True:
                    data = stream.read(settings.chunk_size)
                    if data:
                        f.write(data)
                    else:
                        break
        self._local_path = name
        return name

    def raw(self):
        """provides raw contents of the remote data
        """
        if self._raw:
            return self._raw
        raw = None
        if self._local_path:
            with open(self._local_path) as f:
                raw = f.read()
        else:
            raw = self._get()
            if isinstance(raw, string_types):
                name = raw
                with open(name, "r") as f:
                    raw = f.read()
            else:
                raw = raw.read()
        self._raw = raw
        return raw

    def obj(self):
        if self._obj is not None:
            return self._obj
        else:
            #should be able to pass a file in later
            obj = deserializer(self.fmt)(self.raw())
            self._obj = obj
            return obj

    def delete(self):
        raise NotImplementedError

    def save(self, url=None, prefix=""):
        """use this function to save a NEW data object
        """
        if self.data_url:
            raise KitchenSinkError, "Dataset is already created, cannot be modified"
        if url is None:
            self.data_url = urljoin(prefix, str(uuid.uuid4()))
        else:
            self.data_url = url
        try:
            if self._raw:
                f = cStringIO.StringIO()
                f.write(self._raw)
            elif self._local_path:
                f = open(self._local_path, "rb")
            else:
                f = cStringIO.StringIO()
                data = serializer(self.fmt)(self._obj)
                f.write(data)
            self._put(f)
        except Exception as e:
            f.close()
            logger.exception(e)

    def pipeline(self, existing=False, url=None, prefix=""):
        if self.data_url and not existing:
            raise KitchenSinkError, "Dataset is already created, cannot be modified"
        if url is None:
            self.data_url = urljoin(prefix, str(uuid.uuid4()))
        else:
            self.data_url = url
        try:
            if self._raw:
                f = cStringIO.StringIO()
                f.write(self._raw)
                length = len(self._raw)
            elif self._local_path:
                f = open(self._local_path)
                length = stat(file_path).self._local_path
            else:
                f = cStringIO.StringIO()
                data = serializer(self.fmt)(self._obj)
                f.write(data)
                length = len(data)
            c = self.client()
            hosts = c.call('hosts', _async=False)
            if existing:
                # remove the host that has the data from the chain
                # and add it to the end of the pipeline
                host = c.pick_host(self.data_url)
            else:
                host = self.rpc_url
            hosts = [x for x in hosts if x != host]
            hosts.append(host)
            print hosts
            calls = []
            for idx in range(1, len(hosts)):
                result = c.call('chunked_copy', self.data_url, length, hosts[idx],
                                _queue_name=hosts[idx - 1],
                )
                calls.append(result)
            self._put(f)
            print c.bulk_async_result(calls)
        except Exception as e:
            f.close()
            logger.exception(e)
