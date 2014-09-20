from os.path import join, exists, isdir, relpath, abspath, dirname
import datetime as dt
import posixpath
import logging
import tempfile
from os import stat, makedirs, remove
import random
import uuid
import cStringIO
import time

from six import string_types
try:
    import gevent
except:
    gevent = None
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

def _write(finput, f):
    if isinstance(finput, string_types):
        finput = cStringIO(finput)
    if isinstance(f, string_types):
        f = open(f, 'wb+')
    try:
        while True:
            data = finput.read(settings.chunk_size)
            if data:
                f.write(data)
                if gevent:
                    gevent.sleep(0)
            else:
                break
    finally:
        finput.close()
        f.close()

def _read(path):
    with open(path, "rb") as f:
        return f.read()

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
        """redis key for storing local paths for remote data sources
        """
        if self.prefix:
            return self.prefix + ":data:path:" + key
        else:
            return "data:path:" + key

    def starting_key(self, key):
        """redis key for storing information about
        beginning to write a remote data source
        """
        if self.prefix:
            return self.prefix + ":datastarting:path:" + key
        else:
            return "datastarting:path:" + key

    def data_key(self, key):
        """redis key for metadata about the remote data source
        (filesizes, datatype, format)
        """
        if self.prefix:
            return self.prefix + ":data:info:" + key
        else:
            return "data:info:" + key

    def search(self, pattern):
        """search the catalog for remote data urls
        currently implemented with redis.keys which
        isn't recommended for production use
        """
        if self.prefix:
            prefix = self.prefix + ":data:info:"
        else:
            prefix = self.prefix + "data:info:"
        pattern = prefix + pattern
        ### implement this with scan later
        keys = self.conn.keys(pattern)
        return [x[len(prefix):] for x in keys]

    def setup_file_path_from_url(self, url):
        """given a url, setup the file path in the data directory
        ensuring that necessary subdirs are created, and that the
        file path is valid
        """
        splits = urlsplit(url, "")
        file_path = abspath(join(self.datadir, *splits))
        if not file_path.startswith(self.datadir):
            raise KitchenSinkError, "Must be inside datadir"
        if not exists(dirname(file_path)):
            makedirs(dirname(file_path))
        return file_path

    def write(self, finput, url, is_new=True, data_type="object", fmt="cloudpickle"):
        """writes a file, to a data url.
        is_new - is this a new object in a catalog, or a copy of an existing one.
        data_type - data_type of the object (object or file)
        fmt - one of our serialization formats
        if the targetfile path exists - do nothing (this should't happen, maybe we should throw
        an error)
        if this file is new, then write the metadata into the catalog
        otherwise just write the data to local disk
        """
        file_path = self.setup_file_path_from_url(url)
        if is_new:
            if self.url_exists(url):
                raise KitchenSinkError("path already being used")
            else:
                self.init_addition(file_path, url)
        if not exists(file_path):
            _write(finput, file_path)
        if is_new:
            size = stat(file_path).st_size
            self.set_metadata(url, size, data_type=data_type, fmt=fmt)
        self.add(file_path, url)
        return file_path

    def delete(self, url):
        """delete the data url from this node only.  to truly remove it
        you need to delete it from all nodes
        """
        start_key = self.starting_key(url)
        path_key = self.path_key(url)
        data_key = self.data_key(url)
        self.conn.hdel(path_key, self.host_url)
        self.conn.hdel(start_key, self.host_url)
        if not self.conn.exists(path_key):
            self.conn.delete(data_key)
        file_path = self.setup_file_path_from_url(url)
        remove(file_path)

    def write_chunked(self, iterator, url, is_new=True):
        """same as chunked, but write data from an iterator
        (we use chunked reads during pipelining data, so we can
        stream data in as it's being processed)
        """
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
            self.set_metadata(url, size, data_type=data_type)
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
        """add a url to the catalog
        """
        path_key = self.path_key(url)
        self.conn.hset(path_key, self.host_url, file_path)
        return file_path

    def init_addition(self, file_path, url):
        """declare our intention that we are beginnning to add
        a data url to the catalog
        """
        start_key = self.starting_key(url)
        self.conn.hset(start_key, self.host_url, file_path)

    def set_metadata(self, url, size, data_type="object", fmt="cloudpickle"):
        data_key = self.data_key(url)
        self.conn.hmset(data_key, {'size' : str(size),
                                   'data_type' : data_type,
                                   'fmt' : fmt}
        )

    def get_chunked_iterator(self, url, length, host_url=None):
        """return a chunked iterator for the contents of a file
        at another host.  This is used for pipelining, so that
        we can stream data in while it's being written
        """
        if host_url is None:
            host_url = self.host_url
        data_read = 0
        c = Client(host_url, rpc_name='data', queue_name='data')
        while True:
            if data_read == length:
                break
            data = c._get_data(url, data_read, settings.chunk_size)
            if data.status_code != 200:
                raise KitchenSinkError("http error")
            data = data.raw.read()
            data_read += len(data)
            logger.debug ("read %s of %s from %s to %s" % (data_read, length,
                                                          host_url, settings.host_url))
            if gevent:
                gevent.sleep(0)
            if len(data) == 0:
                time.sleep(1.0)
            yield data

    def get(self, url, host=None):
        """returns a stream for the given data url
        it is up to the caller of this to close the stream!
        """
        hosts_info, data_info = self.get_info(url)
        if self.host_url in hosts_info:
            return open(hosts_info[self.host_url], 'rb')
        else:
            host = hosts_info.keys()[0]
            logger.info("retrieving %s from %s", url, host)
            c = Client(host, rpc_name='data', queue_name='data')
            return c._get_data(url).raw

    def get_info(self, url):
        hosts_info = self.conn.hgetall(self.path_key(url))
        data_info = self.conn.hgetall(self.data_key(url))
        if 'size' in data_info:
            data_info['size'] = int(data_info['size'])
        return (hosts_info, data_info)

    def get_file_path(self, url, unfinished=False):
        """retrieve file path for the url.
        unfinished means the data is not in the catalog yet
        (we're pipelining it)
        if the url does not exist on this host, return None
        """
        if unfinished:
            path = self.setup_file_path_from_url(url)
            if not exists(path):
                return None
        hosts_info, data_info = self.get_info(url)
        try:
            file_path = hosts_info[self.host_url]
        except KeyError:
            return None
        if not file_path.startswith(self.datadir):
            raise KitchenSinkError, "Must be inside datadir"
        return file_path

## should factor this out into a separate module
class RemoteData(object):
    """RemoteData objects can contain raw data (in memory, in self._raw,
    or on disk, in self._local_path.  Or as a deserialized object, in
    self._obj.

    Maybe this isn't the greatest approach, mixing them all together in one object
    but that's what we have for now.  Currently, if you ask for self.obj(), or
    self.local_path(), or self.raw(), we will retrieve it from what we perceive
    to be the cheapest source.  For example, if you have the stream in memory
    (self._raw), and you ask for the local path, we will write it to a temp file
    and return you that path
    """
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

    def _get_stream(self):
        """returns either the stream associated with a data_url
        """
        if settings.catalog:
            return settings.catalog.get(self.data_url)
        c = self.client(self.rpc_url)
        url = c.pick_host(self.data_url)
        resp = self.client(url)._get_data(self.data_url)
        return resp.raw

    def _put(self, f, data_type='object', fmt="cloudpickle"):
        logger.debug("posting %s to %s", self.data_url, self.rpc_url)
        f.seek(0)
        if settings.catalog:
            settings.catalog.write(f, self.data_url, is_new=True, data_type=data_type,
                                   fmt=self.fmt)
        else:
            c = self.client(self.rpc_url)
            return c._put_data(self.data_url, f, data_type=data_type, fmt=fmt)

    def _existing_file_path(self):
        if settings.catalog:
            name = settings.catalog.get_file_path(self.data_url)
            if name is not None:
                self._local_path = name
                return name

    def local_path(self):
        """provides a path to a local file that contains the
        contents of this remote data source (downloads if necessary)
        """
        if self._local_path:
            return self._local_path
        path = self._existing_file_path()
        if path is not None:
            return path

        ### if we have the data in memory, write it to a local file and return it
        if self._raw:
            name = tempfile.NamedTemporaryFile(prefix="ks-data-").name
            _raw_write(self._raw, name)
            self._local_path = name
            return name
        ### if we have an in memory object, serialize it, write to a file
        ### and return it
        if self._obj:
            name = tempfile.NamedTemporaryFile(prefix="ks-data-").name
            data = serializer(self.fmt)(self._obj)
            _raw_write(data, name)
            self._local_path = name
            return name
        ### grab the stream, write to temporary file, return the path
        stream = self._get()
        try:
            name = tempfile.NamedTemporaryFile(prefix="ks-data-").name
            _raw_write(stream, name)
            self._local_path = name
            return name
        finally:
            stream.close()

    def raw(self):
        """provides raw contents of the remote data
        """
        if self._raw:
            return self._raw

        path = self._existing_file_path()
        if path is not None:
            self._raw = _read(path)
            return self._raw

        if self._local_path:
            self._raw = _read(self._local_path)
            return self._raw

        if self._obj:
            self._raw = serializer(self.fmt)(self._obj)
            return self._raw
        try:
            self._raw = self._get()
            return self._raw
        finally:
            stream.close()

    def obj(self):
        if self._obj is not None:
            return self._obj
        #should be able to pass a file in later
        try:
            raw = self.raw()
            obj = deserializer(self.fmt)(raw)
            self._obj = obj
            return obj
        except Exception as e:
            logger.error("error with %s on %s raw len %s",
                         self.data_url,
                         settings.data_rpc_url,
                         len(raw)
            )
            logger.exception(e)
            raise

    def delete(self):
        raise NotImplementedError

    def _save_stream(self):
        if self._raw:
            return len(self._raw), cStringIO.StringIO(self._raw)
        elif self._local_path:
            length = stat(file_path).st_size
            return length, open(self._local_path, "rb")
        else:
            data = serializer(self.fmt)(self._obj)
            return len(data), cStringIO.StringIO(data)

    def save(self, url=None, prefix=""):
        """use this function to save a NEW data object
        """
        if self.data_url:
            raise KitchenSinkError, "Dataset is already created, cannot be modified"
        if url is None:
            self.data_url = urljoin(prefix, str(uuid.uuid4()))
        else:
            self.data_url = url
        length, f = self._save_stream()
        try:
            data_type = "object" if self._obj is not None else None
            fmt = self.fmt if self._obj is not None else None
            self._put(f, data_type=data_type, fmt=fmt)
        except Exception as e:
            f.close()
            raise e

    def pipeline(self, existing=False, url=None, prefix=""):
        """pipeline this datasource across all nodes
        existing - means this object already exists in the system
        otherwise a new one will be saved and created
        """
        if existing:
            return self.pipeline_existing()
        if self.data_url and not existing:
            raise KitchenSinkError, "Dataset is already created, cannot be modified"
        if not self.data_url:
            if url is None:
                self.data_url = urljoin(prefix,
                                        str(uuid.uuid4()))
            else:
                self.data_url = url
        length, f = self._save_stream()
        try:
            c = self.client()
            host = self.rpc_url

            client = self._pipeline_existing(host, length=length)
            self._put(f)
            return c.br()
        except Exception as e:
            f.close()
            raise e

    def _pipeline_existing(self, starting_host, length=None):
        c = self.client()
        active_hosts, results = c.data_info(self, [self.data_url])
        host_info, data_info = results[self.data_url]
        if length is None:
            length = data_info['size']
        hosts = c.call('hosts', _async=False)
        hosts = [x for x in hosts if x not in host_info]
        hosts = [x for x in hosts if x != starting_host]
        hosts.append(starting_host)
        print hosts
        calls = []
        for idx in range(1, len(hosts)):
            c.bc('chunked_copy', self.data_url, length, hosts[idx],
                 _queue_name=hosts[idx - 1],
            )
        c.execute()
        return c

    def pipeline_existing(self):
        c = self.client()
        host = c.pick_host(self.data_url)
        client = self._pipeline_existing(host)
        client.execute()
        return client.br()

    def __repr__(self):
        if self.data_url:
            return "RemoteData(data_url='%s')" % self.data_url
        elif self._local_path:
            return "RemoteData(local_path='%s')" % self._local_path
        else:
            return "RemoteData(obj=obj)"

    def __getitem__(self, arg):
        if settings.catalog:
            print ("loading direct")
            obj = do(self.obj()[arg])
            obj.save()
            return obj
        else:
            c = Client(self.rpc_url)
            return c.async_result(c.call(self.__class__.__getitem__, self, arg))

    __getitem__.ks_memoize = True

def du(url):
    return RemoteData(data_url=url)
def dp(path):
    return RemoteData(local_path=path)
def do(obj):
    return RemoteData(obj=obj)
