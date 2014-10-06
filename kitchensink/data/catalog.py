from os.path import join, exists, isdir, relpath, abspath, dirname
import datetime as dt
import posixpath
import logging
import tempfile
from os import stat, makedirs, remove
import random
import uuid
from cStringIO import StringIO
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
from .funcs import get_info_bulk, hosts

logger = logging.getLogger(__name__)


def _write(finput, f):
    if isinstance(finput, string_types):
        finput = StringIO(finput)
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
    """
    location_key - set of which host names contain this data url
    info_key - hashset, metadata about the file
    one of these keys, 'state', is either 'starting', or 'ready'
    presence of the location_key indicates that the dataset exists in the system
    where as info_key will have state set to 'starting' if it's just pending
    """
    def __init__(self, connection, datadir, hostname, prefix=None):
        """connection - redis connection
        hostname - name
        """
        if prefix is None:
            prefix = settings.prefix
        self.prefix = ""
        self.conn = connection
        self.datadir = datadir
        self.hostname = hostname

    def host_url(self, hostname=None):
        if hostname is None:
            hostname = self.hostname
        return settings.server_manager.host_url(hostname)

    def location_key(self, key):
        """redis key for storing local paths for remote data sources
        """
        if self.prefix:
            return self.prefix + ":data:path:" + key
        else:
            return "data:path:" + key

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
        prefix = self.location_key('')
        pattern = self.location_key(pattern)
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

    def bootstrap(self, url, data_type='object', fmt='cloudpickle'):
        file_path = self.setup_file_path_from_url(url)
        if self.url_exists(url):
            is_new = False
        else:
            is_new = True
        if is_new:
            size = stat(file_path).st_size
            self.set_metadata(url, size, data_type=data_type, fmt=fmt)
        self.add(file_path, url)

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
                raise KitchenSinkError("path already being used %s" % url)
            else:
                self.begin_addition(file_path, url)
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
        location_key = self.location_key(url)
        data_key = self.data_key(url)
        self.conn.srem(location_key, self.hostname)
        if not self.conn.exists(location_key):
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
                self.begin_addition(file_path, url)
        with open(file_path, "wb+") as f:
            for chunk in iterator:
                f.write(chunk)
        if is_new:
            size = stat(file_path).st_size
            self.set_metadata(url, size, data_type=data_type)
        self.add(file_path, url)
        return file_path

    def url_exists(self, url):
        location_key = self.location_key(url)
        return self.conn.exists(location_key)

    def add(self, file_path, url):
        """add a url to the catalog
        """
        location_key = self.location_key(url)
        self.conn.sadd(location_key, self.hostname)
        return file_path

    def begin_addition(self, file_path, url):
        """declare our intention that we are beginnning to add
        a data url to the catalog
        """
        self.conn.hset(self.data_key(url), 'state', 'starting')

    def set_metadata(self, url, size, data_type="object", fmt="cloudpickle"):
        data_key = self.data_key(url)
        self.conn.hmset(data_key,
                        {'state' : 'ready',
                         'size' : str(size),
                         'data_type' : data_type,
                         'fmt' : fmt}
        )

    def get_chunked_iterator(self, url, length, hostname=None, host_url=None):
        """return a chunked iterator for the contents of a file
        at another host.  This is used for pipelining, so that
        we can stream data in while it's being written
        """
        if hostname is None:
            hostname = self.hostname
        if host_url is None:
            host_url = self.host_url(hostname=hostname)
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

    def get(self, url, hostname=None, host_url=None):
        """returns a stream for the given data url
        it is up to the caller of this to close the stream!
        """
        if hostname is None:
            hostname = self.hostname
        if host_url is None:
            host_url = self.host_url(hostname=hostname)
        hosts, location_info, data_info = self.get_info(url)
        if self.hostname in location_info:
            file_path = self.setup_file_path_from_url(url)
            return open(file_path, 'rb')
        else:
            hostname = random.sample(location_info, 1)[0]
            host_url = hosts[hostname]
            logger.info("retrieving %s from %s", url, host_url)
            c = Client(host_url, rpc_name='data', queue_name='data')
            return c._get_data(url).raw

    def _get_info(self, url):
        """retrieves all information about a data url
        does not take into account whether hosts are active,
        etc..
        """
        location_info = self.conn.smembers(self.location_key(url))
        data_info = self.conn.hgetall(self.data_key(url))
        if 'size' in data_info:
            data_info['size'] = int(data_info['size'])
        return (location_info, data_info)

    def get_info(self, url):
        hosts , results = get_info_bulk([url])
        location_info, data_info = results[url]
        return hosts, location_info, data_info

    def get_file_path(self, url, unfinished=False):
        """retrieve file path for the url.
        unfinished means the data is not in the catalog yet
        (we're pipelining it)
        if the url does not exist on this host, return None
        """
        path = self.setup_file_path_from_url(url)
        if unfinished and exists(path):
            return path
        hosts, location_info, data_info = self.get_info(url)
        if self.hostname not in location_info:
            return None
        return path
