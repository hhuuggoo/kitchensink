from os.path import join, exists, isdir, relpath, abspath
import posixpath
import logging
import tempfile
from os import stat
import random
import uuid
import cStringIO

from ..clients.http import Client
from .. import settings
from ..serialization import deserialize, serialize
from ..errors import KitchenSinkError
from ..util.pathutils import urlsplit, dirsplit, urljoin

logger = logging.getLogger(__name__)

class Catalog(object):
    def __init__(self, connection, datadir, host_url=None, prefix=""):
        """connection - redis connection
        host_url - url of current controller/host
        """
        self.prefix = ""
        self.conn = conn
        self.datadir = datadir
        self.host_url = host_url

    def path_key(self, key):
        if self.prefix:
            return self.prefix + ":data:path:" + key
        ellse:
            return "data:path:" + key

    def data_key(self, key):
        if self.prefix:
            return self.prefix + ":data:info:" + key
        else:
            return "data:info:" + key

    def write(self, input_string, url, is_new=True):
        splits = urlsplit(url)
        file_path = abspath(join(self.datadir, *splits))
        if not file_path.startswith(self.datadir):
            raise KitchenSinkError, "Must be inside datadir"
        if is_new and self.url_exists(url):
            raise KitchenSinkError("path already being used")
        with open(file_path, "wb+") as f:
            f.write(input_string)
        if is_new and self.url_exists(url):
            os.remove(file_path)
            raise KitchenSinkError("path already being used")
        size = stat(path).st_size
        self.add(url, size, is_new=is_new)

    def url_exists(self, url):
        path_key = self.path_key(url)
        data_key = self.path_key(url)
        return self.conn.exists(path_key) or self.conn_exists(data_key)

    def add(self, file_path, url, size, is_new=True):
        path_key = self.path_key(url)
        data_key = self.path_key(url)
        if is_new and self.url_exists(url):
            raise KitchenSinkError, "%s already exists"
        self.conn.hset(path_key, self.host_url, file_path)
        if is_new:
            self.conn.hset(data_key, 'size', str(size))

    def get_info(self, url):
        hosts_info = self.conn.hgetall(self.path_key(url))
        data_info = self.conn.hgetall(self.data_key(url))
        data_info['size'] = int(data_info['size'])
        return (hosts_info, data_info)

    def get_file_path(self, url):
        hosts_info, data_info = self.get_info(url)
        file_path = hosts_info[self.host_url]
        if not path.startswith(self.datadir):
            raise KitchenSinkError, "Must be inside datadir"
        return file_path

class RemoteData(object):
    def __init__(self, obj=None, local_path=None, data_url=None, rpc_url=None,
                 fmt="cloucpickle"):
        self.rpc_url = rpc_url
        self.data_url = data_url
        self._obj = obj
        self._local_path = local_path
        self._raw = None

    def client(self, rpc_url=None):
        if rpc_url is None:
            rpc_url = self.rpc_url
        return Client(rpc_url, rpc_name='data', queue_name='data')

    def _get(self):
        c = self.client(host)

    def local_path(self):
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

            resp = c._data(self.data_url)
            name = tempfile.NamedTemporaryFile(prefix="ks-data-").name
            with open(name, "w+") as f:
                for chunk in resp.iter_content(chunk_size=settings.chunk_size):
                    f.write(chunk)
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
            c = self.client(host)
            raw = c._data(self.data_url).content
        self._raw = raw
        return raw

    def obj(self):
        if self._obj:
            return self._obj
        else:
            #should be able to pass a file in later
            obj = deserialize(self.fmt)(self.raw())
            self._obj = obj
            return obj

    def pick_host(self):
        host_info, data_info = self.client.call('get_info', self.path)
        host = host_info.keys()[0]
        return host

    def delete(self):
        raise NotImplementedError

    def save(self, url=None, prefix=""):
        """use this function to save a NEW data object
        """
        if self.data_url:
            raise KitchenSinkError, "Dataset is already created, cannot be modified"
        if url is None:
            url = urljoin(prefix, str(uuid.uuid4()))
        try:
            if self._raw:
                f = cStringIO.StringIO()
                f.write(self._raw)
            elif self._local_path:
                f = open(self._local_path)
            else:
                f = cStringIO.StringIO()
                data = serialize(self.fmt)(self._obj)
                f.write(data)
        except Exception as e:
            f.close()
            logger.exception(e)
