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
from .catalog import _write, _read
logger = logging.getLogger(__name__)



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
    def __init__(self, obj=None, local_path=None, data_url=None,
                 raw=None,
                 rpc_url=None,
                 fmt="cloudpickle"):
        if rpc_url is None:
            rpc_url = settings.data_rpc_url
        self.rpc_url = rpc_url
        self.data_url = data_url
        self.fmt = fmt
        self._obj = obj
        self._local_path = local_path
        self._raw = raw

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
        node, url = c.pick_host(self.data_url)
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
            _write(self._raw, name)
            self._local_path = name
            return name
        ### if we have an in memory object, serialize it, write to a file
        ### and return it
        if self._obj is not None:
            name = tempfile.NamedTemporaryFile(prefix="ks-data-").name
            data = serializer(self.fmt)(self._obj)
            _write(data, name)
            self._local_path = name
            return name
        ### grab the stream, write to temporary file, return the path
        stream = self._get_stream()
        try:
            name = tempfile.NamedTemporaryFile(prefix="ks-data-").name
            _write(stream, name)
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

        if self._obj is not None:
            self._raw = serializer(self.fmt)(self._obj)
            return self._raw
        stream = None
        try:
            stream = self._get_stream()
            self._raw = stream.read()
            return self._raw
        finally:
            if stream:
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
            logger.error("error with %s on %s raw",
                         self.data_url,
                         settings.data_rpc_url,
            )
            logger.exception(e)
            raise

    def delete(self):
        raise NotImplementedError

    def _save_stream(self):
        if self._raw:
            return len(self._raw), StringIO(self._raw)
        elif self._local_path:
            length = stat(self._local_path).st_size
            return length, open(self._local_path, "rb")
        else:
            data = serializer(self.fmt)(self._obj)
            return len(data), StringIO(data)

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
        finally:
            f.close()

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
            client = self._pipeline_existing(starting_url=self.rpc_url, length=length)
            self._put(f)
            return client.br()
        finally:
            f.close()

    def _pipeline_existing(self, starting_host=None, starting_url=None, length=None):
        c = self.client()
        active_hosts, results = c.data_info([self.data_url])
        if starting_host is None:
            #ugly...
            from ..utils.funcs import reverse_dict
            starting_host = reverse_dict(active_hosts)[starting_url]
        location_info, data_info = results[self.data_url]
        if length is None:
            length = data_info['size']
        hosts = set(active_hosts.keys())
        hosts = [x for x in hosts if x not in location_info]
        hosts = [x for x in hosts if x != starting_host]
        hosts.append(starting_host)
        print hosts
        for idx in range(1, len(hosts)):
            queue = c.queue('data', host=hosts[idx - 1])
            c.bc('chunked_copy', self.data_url, length, hosts[idx],
                 _queue_name=queue,
            )
        c.execute()
        return c

    def pipeline_existing(self):
        c = self.client()
        node, url = c.pick_host(self.data_url)
        client = self._pipeline_existing(node)
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
            obj = do(self.obj()[arg])
            obj.save()
            return obj
        else:
            c = Client(self.rpc_url)
            return c.async_result(c.call(self.__class__.__getitem__, self, arg))

def du(url):
    return RemoteData(data_url=url)
def dp(path):
    return RemoteData(local_path=path)
def do(obj):
    return RemoteData(obj=obj)
def dr(raw):
    return RemoteData(raw=raw)
