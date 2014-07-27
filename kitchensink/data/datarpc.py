import logging
import sys

from ..rpc import RPC
from .. import settings
from . import hosts

logger = logging.getLogger(__name__)
def get_info(path):
    #TODO: check against availble servers
    #TODO: pipeline redis operations
    host_info, data_info = settings.catalog.get_info(path)
    return host_info, data_info

def search_path(path_pattern):
    return settings.catalog.search(path_pattern)

def chunked_copy(url, length, host):
    #print >> sys.stderr ,"chunked copy of %s from %s to %s" % (url, host, settings.host_url)
    logger.info("chunked copy of %s from %s to %s" % (url, host, settings.host_url))
    iterator = settings.catalog.get_chunked_iterator(url, length, host)
    settings.catalog.write_chunked(iterator, url, is_new=False)

def delete(url):
    settings.catalog.delete(url)

def make_rpc():
    rpc = RPC()
    rpc.register_function(get_info)
    rpc.register_function(search_path)
    rpc.register_function(hosts)
    rpc.register_function(chunked_copy)
    rpc.register_function(delete)
    return rpc
