import logging
import sys

from ..rpc import RPC
from .. import settings

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

def make_rpc():
    rpc = RPC()
    rpc.register_function(get_info)
    rpc.register_function(search_path)
    rpc.register_function(hosts)
    rpc.register_function(chunked_copy)
    return rpc
