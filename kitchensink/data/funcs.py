import logging
import sys

from .. import settings

logger = logging.getLogger(__name__)

def hosts(to_write=False):
    """if to_write is True, then only non-read-only hosts
    will be returned
    """
    return settings.server_manager.active_hosts(to_write=to_write)

def get_info_bulk(urls):
    """
    active_hosts - dict of active hostnames to host urls
    results - dict of url -> data for each url
    each value is a tuple (location_info - which hosts have the data)
    and data_info, metadata about the url, (file size, md5s, etc..)
    """
    results = {}
    active_hosts = hosts()
    for u in urls:
        location_info, data_info = settings.catalog._get_info(u)
        for host in list(location_info):
            if host not in active_hosts:
                location_info.remove(host)
        results[u] = location_info, data_info
    return active_hosts, results

def search_path(path_pattern):
    return settings.catalog.search(path_pattern)

def chunked_copy(url, length, host):
    #print >> sys.stderr ,"chunked copy of %s from %s to %s" % (url, host, settings.host_url)
    logger.info("chunked copy of %s from %s to %s" % (url, host, settings.host_url))
    iterator = settings.catalog.get_chunked_iterator(url, length, hostname=host)
    settings.catalog.write_chunked(iterator, url, is_new=False)

def delete(url):
    settings.catalog.delete(url)

def bootstrap(url, data_type='object', fmt='cloudpickle'):
    settings.catalog.bootstrap(url, data_type=data_type, fmt=fmt)
