import logging

from .. import settings

logger = logging.getLogger(__name__)
class Servers(object):
    """Class to manage redis data structures
    surrounding which hosts/servers are active
    and how to connect to them
    - host info key, mapping host to url
    - also the catalog of all available hosts
    - active host key expires when host is down
    """
    def __init__(self, conn):
        self.conn = conn
        self.prefix = settings.prefix
        self.hosts = None

    def host_info_key(self):
        if self.prefix:
            return self.prefix + ":" + "host"
        else:
            return "host"

    def active_host_key(self, hostname):
        if self.prefix:
            return self.prefix + ":" + "active" + ":" + hostname
        else:
            return "active" + ":" + hostname

    def register(self, hostname, hosturl):
        logger.info('REGISTER %s %s', hostname, hosturl)
        self.conn.hset(self.host_info_key(), hostname, hosturl)
        self.conn.set(self.active_host_key(hostname), 'active')

    def remove(self, hostname):
        self.conn.hdel(self.host_info_key(), hostname)
        self.conn.delete(self.active_host_key(hostname))

    def active_loop(self, hostname):
        self.conn.set(self.active_host_key(hostname), 'active')
        self.conn.expire(self.active_host_key(hostname), settings.timeout)

    def host_url(self, hostname):
        return self.conn.hget(self.host_info_key(), hostname)

    def active_hosts(self):
        host_info = self.conn.hgetall(self.host_info_key())
        all_host_names = host_info.keys()
        active_host_keys = [self.active_host_key(x) for x in all_host_names]
        active_hosts = self.conn.mget(active_host_keys)
        temp = zip(all_host_names, active_hosts)
        active_hosts = [host_name for host_name, active_flag in temp \
                        if active_flag]
        active_hosts = set(active_hosts)
        for k in host_info.keys():
            if k not in active_hosts:
                host_info.pop(k)
        return host_info
