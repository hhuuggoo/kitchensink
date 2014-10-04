from .. import settings

class Servers(object):
    def __init__(self, conn):
        self.conn = conn
        self.prefix = settings.prefix
        self.hosts = None

    def host_key(self):
        if self.prefix:
            return self.prefix + ":" + "host"
        else:
            return "host"

    def active_flag_key(self, hostname):
        if self.prefix:
            return self.prefix + ":" + "active" + ":" + hostname
        else:
            return "active" + ":" + hostname

    def register(self, hostname, hosturl):
        self.conn.hset(self.host_key(), hostname, hosturl)

    def active_loop(self, hostname):
        self.conn.expire(self.active_flag_key(hostname), settings.timeout)

    def remove(self, hostname):
        self.conn.hdel(self.host_key(), hostname)

    def load_hosts(self):


    def active_hosts(self):
