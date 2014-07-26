import atexit
import time
from threading import Thread

import redis
from rq import Queue, Connection

from .app import app, rpcblueprint
from . import views
from ..taskqueue import TaskQueue
from .. import settings
from ..data import Catalog

def get_queue(name):
    if not name in rpcblueprint.queues:
        with Connection(rpcblueprint.r):
            queue = Queue(name)
            rpcblueprint.queues[name] = queue
    return rpcblueprint.queues[name]

def make_app(redis_connection_obj, port, host_url, datadir):
    app.register_blueprint(rpcblueprint, url_prefix="/rpc")
    app.port = port
    rpcblueprint.r = redis.StrictRedis(host=redis_connection_obj['host'],
                                       port=redis_connection_obj['port'],
                                       db=redis_connection_obj['db'])
    rpcblueprint.task_queue = TaskQueue(rpcblueprint.r)
    settings.setup_server(rpcblueprint.r, datadir, host_url,
                          Catalog(rpcblueprint.r, datadir, host_url))
    rpcblueprint.heartbeat_thread = HeartbeatThread()
    return app

def register_rpc(rpc, name='default'):
    rpcblueprint.rpcs[name] = rpc
    rpc.setup_queue(rpcblueprint.task_queue)

def close():
    rpcblueprint.heartbeat_thread.kill = True
    rpcblueprint.heartbeat_thread.join()

def run():
    app.debug = True
    rpcblueprint.heartbeat_thread.start()
    atexit.register(close)
    app.run(host='0.0.0.0', port=app.port, use_reloader=False)
    close()

class HeartbeatThread(Thread):
    def run(self):
        self.kill = False
        if settings.prefix:
            self.host_key = settings.prefix + ":" + "hosts"
            self.hostinfo_key = settings.prefix + ":" + "hostinfo:%s" %settings.host_url
        else:
            self.host_key = "hosts"
            self.hostinfo_key = "hostinfo:%s" %settings.host_url
        def loop():
            settings.redis_conn.sadd(self.host_key, self.hostinfo_key)
            settings.redis_conn.setex(self.hostinfo_key,
                                      settings.timeout,
                                      settings.host_url)
        def remove():
            settings.redis_conn.srem(self.host_key, self.hostinfo_key)
            settings.redis_conn.delete(self.hostinfo_key, settings.host_url)
        loop()
        while True:
            if self.kill:
                break
            else:
                loop()
                time.sleep(1)
        remove()
