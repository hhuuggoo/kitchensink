import atexit
import time
from threading import Thread
try:
    import gevent
except:
    gevent = None
import redis
import redis.connection
from rq import Queue, Connection

from .app import app, rpcblueprint
from . import views
from ..taskqueue import TaskQueue
from .. import settings
from ..data import Catalog, Servers

def get_queue(name):
    if not name in rpcblueprint.queues:
        with Connection(rpcblueprint.r):
            queue = Queue(name)
            rpcblueprint.queues[name] = queue
    return rpcblueprint.queues[name]

def make_app(redis_connection_obj, port, host_url, host_name, datadir, read_only):
    app.register_blueprint(rpcblueprint, url_prefix="/rpc")
    app.port = port
    if gevent:
        redis.connection.socket = gevent.socket
    rpcblueprint.r = redis.StrictRedis(host=redis_connection_obj['host'],
                                       port=redis_connection_obj['port'],
                                       db=redis_connection_obj['db'])
    rpcblueprint.task_queue = TaskQueue(rpcblueprint.r)
    server_manager = Servers(rpcblueprint.r)
    settings.setup_server(rpcblueprint.r, datadir, host_url, host_name,
                          Catalog(rpcblueprint.r, datadir, host_name),
                          server_manager,
                          _read_only=read_only
    )
    rpcblueprint.heartbeat_thread = HeartbeatThread()
    return app

def register_rpc(rpc, name='default'):
    rpcblueprint.rpcs[name] = rpc
    rpc.setup_queue(rpcblueprint.task_queue)

def close():
    rpcblueprint.heartbeat_thread.kill = True
    rpcblueprint.heartbeat_thread.join()

def run(gevent=False):
    app.debug = True
    settings.server_manager.register(settings.host_name, settings.host_url, settings.read_only)
    rpcblueprint.heartbeat_thread.start()
    atexit.register(close)
    if gevent:
         from gevent.pywsgi import WSGIServer
         http_server = WSGIServer(("0.0.0.0", app.port), app)
         http_server.serve_forever()
    else:
        app.run(host='0.0.0.0', port=app.port, use_reloader=False)
    close()

class HeartbeatThread(Thread):
    def run(self):
        self.kill = False
        def loop():
            settings.server_manager.active_loop(settings.host_name)
        def remove():
            settings.server_manager.remove(settings.host_name)
        loop()
        while True:
            if self.kill:
                break
            else:
                loop()
                time.sleep(1)
        remove()
