import redis
from rq import Queue, Connection

from .app import app, rpcblueprint
from . import views
from ..taskqueue import TaskQueue
from .. import settings

def get_queue(name):
    if not name in rpcblueprint.queues:
        with Connection(rpcblueprint.r):
            queue = Queue(name)
            rpcblueprint.queues[name] = queue
    return rpcblueprint.queues[name]

def make_app(redis_connection_obj, port):
    app.register_blueprint(rpcblueprint, url_prefix="/rpc")
    app.port = port
    rpcblueprint.r = redis.StrictRedis(host=redis_connection_obj['host'],
                                       port=redis_connection_obj['port'],
                                       db=redis_connection_obj['db'])
    rpcblueprint.task_queue = TaskQueue(rpcblueprint.r)
    settings.redis_conn = rpcblueprint.r
    return app

def register_rpc(rpc, name='default'):
    rpcblueprint.rpcs[name] = rpc
    rpc.setup_queue(rpcblueprint.task_queue)


def run():
    app.debug = True
    app.run(host='0.0.0.0', port=app.port, use_reloader=False)
