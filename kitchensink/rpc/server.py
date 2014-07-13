import redis

from .app import app, rpcblueprint
from . import views
def make_app(redis_connection_obj, port):
    app.register_blueprint(rpcblueprint, url_prefix="/rpc")
    app.port = port
    rpcblueprint.r = redis.StrictRedis(host=redis_connection_obj['host'],
                                       port=redis_connection_obj['port'],
                                       db=redis_connection_obj['db'])
    return app

def register_rpc(rpc, name='default'):
    rpcblueprint.rpcs[name] = rpc

def run():
    app.debug = True
    app.run(host='0.0.0.0', port=app.port, use_reloader=False)
