from .app import app, nodeblueprint
import redis

def make_app(redis_connection_obj, port):
    app.register_blueprint(nodeblueprint, url_prefix="/node")
    app.port = port
    nodeblueprint.r = redis.StrictRedis(host=redis_connection_obj['host'],
                                        port=redis_connection_obj['port'],
                                        db=redis_connection_obj['db'])
    return app

def run():
    app.debug = True
    app.run(host='0.0.0.0', port=app.port)
