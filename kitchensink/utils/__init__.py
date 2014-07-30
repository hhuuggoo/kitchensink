from six.moves.urllib.parse import urlparse, parse_qs, urlencode
from logging.config import dictConfig
import logging
try:
    import gevent
except:
    gevent = None

def parse_redis_connection(url):
    split = urlparse(url)
    protocol = split.scheme
    netloc = split.netloc
    path = split.path
    db = int(parse_qs(split.query)['db'][0])
    if protocol == 'tcp':
        host, port = netloc.split(":")
        port = int(port)
        return {'protocol' : protocol,
                'host' : host,
                'port' : port,
                'db' : db}
    elif protocol == 'unix':
        #not supported yet
        return {'protocol' : protocol,
                'path' : path,
                'db' : db}

def make_query_url(url, data):
    qs = urlencode(data)
    return url + "?" + qs

def update_dictionaries(*dictionaries):
    result = {}
    for d in dictionaries:
        result.update(d)
    return result

def send_file(file_or_buffer):
    from flask import Response
    chunksize=10000
    def generator():
        with open(file_or_buffer, "rb") as f:
            while True:
                result = f.read(chunksize)
                if not result:
                    break
                else:
                    yield result
                    if gevent:
                        gevent.sleep(0)
    return Response(generator(),
                    mimetype='application/octet-stream')
def setup_loghandlers(level=None):
    if not logging._handlers:
        dictConfig({
            'version': 1,
            'disable_existing_loggers': False,

            'formatters': {
                'console': {
                    'format': "%(asctime)-15s %(name)s:%(lineno)s:%(message)s"
                },
            },

            'handlers': {
                'console': {
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'console',
                },
            },

            'root': {
                'handlers': ['console'],
                'level': level or 'INFO',
            }
        })
