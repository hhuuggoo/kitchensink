from six.moves.urllib.parse import urlparse, parse_qs

def parse_redis_connection(url):
    split = urlparse("tcp://localhost:6379?db=9")
    protocol = split.scheme
    netloc = split.netloc
    path = split.path
    db = int(parse_qs(split.query)['db'])
    if protocol == 'tcp':
        host, port = netloc.split(":")
        return {'protocol' : protocol,
                'host' : host,
                'port' : port,
                'db' : db}
    elif protocol == 'unix':
        #not supported yet
        return {'protocol' : protocol,
                'path' : path
                'db' : db}
