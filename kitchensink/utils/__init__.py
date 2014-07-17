from six.moves.urllib.parse import urlparse, parse_qs, urlencode
def parse_redis_connection(url):
    split = urlparse("tcp://localhost:6379?db=9")
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
