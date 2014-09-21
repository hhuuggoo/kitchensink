from .serialization import json_serialization, dill_serialization, pickle_serialization

node_url = None
formats = {}
def register_serialization(name, serializer, deserializer):
    formats[name] = (serializer, deserializer)

def register_default_serialization():
    register_serialization('json',
                           json_serialization.serialize,
                           json_serialization.deserialize)
    register_serialization('dill',
                           dill_serialization.serialize,
                           dill_serialization.deserialize)
    register_serialization('pickle',
                           pickle_serialization.serialize,
                           pickle_serialization.deserialize)
register_default_serialization()

def serializer(fmt):
    return formats[fmt][0]

def deserializer(fmt):
    return formats[fmt][1]

# we're sending function call metadata as json, and
# the rest as whatever format the user wants to send
# separator splits it out
separator = '""""'
chunk_size = 2000000

#toset
catalog = None
datadir = None
host_url = None
redis_conn = None
prefix = ""
timeout = 10
is_server = None

rpc_url = None
data_rpc_url = None

def setup_server(_redis_conn, _datadir, _host_url, _catalog):
    if not _host_url.endswith("/"):
        _host_url += "/"
    global catalog
    global datadir
    global host_url
    global rpc_url
    global redis_conn
    global data_rpc_url
    global is_server

    is_server = True
    catalog = _catalog
    datadir = _datadir
    host_url = _host_url
    data_rpc_url = host_url
    rpc_url = host_url
    redis_conn = _redis_conn

def setup_client(_rpc_url):
    if not _rpc_url.endswith("/"):
        _rpc_url += "/"
    global rpc_url
    global data_rpc_url
    global is_server
    is_server = False
    rpc_url = _rpc_url
    data_rpc_url = rpc_url

def client():
    from .clients import http
    return http.Client(rpc_url)
