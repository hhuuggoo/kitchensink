# we're sending function call metadata as json, and
# the rest as whatever format the user wants to send
# separator splits it out
separator = '""""'
chunk_size = 2000000

#toset
catalog = None
datadir = None
host_url = None
host_name = None
redis_conn = None
prefix = "temp/"
timeout = 10
is_server = None

rpc_url = None
data_rpc_url = None
server_manager = None

def setup_server(_redis_conn, _datadir, _host_url, _host_name,
                 _catalog, _server_manager):
    if not _host_url.endswith("/"):
        _host_url += "/"
    global catalog
    global datadir
    global host_url
    global host_name
    global rpc_url
    global redis_conn
    global data_rpc_url
    global is_server
    global server_manager

    is_server = True
    catalog = _catalog
    datadir = _datadir
    host_name = _host_name
    host_url = _host_url
    data_rpc_url = host_url
    rpc_url = host_url
    redis_conn = _redis_conn
    server_manager = _server_manager

def setup_client(_rpc_url):
    if not _rpc_url.endswith("/"):
        _rpc_url += "/"
    global rpc_url
    global data_rpc_url
    global is_server
    global host_name
    is_server = False
    rpc_url = _rpc_url
    data_rpc_url = _rpc_url
    from .utils.funcs import reverse_dict
    active_hosts = client().call('hosts',
                                 _async=False,
                                 _rpc_name='data')
    host_name = reverse_dict(active_hosts)[rpc_url]

def client(rpc_name='default', queue_name='default', fmt='cloudpickle'):
    from .clients import http
    return http.Client(rpc_url,
                       rpc_name=rpc_name,
                       queue_name=queue_name,
                       fmt=fmt)
