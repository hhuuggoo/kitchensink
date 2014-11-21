from .funcs import (get_info_bulk, search_path, hosts, chunked_copy, delete, bootstrap)
from ..rpc import RPC


def make_rpc():
    rpc = RPC(allow_arbitrary=False)
    rpc.register_function(get_info_bulk)
    rpc.register_function(search_path)
    rpc.register_function(hosts)
    rpc.register_function(chunked_copy)
    rpc.register_function(delete)
    rpc.register_function(bootstrap)
    return rpc
