from ..rpc import RPC
from .. import settings

def get_info(path):
    host_info, data_info = settings.catalog.get_info(path)
    return host_info, data_info
