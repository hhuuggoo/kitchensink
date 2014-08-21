import logging
import sys

from ..rpc import RPC
from .. import settings
logger = logging.getLogger(__name__)

def cancel_all():
    keys = settings.redis_conn.keys("rq:job*")
    if keys:
        settings.redis_conn.delete(*keys)

def make_rpc():
    rpc = RPC()
    rpc.register_function(cancel_all)
    return rpc
