import logging
import sys
import time
from contextlib import contextmanager

import pandas as pd

from ..rpc import RPC
from ..taskqueue.objs import current_job_id
from .. import settings
from ..serialization import serializer, deserializer

logger = logging.getLogger(__name__)

def cancel_all():
    keys = settings.redis_conn.keys("rq:job*")
    if keys:
        settings.redis_conn.delete(*keys)

def retrieve_profile(jids):
    connection = settings.redis_conn
    all_messages = []
    for jid in jids:
        key = "rq:profile:%s" % jid
        msgs = connection.lrange(key, 0, -1)
        if msgs:
            connection.ltrim(key, len(msgs), -1)
        big_message = {}
        for msg in msgs:
            msg = deserializer('cloudpickle')(msg)
            big_message.update(msg)
        all_messages.append(big_message)
    if all_messages:
        return pd.DataFrame(all_messages).sum()
    else:
        return None

#from dabaez
def timethis(what):
    @contextmanager
    def benchmark():
        start = time.time()
        yield
        end = time.time()
        jid = current_job_id()
        if settings.is_server and jid:
            connection = settings.redis_conn
            msg = {what : end-start}
            msg = serializer('cloudpickle')(msg)
            key = "rq:profile:%s" % jid
            connection.lpush(key, msg)
            connection.expire(key, 1800)
        else:
            print("%s : %0.3f seconds" % (what, end-start))
    return benchmark()

def make_rpc():
    rpc = RPC()
    rpc.register_function(cancel_all)
    rpc.register_function(retrieve_profile)
    return rpc
