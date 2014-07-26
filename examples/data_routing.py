import logging
import time

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings

settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)
"""Assuming you have 3 hosts, the first setup with the command:

python -m kitchensink.scripts.start --datadir /tmp/data1 --num-workers 3

And the second 2:

python -m kitchensink.scripts.start  --datadir /tmp/data2 --no-redis --node-url=http://localhost:6324/ --num-workers 2

python -m kitchensink.scripts.start  --datadir /tmp/data3 --no-redis --node-url=http://localhost:6325/ --num-workers 2
"""

def test_func(a, b, desired_host=None):
    from kitchensink import settings
    print desired_host, settings.host_url
    result = a.obj() + b.obj()
    result = RemoteData(obj=result)
    result.save()
    return result

df1 = pd.DataFrame({'a' : np.arange(100000)})
df2 = pd.DataFrame({'a' : np.arange(10000)})
remote1 = RemoteData(obj=df1, rpc_url="http://localhost:6323/")
remote1.save(prefix="routing")
remote2 = RemoteData(obj=df2, rpc_url="http://localhost:6324/")
remote2.save(prefix="routing")
jid = c.call(test_func, remote1, remote2, desired_host=6323)
result = c.bulk_async_result([jid])


df1 = pd.DataFrame({'a' : np.arange(200000)})
remote3 = RemoteData(obj=df1, rpc_url="http://localhost:6325/")
remote3.save()
jid2 = c.call(test_func, remote1, remote2, desired_host=6323)
jid = c.call(test_func, remote3, remote2, desired_host=6325)
result = c.bulk_async_result([jid, jid2])
