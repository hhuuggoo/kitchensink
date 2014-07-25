import logging
import time

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings
import dill

logging.basicConfig(level=logging.INFO)
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)
settings.setup_client("http://localhost:6323/")
c = Client("http://localhost:6323/", rpc_name='test')
def test_func(a, b, desired_host=None):
    from kitchensink import settings
    print desired_host, settings.host_url
    result = a.obj() + b.obj()
    result = RemoteData(obj=result)
    result.save()
    print "**DONE", time.time()
    return result

df1 = pd.DataFrame({'a' : np.arange(100000)})
df2 = pd.DataFrame({'a' : np.arange(10000)})
remote1 = RemoteData(obj=df1)
remote1.save(prefix="routing")
remote2 = RemoteData(obj=df2, rpc_url="http://localhost:6324/")
remote2.save(prefix="routing")
jid = c.call(test_func, remote1, remote2, desired_host=6323)
result = c.bulk_async_result([jid])
#print result[0].obj()

df1 = pd.DataFrame({'a' : np.arange(200000)})
remote3 = RemoteData(obj=df1, rpc_url="http://localhost:6325/")
remote3.save()
#c = Client("http://localhost:6325/", rpc_name='test')
jid2 = c.call(test_func, remote1, remote2, desired_host=6323)
jid = c.call(test_func, remote3, remote2, desired_host=6325)
result = c.bulk_async_result([jid, jid2])
#print result[0].obj()
