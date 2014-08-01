import logging
import time

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings

settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)
def test_func(a, b):
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

#does not execute
c.bulk_call(test_func, remote1, remote2)
c.bulk_call(test_func, remote1, remote2)
c.bulk_call(test_func, remote1, remote2)
c.bulk_call(test_func, remote1, remote2)
c.bulk_call(test_func, remote1, remote2)
c.execute()
c.bulk_results()
