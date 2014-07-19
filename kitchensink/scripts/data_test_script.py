import logging

import pandas as pd

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings

logging.basicConfig(level=logging.DEBUG)
settings.setup_client("http://localhost:6323/")
df = pd.DataFrame({'a' : [1,2,3,4,5]})
df2 = pd.DataFrame({'a' : [2,4,6,8,10]})

remote = RemoteData(obj=df)
remote.save()

remote = RemoteData(data_url=remote.data_url)
obj =remote.obj()
#path = remote.local_path()
print obj

a = RemoteData(obj=df)
b = RemoteData(obj=df2)
a.save()
b.save()

def test_func(x, y):
    result = a.obj() + b.obj()
    result = RemoteData(obj=result)
    result.save()
    return result

c = Client("http://localhost:6323/", rpc_name='test')
result = c.call(test_func, a, b).result()
print result
print result.obj()
