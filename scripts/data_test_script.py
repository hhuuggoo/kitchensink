import logging

import pandas as pd

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings
import dill

logging.basicConfig(level=logging.DEBUG)
settings.setup_client("http://localhost:6323/")
df = pd.DataFrame({'a' : [1,2,3,4,5]})
df2 = pd.DataFrame({'a' : [2,4,6,8,10]})
remote = RemoteData(obj=df)
remote.save()
remote = RemoteData(data_url=remote.data_url, rpc_url="http://localhost:6324/")
obj = remote.obj()
print obj

a = RemoteData(obj=df)
b = RemoteData(obj=df2)
a.save(prefix="foo/bar")
b.save(prefix="foo/bar")
def test_func(x, y):
    result = a.obj() + b.obj()
    result = RemoteData(obj=result)
    result.save()
    return result
c = Client("http://localhost:6324/", rpc_name='test')
result = c.async_result(c.call(test_func, a, b))
print result
print result.local_path()
print result.obj()
print c.path_search('foo/bar/*')
