import logging

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings

settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)

df = pd.DataFrame({'a' : np.arange(2000000)})
store = pd.HDFStore('test.hdf5')
store['df'] = df
store.close()
remote = RemoteData(local_path="test.hdf5")
remote.save(prefix="testdata/test")
print remote.data_url

new_remote = RemoteData(data_url=remote.data_url)
jid = c.call(lambda obj : pd.HDFStore(obj.local_path()).select("df").head(10), new_remote)
print c.async_result(jid)
print c.path_search('testdata/*')
print new_remote.local_path()
