import logging

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings

settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)

df = pd.DataFrame({'a' : np.arange(200000)})
store = pd.HDFStore('test.hdf5')
store['df'] = df
store.close()
remote = RemoteData(local_path="test.hdf5")
remote.save(url="testdata/test.hdf5")

new_remote = RemoteData(data_url="testdata/test.hdf5")
jid = c.call(lambda obj : pd.HDFStore(obj.local_path()).select("df").head(10), new_remote)
print c.async_result(jid)
print c.path_search('testdata/*')
