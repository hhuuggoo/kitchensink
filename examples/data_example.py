import logging

import pandas as pd
import numpy as np

from kitchensink import client, setup_client, do, du, dp
"""single node setup

This example illustrates basic usage of remote data sources

first example works with a remote file
second example works with a remote object(stored by pickle)
"""

setup_client("http://localhost:6323/")
c = client()
df = pd.DataFrame({'a' : np.arange(2000000)})
store = pd.HDFStore('test.hdf5')
store['df'] = df
store.close()

"""dp is a convenience function, equivalent to RemoteData(local_path=<path>)
We construct a remote data object, and save the data to the server
(which  generates a url).  Then we create a new RemoteData pointer with du
(short for data url, equivalent to RemoteData(data_url=<data_url>)
and we use that in a function call
"""

remote = dp("test.hdf5")
remote.save(prefix="testdata/test")
print remote.data_url

new_remote = du(remote.data_url)
def head(obj, name):
    store = pd.HDFStore(obj.local_path())
    return store.select(name).head(10)

c.bc(head, new_remote, 'df')
c.execute()
result = c.br()[0]
print result

"""do is short for dataobject, equivalent to RemoteData(obj=<obj>)
"""
remote = do(df)
remote.save()
def head(obj):
    return obj.obj().head(10)
new_remote = du(remote.data_url)
c.bc(head, new_remote)
c.execute()
print c.br()[0]
