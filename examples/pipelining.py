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
df = pd.DataFrame({'a' : np.arange(100000)})
remote = RemoteData(obj=df)
#remote.pipeline(prefix="testdata")
remote = RemoteData(obj=df)
remote.save()
remote.pipeline_existing()
print c.call('get_info', remote.data_url, _rpc_name='data', _async=False)

c.reduce_data_hosts(remote.data_url, number=1)
print c.call('get_info', remote.data_url, _rpc_name='data', _async=False)
