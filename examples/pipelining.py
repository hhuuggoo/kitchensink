import logging
import time

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings

settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)
"""follow multi node instructions from README.md
"""
df = pd.DataFrame({'a' : np.arange(100000)})
remote = RemoteData(obj=df)
retval = remote.pipeline(prefix='pipeline_test')
print retval
print c.data_info([remote.data_url])
