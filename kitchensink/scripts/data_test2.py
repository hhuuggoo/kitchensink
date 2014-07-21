import logging

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings
import dill

logging.basicConfig(level=logging.INFO)
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)
settings.setup_client("http://localhost:6323/")
df = pd.DataFrame({'a' : np.arange(100000)})
remote = RemoteData(obj=df)
remote.pipeline(prefix="testdata")
