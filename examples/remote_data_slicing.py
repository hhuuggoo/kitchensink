import logging

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import du, do
from kitchensink import settings

settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)

df = pd.DataFrame({'a' : np.arange(2000000)})

obj = do(df)
obj.save()
print obj[100:400].obj()
print obj['a']
print obj['a'][100:400].obj()
