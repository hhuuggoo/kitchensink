import logging

import pandas as pd
from kitchensink.data import RemoteData
from kitchensink import settings

logging.basicConfig(level=logging.DEBUG)
settings.setup_client("http://localhost:6323/")
df = pd.DataFrame({'a' : [1,2,3,4,5]})

remote = RemoteData(obj=df)
remote.save()

remote = RemoteData(data_url=remote.data_url)
obj =remote.obj()
#path = remote.local_path()
print obj
