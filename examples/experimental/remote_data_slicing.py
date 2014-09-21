import time

import pandas as pd
import numpy as np

from kitchensink import client, setup_client, do

setup_client("http://localhost:6323/")
c = client()
df = pd.DataFrame({'a' : np.arange(2000000)})
obj = do(df)
obj.save()
print obj[100:110].obj()
print obj[100:110]['a'].obj()
