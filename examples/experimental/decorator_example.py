import logging

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import du, do
from kitchensink import settings
from kitchensink.utils.decorators import remote
settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)

df = pd.DataFrame({'a' : np.arange(3)})
obj = do(df)
obj.save()

@remote
def mult(x):
    return do(2 * x)
print "**LOCAL"
print mult(df).obj() #executes locally
print mult(obj).obj() #xecutes locally

c.bc(mult, df)
c.execute()
print "**REMOTE"
print c.br()[0].obj() #executed remote
