import logging

import pandas as pd
import numpy as np

from kitchensink.clients.http import Client
from kitchensink.data import RemoteData
from kitchensink import settings

settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)
c.reducetree("memoize*")

def add(x, y):
    return x + y
add.ks_memoize = True

c.bc(add, 1, 2)
c.execute()
c.br()

c.bc(add, 1, 2)
c.execute()
c.br()

c.bc(add, 1, 2)
c.execute()
c.br()
