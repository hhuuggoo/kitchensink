import time

from kitchensink import client, setup_client
"""follow single node setup in README.md.  this example illustrates receiving an error
from the remote function
"""
setup_client('http://localhost:6323/')
c = client()
c.bc(lambda x, y : x+y, 1, "sdf")
c.execute()
c.br()
