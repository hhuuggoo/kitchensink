import time

from kitchensink import client, setup_client

"""
follow single node setup in README.md. This example illustrates parallel execution, as well as passing stdout back to the client
"""
def sleep_func():
    time.sleep(0.5)
    print 'sleep'
    time.sleep(0.5)
    print 'sleep'

setup_client('http://localhost:6323/')
c = client()
st = time.time()

c.bc(sleep_func)
c.bc(sleep_func)
c.bc(sleep_func)
c.execute()
results = c.br()

ed = time.time()

print ed-st
