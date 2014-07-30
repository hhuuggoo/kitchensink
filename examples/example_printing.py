import logging
import time

from kitchensink.clients.http import Client
from kitchensink import settings
settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)

def test_func(x):
    time.sleep(1)
    print (x)
    time.sleep(1)
    print (x)
    return x


st = time.time()
#c.bulk_call(test_func, 1, _intermediate_results=False)
#c.execute()
c.call(test_func, 1, _async=False)
ed = time.time()
print ed-st
#job2 = c.call(test_func, 2)
#job3 = c.call(test_func, 3)
#print c.bulk_async_result([job1, job2, job3])
