import logging
import time
import datetime as dt

from kitchensink.clients.http import Client
from kitchensink import settings
settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)

def test_func(x):
    time.sleep(1)
    print "FOO"
    time.sleep(1)
    print "FOO"

st = time.time()
print "%f" % st
c.bulk_call(test_func, 1,)
c.execute()
c.bulk_results()
#c.async_result(c.call(test_func, 1, _intermediate_results=False))
#c.call(test_func, 1, _async=False)
ed = time.time()
print "%f" % ed
print ed-st
#job2 = c.call(test_func, 2)
#job3 = c.call(test_func, 3)
#print c.bulk_async_result([job1, job2, job3])
