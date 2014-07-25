from kitchensink.clients.http import Client
import pandas as pd
import time
import numpy as np

c = Client("http://localhost:6323/", rpc_name='test')
a = pd.DataFrame({'a' : [1,2,3,4,5]})
def test_func2(x, y):
    return np.linspace(10, 200)

def test_func(x, y):
    print 'foo'
    return test_func2(x, y)

jobids = [c.call(test_func, a, a, _async=True) for cc in range(10)]
print c.bulk_async_result(jobids)
print c.call('dummy_mult', a, a, _async=False)
print c.call('dummy_add', a, a, _async=False)
try:
    len(c.call('dummy_mult', a, "sdf", _async=False))
except Exception as e:
    print "thrown exception, this is correct"

a = pd.DataFrame({'a' : [1,2,3,4,5, 6]})
print c.call('dummy_add', a, a, _async=True)
try:
    len(c.call('dummy_add', a, "sdf", _async=True))
except Exception as e:
    print "thrown exception this is correct"

c = Client("http://localhost:6323/", rpc_name='test', fmt='json')
print c.call('dummy_add', 2, 1, _async=False)
print c.call('dummy_add', 2, 1, _async=True)
