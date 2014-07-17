from kitchensink.clients.http import Client
import pandas as pd

c = Client("http://localhost:6323/", rpc_name='test')
a = pd.DataFrame({'a' : [1,2,3,4,5]})
#print c.call('dummy_mult', a, a, _async=False)
#print c.call('dummy_add', a, a, _async=False)
#print c.call('dummy_mult', a, "sdf", _async=False)
a = pd.DataFrame({'a' : [1,2,3,4,5, 6]})
print c.call('dummy_add', a, a, _async=True)
print c.call('dummy_add', a, "sdf", _async=True)
