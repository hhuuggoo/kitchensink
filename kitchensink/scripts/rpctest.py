from kitchensink.clients.http import Client
import pandas as pd

c = Client("http://localhost:6323/", rpc_name='test')
a = pd.DataFrame({'a' : [1,2,3,4,5]})
print c.call('dummy_mult', a, a, _async=False)
