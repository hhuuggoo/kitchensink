import logging
import time

from kitchensink.clients.http import Client
from kitchensink import settings


logging.basicConfig(level=logging.INFO)
logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)
settings.setup_client("http://localhost:6323/")
df = pd.DataFrame({'a' : np.arange(100000)})

def test_func(x):
    time.sleep(1)
    print (x)
    time.sleep(1)
    print (x)
    time.sleep(1)
    print (x)
    time.sleep(1)
    print (x)
    time.sleep(1)
    print (x)
    return x


c = Client("http://localhost:6323/", rpc_name='data')
job1 = c.call(test_func, 1)
job2 = c.call(test_func, 2)
job3 = c.call(test_func, 3)
print c.bulk_async_result([job1, job2, job3])
