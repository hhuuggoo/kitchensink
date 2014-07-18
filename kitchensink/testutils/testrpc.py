from ..rpc import RPC
import time
def dummy_mult(x, y):
    return x * y

def dummy_add(x, y):
    return x + y

def dummy_sleeper_add(x, y):
    print ('sleep 1')
    time.sleep(1)
    print ('sleep 2')
    time.sleep(1)
    print ('sleep 3')
    time.sleep(1)
    return x + y

def make_rpc():
    rpc = RPC()
    rpc.register_function(dummy_mult)
    rpc.register_function(dummy_add)
    rpc.register_function(dummy_sleeper_add)
    return rpc
