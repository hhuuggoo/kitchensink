from ..rpc import RPC

def dummy_mult(x, y):
    return x * y

def dummy_add(x, y):
    return x + y

def make_rpc():
    rpc = RPC()
    rpc.register_function(dummy_mult)
    rpc.register_function(dummy_add)
    return rpc
