from ..rpc import RPC
from blaze import compute as c
from blaze.compute import chunks

def compute(expr, rd):

    return c(expr, rd.obj())

def make_rpc():
    rpc = RPC(allow_arbitrary=False)
    rpc.register_function(compute, 'compute')
    return rpc
