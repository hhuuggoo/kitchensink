from ..rpc import RPC

def compute(expr, rd):
    from blaze import compute as c
    return c(expr, rd.obj())

def make_rpc():
    rpc = RPC(allow_arbitrary=False)
    rpc.register_function(compute, 'compute')
    return rpc
