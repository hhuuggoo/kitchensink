from kitchensink.testutils import testrpc
from kitchensink.rpc.server import register_rpc
rpc = testrpc.make_rpc()
register_rpc(rpc, 'test')
