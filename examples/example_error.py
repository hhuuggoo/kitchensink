from kitchensink import settings
from kitchensink.clients.http import Client
settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)
c.bc(lambda x, y : x+y, 1, "sdf")
c.execute()
c.br()
