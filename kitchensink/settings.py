from .serialization import json_serialization, dill_serialization, pickle_serialization

node_url = None
formats = {}
def register_serialization(name, serializer, deserializer):
    formats[name] = (serializer, deserializer)

def register_default_serialization():
    register_serialization('json',
                           json_serialization.serialize,
                           json_serialization.deserialize)
    register_serialization('dill',
                           dill_serialization.serialize,
                           dill_serialization.deserialize)
    register_serialization('pickle',
                           pickle_serialization.serialize,
                           pickle_serialization.deserialize)
register_default_serialization()

def serializer(fmt):
    return formats[fmt][0]

def deserializer(fmt):
    return formats[fmt][1]

# we're sending function call metadata as json, and
# the rest as whatever format the user wants to send
# separator splits it out
separator = '""""'
chunk_size = 4096

#toset
catalog = None
host_url = None
redis_conn = None
rpc_url = None
