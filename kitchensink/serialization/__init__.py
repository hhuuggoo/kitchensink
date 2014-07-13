from . import json_serialization, dill_serialization, pickle_serialization
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

"""
we're sending function call metadata as json, and
the rest as whatever format the user wants to send
separator splits it out
metadata:
fmt, auth_string, serialized_function, async, queue_name

data:
func_string, args_string, kwargs_string
"""
separator = '""""'

def pack_msg(metadata, data):
    metadata_string = serializer('json')(metadata)
    data_string = serializer(metadata['fmt'])(data)
    return metadata_string + separator + data_string

def unpack_msg(input_string):
    metadata_string, data_string = input_string.split(separator)
    metadata = deserializer('json')(metadata_string)
    data = deserializer(metadata['fmt'])(data_string)
    return metadata, data
