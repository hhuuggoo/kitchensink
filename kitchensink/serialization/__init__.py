from . import json_serialization, dill_serialization, pickle_serialization
from .. import utils

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
"""
format:
   metadata : json object, serialized as json
   the metadata object must contain a 'fmt' k/v pair which
   indicates the format of the payload.
   the metadata also must contain a 'len' k/v/ pair
   which contains the length of the resulting binary messages.
   if there is only one binary message, then 'len' can be omitted

   a separator follows

   then the data block begins, which is N data strings
   encoded by 'fmt'
"""
def pack_msg(metadata, *data):
    data_strings = []
    for d in data:
        data_string = serializer(metadata['fmt'])(d)
        data_strings.append(data_string)
    metadata['len'] = [len(x) for x in data_strings]
    metadata_string = serializer('json')(metadata)
    return metadata_string + separator + "".join(data_string)

def unpack_msg(input_string):
    metadata = unpack_metadata(input_string)
    data = unpack_data(metadata, input_string)
    return metadata, data

def unpack_metadata(input_string):
    metadata_string, data_string = input_string.split(separator)
    metadata = deserializer('json')(metadata_string)
    return metadata

def unpack_data(metadata, input_string):
    metadata_string, data_string = input_string.split(separator)
    func = deserializer(metadata['fmt'])
    data = []
    lengths = metadata.get('len', [len(data_string)])
    idx = 0
    for l in lengths:
        obj = func(data_string[idx:idx + l])
        data.append(obj)
        idx += l
    return data

"""
RPC Call format:

metadata: contains func_string (optional)
fmt (mandatory)
queue_name (mandatory)
auth_string (optional)
async (mandatory)
data block : series of serialized dictionaries with
  func, args, kwargs

either the data_block must contain func, or the metadata must
contain func_string

we only want one data object (which is a dictionary with those things)
but we allow for passing in several, so that we can modify the
data block without deserializing the data block. This is because
un-dilling or un-pickling can be dangerous

as a result, all data blocks are converted into dicts, and then
updated, so that the last dicts values takes precedence
"""

def pack_rpc_call(metadata, *data, **kwargs):
    """serialize is the only kwarg here.
    this tells us whether ot not we should serialize the data blocks
    (this way, if the data block is already serialized, we don't
    need to do it).  defaults to True
    """
    serialize = kwargs.pop('serialize', True)
    func = serializer(metadata['fmt'])
    if serialize:
        data_strings = [func(x) for x in data]
    else:
        data_strings = data
    metadata['len'] = [len(x) for x in data_strings]
    metadata_string = serializer('json')(metadata)
    return metadata_string + separator + "".join(data_strings)

def append_rpc_data(input_string, data, serialize=True):
    metadata_string, data_string = input_string.split(separator)
    metadata = deserializer('json')(metadata_string)
    if serialize:
        fmt = metadata['fmt']
        func = serializer(metadata['fmt'])
        data = func(data)
    metadata.setdefault('len', len(data_string))
    lengths = metadata['len']
    lengths.append(len(data))
    metadata['len'] = lengths
    metadata_string = serializer('json')(metadata)
    return metadata_string + separator + data_string + data

def unpack_rpc_metadata(input_string):
    return unpack_metadata(input_string)

def unpack_rpc_data(metadata, input_string):
    datas = unpack_data(metadata, input_string)
    data = utils.update_dictionaries(*datas)
    return data

def unpack_rpc_call(input_string):
    metadata = unpack_rpc_metadata(input_string)
    data = unpack_rpc_data(metadata, input_string)
    return metadata, data

"""
RPC result format (same as our message format, except only 1 data payload)
"""

def pack_result(metadata, data):
    return pack_msg(metadata, data)

def unpack_result(input_string):
    return unpack_msg(input_string)
