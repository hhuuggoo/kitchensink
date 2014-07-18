from . import (json_serialization, dill_serialization,
               pickle_serialization, cloudpickle)

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
    register_serialization('cloudpickle',
                           cloudpickle.dumps,
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
   msg_format : json object, serialized as json
       contains fmt : a list of formats for the remaining data objects
       and len : an array of the lengths of the data in the remaining data objects
   then the data block begins, which is N data strings
   encoded by the 'fmt'
"""
def pack_msg(*data, **kwargs):
    fmts = kwargs.pop('fmt')
    data_strings = []
    lengths = []
    for f, d in zip(fmts, data):
        data_string = serializer(f)(d)
        data_strings.append(data_string)
        lengths.append(len(data_string))
    msg_format = {'len' : lengths, 'fmt' : fmts}
    msg_format_string = serializer('json')(msg_format)
    return msg_format_string + separator + "".join(data_strings)

def unpack_msg(input_string):
    msg_format = unpack_msg_format(input_string)
    data = unpack_msg_data(msg_format, input_string)
    return msg_format, data

def unpack_msg_format(input_string):
    return deserializer('json')(input_string.split(separator, 1)[0])

def unpack_msg_data(msg_format, input_string, index=None, override_fmt=None):
    """won't work if you specify incorrect override_fmt, but
    override_fmt is used to make sure we don't use something like dill
    when we don't want it (Security)
    """
    _, data_string = input_string.split(separator)
    def unpack_data(index):
        if override_fmt is None:
            fmt = msg_format['fmt'][index]
        else:
            fmt = override_fmt
        length = msg_format['len'][index]
        idx = sum(msg_format['len'][:index])
        func = deserializer(fmt)
        data = func(data_string[idx:idx+length])
        return data
    if index is not None:
        return unpack_data(index)
    else:
        return [unpack_data(x) for x in range(len(msg_format['fmt']))]

"""
RPC Call format:
msg_format: similar to msg above
metadata: contains func_string (optional)
result_fmt (mandatory)
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
    fmt = kwargs.pop('fmt', 'dill')
    #always use json for rpc metadata
    fmts = ['json'] + [fmt for d in data]
    return pack_msg(metadata, *data, fmt=fmts)

def append_rpc_data(input_string, data, fmt=None):
    msg_format, data_string = input_string.split(separator)
    msg_format = deserializer('json')(msg_format)
    new_string = serializer(fmt)(data)
    msg_format['len'].append(len(new_string))
    msg_format['fmt'].append(fmt)
    return serializer('json')(msg_format) + separator + data_string + new_string

def unpack_rpc_call(input_string):
    msg_format, data = unpack_msg(input_string)
    metadata = data[0]
    data = utils.update_dictionaries(*data[1:])
    return msg_format, metadata, data

def unpack_rpc_metadata(input_string):
    msg_format = unpack_msg_format(input_string)
    return unpack_msg_data(msg_format, input_string, index=0, override_fmt='json')


"""
RPC result format (same as our message format, except only 1 data payload)
"""

def pack_result(metadata, data, fmt=None):
    return pack_msg(metadata, data, fmt=['json', fmt])

def unpack_result(input_string):
    return unpack_msg(input_string)
