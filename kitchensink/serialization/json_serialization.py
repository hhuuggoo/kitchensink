import pandas as pd
import numpy as np
import cPickle as pickle
import json

def encode_dataframe(obj):
    pass

def encode_numpy(obj):
    pass

def encode_dtype(obj):
    # punting on properly encoding dtypes
    # in a transparent manner for now
    val = pickle.dumps(obj)
    return {'type' : 'dtype',
            'val' : val}
    
def decode_dtype(obj):
    return pickle.loads(str(obj['val']))

class KitchenSinkEncoder(json.JSONEncoder):
    def default(self, obj):
        ## array types
        if isinstance(obj, np.dtype):
            return encode_dtype(obj)
            
def object_hook(obj):
    if 'type' in obj:
        if obj['type'] == 'dtype':
            return decode_dtype(obj)
    return obj

def serialize(obj, encoder=KitchenSinkEncoder, **kwargs):
    return json.dumps(obj, cls=encoder, **kwargs)

def deserialize(obj, **kwargs):
    return json.loads(obj, object_hook=object_hook, **kwargs)
