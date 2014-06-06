from kitchensink.serialization.json_serialization import serialize, deserialize
import numpy as np
import pandas as pd

def test_simple_dtype():
    obj = {'a' : 1, 'dtype' : np.dtype('float64')}
    strval = serialize(obj)
    newobj = deserialize(strval)
    print newobj
    assert obj == newobj

def test_complex_dtype():
    obj = {'a' : 1, 'dtype' : np.dtype([('date', 'datetime64[ns]'), ('val', 'f8')])}
    strval = serialize(obj)
    newobj = deserialize(strval)
    print newobj
    assert obj == newobj

