import cPickle as pickle

import pandas as pd
import numpy as np

from kitchensink import setup_client, client, du, do, dp, dr, RemoteData

def test_remote_data_source_conversions():
    ### remote data sources can be accessed as an object, local path, or raw data
    ### test conversions of all

    df1 = pd.DataFrame({'a' : np.arange(100000)})
    shape = df1.shape

    #start with a python object - we should be able to convert to raw and local path
    obj = do(df1)
    path = obj.local_path()
    with open(path, "rb") as f:
        df = pickle.load(f)
    assert df.shape == shape
    df = pickle.loads(obj.raw())
    assert df.shape == shape

    #start with a raw data,  should be able to convert to raw and local path
    obj = dr(obj.raw())
    assert obj.obj().shape == shape
    path = obj.local_path()
    with open(path, 'rb') as f:
        df = pickle.load(f)
    assert df.shape == shape

    #start with a file,  should be able to convert to obj and raw
    obj = dp(obj.local_path())
    assert obj.obj().shape == shape
    df = pickle.loads(obj.raw())
    assert df.shape == shape
