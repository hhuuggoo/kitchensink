from __future__ import print_function
import cPickle as pickle
import json

import pandas as pd
import numpy as np
from rq.job import Status

from kitchensink.testutils.testrpc import make_rpc, dummy_add
from kitchensink.serialization import (json_serialization,
                                       dill_serialization,
                                       pickle_serialization)

def test_rpc():
    a = pd.DataFrame({'a' : [1,2]})
    b = pd.DataFrame({'a' : [0,1]})
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    arg_string = pickle_serialization.serialize(args)
    kwargs_string = pickle_serialization.serialize(kwargs)
    status, metadata, result = rpc.call('dummy_add',
                                        arg_string,
                                        kwargs_string,
                                        'pickle',
                                        async=False)
    assert status == Status.FINISHED
    assert metadata['fmt'] == 'pickle'
    result = pickle_serialization.deserialize(result)
    result = result == dummy_add(a, b)
    assert np.all(result)

def test_rpc_json():
    a = 1
    b = 2
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    arg_string = json_serialization.serialize(args)
    kwargs_string = json_serialization.serialize(kwargs)
    status, metadata, result = rpc.call('dummy_add',
                                        arg_string,
                                        kwargs_string,
                                        'json',
                                        async=False)
    assert status == Status.FINISHED
    assert metadata['fmt'] == 'json'
    result = json_serialization.deserialize(result)
    assert result == 3

def test_rpc_error():
    a = 1
    b = "sdf"
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    arg_string = json_serialization.serialize(args)
    kwargs_string = json_serialization.serialize(kwargs)
    status, metadata, result = rpc.call('dummy_add',
                                        arg_string,
                                        kwargs_string,
                                        'json',
                                        async=False)
    assert status == Status.FAILED
    assert metadata['fmt'] == 'json'
