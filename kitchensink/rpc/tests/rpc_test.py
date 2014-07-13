from __future__ import print_function
import cPickle as pickle
import json

import pandas as pd
import numpy as np
from rq.job import Status

from kitchensink.testutils.testrpc import make_rpc, dummy_add
from kitchensink.serialization import (json_serialization,
                                       dill_serialization,
                                       pickle_serialization,
                                       pack_msg,
                                       unpack_msg)

def test_rpc():
    a = pd.DataFrame({'a' : [1,2]})
    b = pd.DataFrame({'a' : [0,1]})
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    metadata = {'fmt' : 'dill',
                'async' : False}
    data = {'func' : 'dummy_add',
            'args' : args,
            'kwargs' : kwargs}
    msg = pack_msg(metadata, data)
    result = rpc.call(msg)
    metadata, result = unpack_msg(result)
    status = metadata['status']
    assert status == Status.FINISHED
    assert metadata['fmt'] == 'dill'
    result = result == dummy_add(a, b)
    assert np.all(result)

def test_rpc_json():
    a = 1
    b = 2
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    metadata = {'fmt' : 'json',
                'async' : False}
    data = {'func' : 'dummy_add',
            'args' : args,
            'kwargs' : kwargs}
    msg = pack_msg(metadata, data)
    result = rpc.call(msg)
    metadata, result = unpack_msg(result)
    status = metadata['status']
    assert status == Status.FINISHED
    assert metadata['fmt'] == 'json'
    assert result == 3

def test_rpc_error():
    a = 1
    b = "sdf"
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    metadata = {'fmt' : 'json',
                'async' : False}
    data = {'func' : 'dummy_add',
            'args' : args,
            'kwargs' : kwargs}
    msg = pack_msg(metadata, data)
    result = rpc.call(msg)
    metadata, result = unpack_msg(result)
    status = metadata['status']
    assert status == Status.FAILED
    assert metadata['fmt'] == 'json'
    print (metadata['error'])
