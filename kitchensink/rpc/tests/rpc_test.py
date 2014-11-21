from __future__ import print_function
import cPickle as pickle
import json

import pandas as pd
import numpy as np
from rq.job import Status

from kitchensink.testutils.testrpc import make_rpc, dummy_add
from kitchensink.serialization import (json_serialization,
                                       pickle_serialization,
                                       pack_rpc_call,
                                       unpack_result)

def test_rpc():
    a = pd.DataFrame({'a' : [1,2]})
    b = pd.DataFrame({'a' : [0,1]})
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    metadata = {'result_fmt' : 'cloudpickle',
                'async' : False,
                'func_string' : 'dummy_add'}
    data = {'args' : args,
            'kwargs' : kwargs}
    msg = pack_rpc_call(metadata, data, fmt='cloudpickle')
    result = rpc.call(msg)
    msg_format, [metadata, result] = unpack_result(result)
    status = metadata['status']
    assert status == Status.FINISHED
    result = result == dummy_add(a, b)
    assert np.all(result)

def test_rpc_json():
    a = 1
    b = 2
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    metadata = {'result_fmt' : 'json',
                'async' : False,
                'func_string' : 'dummy_add'}
    data = {
            'args' : args,
            'kwargs' : kwargs}
    msg = pack_rpc_call(metadata, data, fmt='cloudpickle')
    result = rpc.call(msg)
    msg_format, [metadata, result] = unpack_result(result)
    status = metadata['status']
    assert status == Status.FINISHED
    assert metadata['result_fmt'] == 'json'
    assert result == 3

def test_rpc_error():
    a = 1
    b = "sdf"
    rpc = make_rpc()
    args = (a,b)
    kwargs = {}
    metadata = {'result_fmt' : 'json',
                'async' : False,
                'func_string' : 'dummy_add',
    }
    data = {
            'args' : args,
            'kwargs' : kwargs}
    msg = pack_rpc_call(metadata, data, fmt='json')
    result = rpc.call(msg)
    msg_format, [metadata, result] = unpack_result(result)
    status = metadata['status']
    assert status == Status.FAILED
    assert metadata['result_fmt'] == 'json'
    print (metadata['error'])
