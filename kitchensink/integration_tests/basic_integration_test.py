import time as time
import tempfile
import cPickle as pickle

import pandas as pd
import numpy as np

from kitchensink import setup_client, client, du, do, dp
from kitchensink.testutils.integration import (dummy_func,
                                               remote_data_func,
                                               sleep_func,
                                               routing_func,
                                               remote_obj_func,
                                               remote_file,
)
import kitchensink.testutils.integration as integration

setup_module = integration.setup_module
teardown_module = integration.teardown_module

# todo : some of these tests might be timing sensitive -
# we should verify the reliability of these tests later

def test_rpc():
    # test simple execution of a dummy function
    setup_client(integration.url1)
    c = client()
    c.bc(dummy_func, 1)
    c.execute()
    result = c.br()
    assert result == [1]

def test_bulk_calls():
    # test sleep function which should execute in 1 second.
    # should be parallelized, and all 3 calls should execute in ~ 1 second

    setup_client(integration.url1)
    c = client()
    st = time.time()
    c.bc(sleep_func)
    c.bc(sleep_func)
    c.bc(sleep_func)
    c.execute()
    result = c.br()
    ed = time.time()
    print ed-st
    assert ed-st < 2
    assert len(result) ==3

def test_data_routing():
    # test data routing - first call
    # should end up on the node the data is on
    # other 2 calls should be parallelized on the other 2 nodes

    setup_client(integration.url1)
    c = client()
    df1 = pd.DataFrame({'a' : np.arange(100000)})
    remote1 = do(obj=df1)
    remote1.rpc_url = integration.url2
    remote1.save()

    c.bc(routing_func, remote1)
    c.bc(routing_func, remote1)
    c.bc(routing_func, remote1)
    c.execute()
    results = c.br()
    assert results[0] == integration.url2
    assert set(results) == set([integration.url1, integration.url2, integration.url3])

def test_data_routing_mulitple_sources():
    # test data routing - first call
    # we setup a large data source on url1
    # and a smaller source on url2
    # execute 3 calls, url1 should have first priorty, url2, second, url3 third

    setup_client(integration.url1)
    c = client()
    name1 = tempfile.NamedTemporaryFile(prefix='ks-test').name
    name2 = tempfile.NamedTemporaryFile(prefix='ks-test').name
    len1 = 1000000
    len2 = 100000
    with open(name1, 'wb+') as f:
        f.write(len1 * b'0')
    with open(name2, 'wb+') as f:
        f.write(len2 * b'0')
    remote1 = dp(name1)
    remote1.rpc_url = integration.url1
    remote2 = dp(name2)
    remote2.rpc_url = integration.url2
    remote1.save()
    remote2.save()
    c.bc(routing_func, remote1, remote2)
    c.bc(routing_func, remote1, remote2)
    c.bc(routing_func, remote1, remote2)
    c.execute()
    results = c.br()
    assert results == [integration.url1, integration.url2, integration.url3]

def test_remote_data_sources():
    ### test grabbing obj, local_path, and raw data from a remote data source
    setup_client(integration.url1)
    c = client()
    df1 = pd.DataFrame({'a' : np.arange(100000)})
    shape = df1.shape
    obj = do(df1)
    obj.save()
    data_url = obj.data_url
    copy = du(data_url)
    assert copy.obj().shape == shape

    copy = du(data_url)
    df = pickle.loads(copy.raw())
    assert df.shape == shape

    copy = du(data_url)
    path = copy.local_path()
    with open(path, "rb") as f:
        df = pickle.load(f)
    assert df.shape == shape

def test_remote_data_source_conversions():
    ### remote data sources can be accessed as an object, local path, or raw data
    ### test conversions of all

    df1 = pd.DataFrame({'a' : np.arange(100)})
    shape = df1.shape
    obj = do(df1)
    obj.save()
    setup_client(integration.url1)
    c = client()
    c.bc(remote_obj_func, du(obj.data_url))
    c.execute()
    result = c.br()[0]
    assert result.shape == df1.shape

    obj = dp(obj.local_path())
    obj.save()
    c.bc(remote_file, du(obj.data_url))
    c.execute()
    result = c.br()[0]
    result = pickle.loads(result)
    assert result.shape == df1.shape
