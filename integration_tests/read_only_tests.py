import time as time
import tempfile
import cPickle as pickle

import pandas as pd
import numpy as np

from kitchensink import setup_client, client, du, do, dp, Client
from kitchensink.testutils.integration import (dummy_func,
                                               remote_data_func,
                                               sleep_func,
                                               routing_func,
                                               remote_obj_func,
                                               remote_file,
)
import kitchensink.testutils.integration as integration
import logging
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

setup_module = lambda : integration.setup_module(node3_read_only=True)
teardown_module = integration.teardown_module

def test_read_only():
    c = Client(integration.url3)
    c.bc(lambda : do(None).save(url='test_read_only'), _queue_name='default|node3')
    c.execute()
    c.br()
    active_hosts, results = c.data_info(['test_read_only'])
    location_info, data_info = results['test_read_only']
    assert 'node3' not in  location_info
    assert len(location_info) == 1
