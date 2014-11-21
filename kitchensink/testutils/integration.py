# todo use random ports
import tempfile
import sys
import os
import shutil
import time
import cPickle as pickle

import numpy as np
import requests
import pandas as pd
import redis
from simpleservices.redis import start_redis
from simpleservices.process import register_shutdown, ManagedProcess, close_all, close

from ..api import setup_client, client, du, do, dp, Client
from .. import settings

pid_file = 'ks-test.pid'
def setup_module(node3_read_only=False):
    global dir1
    global dir2
    global dir3
    global redis_dir
    global rport
    global url1
    global url2
    global url3


    dir1 = tempfile.mkdtemp(prefix='ks-test')
    dir2 = tempfile.mkdtemp(prefix='ks-test')
    dir3 = tempfile.mkdtemp(prefix='ks-test')
    redis_dir = tempfile.mkdtemp(prefix='ks-test')

    port1 = 6323
    port2 = 6324
    port3 = 6325
    rport = 6326

    url1 = "http://localhost:%s/" % port1
    url2 = "http://localhost:%s/" % port2
    url3 = "http://localhost:%s/" % port3
    redis_url = 'tcp://localhost:%s?db=9' % rport
    start_redis(pid_file, rport, redis_dir, stdout=None, stderr=None)
    wait_redis_start(rport)
    cmd = ['python', '-m', 'kitchensink.scripts.start',
           '--datadir', dir1,
           '--no-redis',
           '--node-name', 'node1',
           '--node-url', url1,
           '--num-workers', '1',
           '--redis-connection', redis_url]
    p = ManagedProcess(cmd, 'node1', pid_file)
    wait_ks_start(url1)
    cmd = ['python', '-m', 'kitchensink.scripts.start',
           '--datadir', dir2,
           '--no-redis',
           '--node-name', 'node2',
           '--node-url', url2,
           '--num-workers', '1',
           '--redis-connection', redis_url]
    p = ManagedProcess(cmd, 'node2', pid_file)
    wait_ks_start(url2)
    cmd = ['python', '-m', 'kitchensink.scripts.start',
           '--datadir', dir3,
           '--no-redis',
           '--node-name', 'node3',
           '--node-url', url3,
           '--num-workers', '1',
           '--redis-connection', redis_url]
    if node3_read_only:
        cmd.append('--read-only')
    p = ManagedProcess(cmd, 'node3', pid_file)
    wait_ks_start(url3)
    # hack - wait for workers to sleep. necessary if
    # we want tests to accurately reflect performance
    time.sleep(1)

def teardown_module():
    close(pid_file, 'node1')
    close(pid_file, 'node2')
    close(pid_file, 'node3')
    wait_ks_gone(url1)
    wait_ks_gone(url2)
    wait_ks_gone(url3)
    close(pid_file, 'redis')
    wait_redis_gone(rport)
    close_all()
    shutil.rmtree(dir1)
    shutil.rmtree(dir2)
    shutil.rmtree(dir3)
    shutil.rmtree(redis_dir)

def wait_until(func, timeout=1.0, interval=0.01):
    st = time.time()
    while True:
        if func():
            return True
        if (time.time() - st) > timeout:
            return False
        time.sleep(interval)

def wait_redis_start(port):
    def helper():
        client = redis.Redis(port=port)
        try:
            return client.ping()
        except redis.ConnectionError:
            pass
    return wait_until(helper)

def wait_redis_gone(port):
    def helper():
        client = redis.Redis(port=port)
        try:
            client.ping()
            return False
        except redis.ConnectionError:
            return True
    return wait_until(helper)

def wait_ks_start(url):
    def helper():
        c = Client(url)
        return c.ping()
    return wait_until(helper)

def wait_ks_gone(url):
    def helper():
        c = Client(url)
        return c.ping() is None
    return wait_until(helper)

def dummy_func(x):
    return x

def remote_data_func(a, b):
    result = a.obj() + b.obj()
    result = do(obj=result)
    result.save()
    return result

def sleep_func():
    time.sleep(0.5)
    print 'sleep'
    time.sleep(0.5)
    print 'sleep'

def routing_func(*args):
    """test function which returns
    the host where data was executed
    """
    time.sleep(0.5)
    return settings.host_url

def remote_obj_func(obj):
    return obj.obj()

def remote_file(obj):
    path = obj.local_path()
    with open(path, 'rb') as f:
        return f.read()
