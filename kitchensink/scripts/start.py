from __future__ import print_function
try:
    import gevent
    import gevent.monkey
    gevent.monkey.patch_all()
except ImportError:
    gevent = False
    pass
from argparse import ArgumentParser
import os
from os.path import abspath
import sys
import logging

import time
import urlparse

from redis.exceptions import ConnectionError

from simpleservices.redis import start_redis
from simpleservices.process import register_shutdown, ManagedProcess, close_all
from werkzeug.serving import run_with_reloader
from kitchensink.utils import parse_redis_connection
from kitchensink.rpc.server import make_app, run as runserver, register_rpc
from kitchensink.rpc import RPC
from kitchensink.data.datarpc import make_rpc as make_data_rpc
from kitchensink.admin import make_rpc as make_admin_rpc
from kitchensink.data import Catalog
import kitchensink.settings as settings
FORMAT = "%(created)f:%(name)s:%(message)s"
comments = \
"""
kitchen sink RPC Server

This command is used to start a single box RPC Server including a web gateway,
one Node, and N worker processes
"""
def parser():
    p = ArgumentParser(comments)
    p.add_argument('--redis-connection',
                   help="redis connection information, tcp://localhost:6379?db=9",
                   default="tcp://localhost:6379?db=9")
    p.add_argument('--node-url', help='url of node', default='http://localhost:6323/')
    p.add_argument('--node-name', help='name of node', default=None)
    p.add_argument('--no-redis',
                   help="do not start redis",
                   default=False,
                   action="store_true")
    p.add_argument('--node-port',
                   help="port for the main worker node of the RPC Server",
                   default=None)
    p.add_argument('--num-workers',
                   help="number of workers",
                   type=int,
                   default=1)
    p.add_argument('--queue',
                   help='queue to operate on',
                   action='append')
    p.add_argument('--reload',
                   help='reload when code has changed',
                   default=False,
                   action='store_true'
    )
    p.add_argument('--read-only',
                   help='if set, you cannot write data to this node',
                   default=False,
                   action='store_true')
    #mandatory
    p.add_argument('--datadir',
                   help='data directory',
                   default='tmp'
    )
    p.add_argument('--module',
                   help='(optional) module with rpc functions',
    )
    return p

def run_args(args):
    run(args.redis_connection, args.node_url,
        args.node_name,
        args.node_port,
        args.num_workers, args.no_redis, args.queue,
        args.module,
        args.datadir,
        args.read_only
    )

def run(redis_connection, node_url, node_name, node_port,
        num_workers, no_redis, queue, module, datadir, read_only):
    if not node_url.endswith("/"):
        node_url += "/"
    if node_name is None:
        node_name = node_url
    datadir = abspath(datadir)
    register_shutdown()
    redis_connection_info = parse_redis_connection(redis_connection)
    if node_port is None:
        node_port = int(urlparse.urlparse(node_url).netloc.split(":")[-1])
    print ("**port", node_port)
    pid_file = "ks-%s.pid" % node_port
    if not no_redis:
        print ("Starting redis on %s" % redis_connection)
        start_redis(pid_file, redis_connection_info['port'], os.getcwd())
        time.sleep(1)
    cmd = [sys.executable, '-m', 'kitchensink.scripts.start_worker',
           '--node-url', node_url,
           '--node-name', node_name,
           '--redis-connection', redis_connection,
           '--datadir', datadir,
    ]
    if module:
        cmd.extend(('--module', module))
    if read_only:
        cmd.append('--read-only')
    app = make_app(redis_connection_info, node_port,
                   node_url, node_name, datadir, read_only)
    app.debug_log_format = FORMAT
    for c in range(10):
        try:
            result = settings.redis_conn.ping()
            break
        except ConnectionError:
            time.sleep(1.0)
    if queue is None:
        queue = ['default', 'data']
    for q in queue:
        cmd.extend(['--queue', q])
    for c in range(num_workers):
        print ('**cmd', cmd)
        ManagedProcess(cmd, 'worker-%s' % c, pid_file)
    data_rpc = make_data_rpc()
    register_rpc(data_rpc, 'data')
    default_rpc = RPC()
    register_rpc(default_rpc, 'default')
    admin = make_admin_rpc()
    register_rpc(admin, 'admin')
    if module:
        mod = __import__(module)
    runserver(gevent=gevent)
    #close_all()


def main():
    p = parser()
    args = p.parse_args()
    def helper():
        run_args(args)
    if args.reload:
        run_with_reloader(helper)
    else:
        helper()

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format=FORMAT)
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)

    logging.getLogger('rq.worker').level = logging.WARNING
    main()
