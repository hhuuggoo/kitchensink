from __future__ import print_function
from argparse import ArgumentParser
import os
import sys
import logging
import time

from simpleservices.redis import start_redis
from simpleservices.process import register_shutdown, ManagedProcess, close_all
from werkzeug.serving import run_with_reloader

from kitchensink.utils import parse_redis_connection
from kitchensink.rpc.server import make_app, run as runserver
import os
print ('**PID', os.getpid())
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
    p.add_argument('--node-url', help='url of node', default='http://localhost:6324/')
    p.add_argument('--no-redis',
                   help="do not start redis",
                   default=False,
                   action="store_true")
    p.add_argument('--head-port',
                   help="port for the main RPC Server",
                   type=int,
                   default=6323)
    p.add_argument('--node-port',
                   help="port for the main worker node of the RPC Server",
                   default=6324)
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
    p.add_argument('--module',
                   help='module with rpc functions',
    )
    return p

def run_args(args):
    run(args.redis_connection, args.node_url, args.head_port,
        args.node_port,
        args.num_workers, args.no_redis, args.queue,
        args.module
    )
print ('foo')
def run(redis_connection, node_url, head_port, node_port,
        num_workers, no_redis, queue, module):
    register_shutdown()
    redis_connection_info = parse_redis_connection(redis_connection)
    if not no_redis:
        print ("Starting redis on %s" % redis_connection)
        start_redis("kitchensink.pid", redis_connection_info['port'], os.getcwd())
        time.sleep(1)
    cmd = [sys.executable, '-m', 'kitchensink.scripts.start_node',
           "--node-url", node_url,
           "--redis-connection", redis_connection,
           "--num-workers", str(num_workers)]
    print (cmd)
    if queue is None:
        queue = ['default']
    for q in queue:
        cmd.extend(['--queue', q])
    node = ManagedProcess(cmd, 'mainnode', 'kitchensink.pid')
    make_app(redis_connection_info, head_port)
    mod = __import__(module)
    runserver()
    close_all()


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
    logging.basicConfig(level=logging.DEBUG)
    main()
