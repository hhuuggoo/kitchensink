from __future__ import print_function
from argparse import ArgumentParser
import os
import sys

from simpleservices.redis import start_redis
from simpleservices.process import register_shutdown

from kitchensink.utils import parse_redis_connection
from kitchensink.rpc.server import make_app, run

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
    p.add_argument('--node-url', help='url of node', default='http://localhost:5091/')
    p.add_argument('--no-redis',
                   help="do not start redis",
                   default=False,
                   action="store_true")
    p.add_argument('--head-port',
                   help="port for the main RPC Server",
                   type=int,
                   default=5090)
    p.add_argument('--node-port',
                   help="port for the main worker node of the RPC Server",
                   default=5091)
    p.add_argument('--num-workers',
                   help="number of workers",
                   type=int,
                   default=1)
    p.add_argument('--queue',
                   help='queue to operate on',
                   action='append')
    return p

def run_args(args):
    run(args.redis_connection, args.node_url, args.head_port, args.node_port,
        args.num_workers, args.no_redis, args.queue)

def run(redis_connection, node_url, head_port, node_port, num_workers, no_redis, queue):
    register_shutdown()
    redis_connection_info = parse_redis_connection(redis_connection)
    if not args.no_redis:
        print ("Starting redis on %s" % args.redis_connection)
        start_redis("kitchensink.pid", redis_connection_info['port'], os.getcwd())
    cmd = [sys.executable, '-m', 'kitchensink.scripts.start_node',
           "--node-url", node_url,
           "--redis-connection", redis_connection,
           "--num-workers", str(num_workers)]
    if len (queue) == 0:
        queue = ['default']
    for q in queue:
        cmd.extend(['--queue', q])
    node = ManagedProcess(cmd, 'mainnode', 'kitchensink.pid')
    make_app(redis_connection_info, head_port)
    run()


def main():
    p = parser()
    args = p.parse_args()
    run_args(args)
