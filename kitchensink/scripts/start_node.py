from argparse import ArgumentParser

from kitchensink.utils import parse_redis_connection
from kitchensink.node.server import make_app, run
from kitchensink import settings

comments = \
"""
kitchen sink RPC Server

This command is used to start a node of an RPC Server.  This will start up
one Node Controller, and N worker processes
"""

def parser():
    p = ArgumentParser(help=comments)
    p.add_argument('--redis-connection',
                   help="redis connection information, <ip>:<port>",
                   default="localhost:6379")
    p.add_argument('--node-url', help='url of node', default='http://localhost:5091/')

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
    run(args.redis_connection, args.node_url, args.node_port,
        args.num_workers, args.queue)

def run(redis_connection, node_url, node_port, num_workers, queue):
    redis_connection_info = parse_redis_connection(redis_connection)
    if num_workers > 0:
        register_shutdown()
    cmd = ['sys.executable', 'kitchensink.scripts.start_worker',
           '--node-url', node_url,
           '--redis-connection', redis_connection]
    if len (queue) == 0:
        queue = ['default']
    for q in queue:
        cmd.extend(['--queue', q])
    for c in range(num_workers):
        ManagedProcess(cmd, 'worker-%s' % c, 'kitchensink.pid')
    make_app(redis_connection_info, node_port)
    run()

def main():
    p = parser()
    args = p.parse_args()
    run_args(args)
