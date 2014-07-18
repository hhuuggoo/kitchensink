from argparse import ArgumentParser
import logging
import sys

from simpleservices.process import register_shutdown, ManagedProcess, close_all
from werkzeug.serving import run_with_reloader

from kitchensink.utils import parse_redis_connection
from kitchensink.node.server import make_app, run as runserver
from kitchensink import settings

comments = \
"""
kitchen sink RPC Server

This command is used to start a node of an RPC Server.  This will start up
one Node Controller, and N worker processes
"""

def parser():
    p = ArgumentParser(comments)
    p.add_argument('--redis-connection',
                   help="redis connection information, <ip>:<port>",
                   default="localhost:6379")
    p.add_argument('--node-url', help='url of node', default='http://localhost:6324/')

    p.add_argument('--node-port',
                   help="port for the main worker node of the RPC Server",
                   type=int,
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
    return p

def run_args(args):
    run(args.redis_connection, args.node_url, args.node_port,
        args.num_workers, args.queue)

def run(redis_connection, node_url, node_port, num_workers, queue):
    redis_connection_info = parse_redis_connection(redis_connection)
    if num_workers > 0:
        register_shutdown()
    cmd = [sys.executable, '-m', 'kitchensink.scripts.start_worker',
           '--node-url', node_url,
           '--redis-connection', redis_connection]
    print (cmd)
    if queue is None:
        queue = ['default']
    for q in queue:
        cmd.extend(['--queue', q])
    for c in range(num_workers):
        ManagedProcess(cmd, 'worker-%s' % c, 'kitchensink.pid')
    make_app(redis_connection_info, node_port)
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
