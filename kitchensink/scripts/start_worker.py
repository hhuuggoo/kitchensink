import logging
from argparse import ArgumentParser
from rq import Queue, Connection, Worker
import redis

from kitchensink import settings
from kitchensink.utils import parse_redis_connection

comments = \
"""
kitchen sink RPC Server

This command is used to start a worker process for the RPC Server.
"""
def parser():
    p = ArgumentParser(comments)
    p.add_argument('--redis-connection',
                   help="redis connection information, <ip>:<port>:db",
                   default="localhost:6379:0")
    p.add_argument('--node-url', help='url of node', default='http://localhost:6324/')
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
    run(args.redis_connection, args.node_url, args.queue)

def run(redis_connection, node_url, queue):
    settings.node_url = node_url
    redis_connection_obj = parse_redis_connection(redis_connection)
    r = redis.StrictRedis(host=redis_connection_obj['host'],
                          port=redis_connection_obj['port'],
                          db=redis_connection_obj['db'])
    if queue is None:
        queue = ['default']
    with Connection(r):
        queues = []
        node_queue = Queue(node_url)
        queues.append(node_queue)
        for q in queue:
            queues.append(Queue(q))
            w = Worker(queues)
    w.work(burst=False)

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
