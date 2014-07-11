from __future__ import print_function
from argparse import ArgumentParser
import os

from simpleservices.redis import start_redis
from simpleservices.process import register_shutdown

comments = \
"""
kitchen sink RPC Server

This command is used to start a single box RPC Server including a web gateway,
one Node Controller, and N worker processes
"""
def parser():
    p = ArgumentParser(comments)
    p.add_argument('--redis-connection', help="redis connection information, <ip>:<port>", default="localhost:6379")
    p.add_argument('--head-port', help="port for the main RPC Server", type=int, default=5090)
    p.add_argument('--controller-port', help="port for the main RPC Server", default=5091)
    p.add_argument('--num-workers', help="port for the main RPC Server", type=int, default=1)
    p.add_argument('--no-redis', help="do not start redis", default=False, action="store_true")
    return p

def run_args(args):
    run(args.redis_connection, args.head_port, args.controller_port,
        args.num_workers, args.no_redis)

def run(head_port, controller_port, num_workers, no_redis):
    if not args.no_redis:
        print ("Starting redis on %s" % args.redis_connection)
        host, port = args.redis_connection.split(":")
        port = int(port)
        start_redis("kitchensink.pid", port, os.getcwd())
        register_shutdown()

def main():
    p = parser()
    args = p.parse_args()
    run_args(args)
