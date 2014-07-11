from argparse import ArgumentParser

comments = \
"""
kitchen sink RPC Server

This command is used to start a the head node of an RPC Server (either web, or zmq)
and possibly startup redis
"""

def parser():
    p = ArgumentParser(help=comments)
    return p


def run(args):
    pass


def main():
    p = parser()
    args = p.parse_args()
    run(args)
