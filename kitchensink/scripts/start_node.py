from argparse import ArgumentParser

comments = \
"""
kitchen sink RPC Server

This command is used to start a node of an RPC Server.  This will start up
one Node Controller, and N worker processes
"""

def parser():
    p = ArgumentParser(help=comments)
    return p


def run():
    pass


def main():
    p = parser()
    args = p.parse_args()
    run(args)
