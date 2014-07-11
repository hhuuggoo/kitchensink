comments = \
"""
kitchen sink RPC Server

This command is used to start a worker process for the RPC Server.
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
