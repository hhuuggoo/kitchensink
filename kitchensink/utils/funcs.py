import logging

from .decorators import remote

logger = logging.getLogger(__name__)

def fhead(obj, start=0, end=10):
    path = obj.local_path()
    results = []
    with open(path, 'r') as f:
        for idx, line in enumerate(f):
            results.append(line)
            if idx == end:
                break
    return results[start:]

def max(obj, col):
    return obj[col].obj().max()

def min(obj, col):
    return obj[col].obj().min()

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in range(0, len(l), n):
        yield l[i:i+n]

def workflow(c, func, data=[]):
    compute = False
    for prefix, threshold in data:
        num_objs = len(c.path_search(prefix))
        if num_objs < threshold:
            logger.info("%s objs found for prefix %s recomputing", num_objs, prefix)
            compute = True
            break
        else:
            logger.info("%s objs found for prefix %s no need to compute",
                        num_objs, prefix)
    if compute:
        logger.info("computing")
        for prefix, threshold in data:
            logger.info("clearing prefix %s", prefix)
            c.reducetree(prefix)
        func()
