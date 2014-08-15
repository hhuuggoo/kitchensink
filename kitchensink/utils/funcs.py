from .decorators import remote
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
