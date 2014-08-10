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

@remote
def max(obj):
    return obj['latitude'].obj().max()
