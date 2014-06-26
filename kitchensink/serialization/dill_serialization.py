import dill

def deserialize(obj):
    return dill.loads(obj)

def serialize(obj):
    return dill.dumps(obj)
