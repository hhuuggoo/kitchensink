import cPickle as pickle

def serialize(obj):
    return pickle.loads(obj)

def deserialize(obj):
    return pickle.dumps(obj)
