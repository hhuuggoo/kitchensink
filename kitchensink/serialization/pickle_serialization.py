import cPickle as pickle

def deserialize(obj):
    return pickle.loads(obj)

def serialize(obj):
    return pickle.dumps(obj)
