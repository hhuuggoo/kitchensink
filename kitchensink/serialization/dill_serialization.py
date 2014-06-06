import dill

def serialize(obj):
    return dill.loads(obj)
    
def deserialize(obj):
    return dill.dumps(obj)
