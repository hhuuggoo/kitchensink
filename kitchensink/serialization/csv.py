import pandas as pd
import cStringIO as StringIO
def serialize(obj):
    assert isinstance(obj, pd.DataFrame)
    f = StringIO.StringIO
    obj.to_csv(f)
    f.seek(0)
    return f.read()

def deserialize():
    f = StringIO.StringIO
    f.write(obj)
    f.seek(0)
    return pd.read_csv(f)
