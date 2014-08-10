from functools import wraps
from ..data import RemoteData
from ..data import datarpc
from ..data.routing import inspect
from .. import settings

def memoize(func):
    func.ks_memoize = True
    return func

def reconcile(obj, infos):
    if isinstance(obj, RemoteData):
        if obj._obj is not None:
            return obj.obj()
        elif obj._local_path is not None:
            return obj.local_path()
        elif infos[-1][obj.data_url][1].get('data_type', 'object') == 'object':
            obj = obj.obj()
        else:
            obj = obj.local_path()
    return obj


def remote(func):
    """This decorator makes it easy to write functions that can be run
    locally on the client or the server.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        urls = inspect(args, kwargs)
        if settings.is_server:
            infos = datarpc.get_info_bulk(urls)
        else:
            c = settings.client()
            infos = c.data_info(urls)
        new_args = []
        for idx, arg in enumerate(args):
            arg = reconcile(arg, infos)
            new_args.append(arg)
        for k, v in kwargs.items():
            kwargs[k] = reconcile(v, infos)
        result = func(*new_args, **kwargs)
        assert isinstance(result, RemoteData)
        return result
    wrapper.ks_remote = True
    return wrapper
