

def inspect(func_args, func_kwargs):
    #fixme : circular import
    from . import RemoteData
    urls = set()
    all_args = list(func_args) + func_kwargs.values()
    for arg in all_args:
        if isinstance(arg, RemoteData):
            urls.add(arg.data_url)
    return urls


def route(urls, hosts, infos, threshold):
    """urls : list of data urls
    hosts: list of host urls
    infos: dict of data_urls to tuple of (host_info, data_info)
    """
    to_copy = {} #dict of host -> how much data needs to be copied
    for host in hosts:
        to_copy[host] = 0
    #M*N algo.. should be ok cause M and N are small
    for url in urls:
        location_info, data_info = infos[url]
        size = data_info['size']
        for host in hosts:
            if host in location_info:
                continue
            else:
                to_copy[host] += size
    to_copy = to_copy.items()
    to_copy.sort(key=lambda x : (x[1], x[0]))
    to_copy = [x for x in to_copy if x[1] < threshold]
    return to_copy
