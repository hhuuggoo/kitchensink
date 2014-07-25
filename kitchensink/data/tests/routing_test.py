from kitchensink.data.routing import route, inspect
from kitchensink.data import RemoteData

def inspect_test():
    a = RemoteData(data_url="foo/1")
    b = RemoteData(data_url="foo/3")
    c = RemoteData(data_url="foo/2")
    args = [a,b]
    kwargs = {'extra' : c}
    result = inspect(args, kwargs)
    assert set(result) == set(["foo/1", "foo/2", "foo/3"])

def routing_test():
    # generate some fake data.  hosts 1/2 have all the data
    # hosts 3/4 are missing the last dataset (size 900000)

    urls = ["foo/1", "foo/2", "foo/3", "foo/4"]
    sizes = [10000, 20000, 30000, 90000]
    hosts = ["http://host1", "http://host2", "http://host3", "http://host4"]
    infos = {}
    for size, url in zip(sizes, urls):
        infos[url] = ({}, {})
        infos[url][1]['size'] = size
        for idx, h in enumerate(hosts):
            infos[url][0][h] = "path" + str(idx)

    infos["foo/4"][0].pop("http://host3")
    infos["foo/4"][0].pop("http://host4")

    #add one url, foo5 which only host 4 has

    infos["foo/5"] = ({'http://host4' : "path5"}, {'size' : 100000000})

    #with a threshold of 5000, only host1 and 2 should be routed
    result = route(urls, hosts, infos, 50000)
    assert set(result) == set([('http://host1', 0), ('http://host2', 0)])

    #with a larger threshold of only host3 and 4 should be at the end
    result = route(urls, hosts, infos, 100000)
    assert set(result[:2]) == set([('http://host1', 0), ('http://host2', 0)])
    assert set(result[2:]) == set([('http://host3', 90000), ('http://host4', 90000)])

    # for the 5th dataset, only host4 has it, and should be routed based on
    # it's size
    result = route(["foo/5"], hosts, infos, 100000)
    assert result == [('http://host4', 0)]
