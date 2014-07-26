### Kitchen Sink

Started to build an RPC server, ended up throwing in everything but the kitchen sink

The goal of this project is to make RPC easy to use - especially for people doing interactive work.  Here are the core features
- passing standard out/ std err back to the client
- passing exceptions back to the client
- http support for transport
- pluggable serialiation (json/dill/pickle/cloudpickle)
- support for remote data
- asynchronous and synchronous calls
- Currently supports both arbitrary function execution (pickled from the client),
  as well as named/registered functions on the server, which can be called
  by strings
- Support for banning arbitrary functions, and only executing registered functions
  (not implemented yet, but easy)

### Quickstart
To get started, you only need one server.

`python -m kitchensink.scripts.start --datadir /tmp/data1 --num-workers 2`

That command will auto-start a redis instance for you.  For a production deployment,
you should start your own server.  Then on each box, start a node with the following
command

`python -m kitchensink.scripts.start  --datadir /tmp/data2 --no-redis --node-url=http://localhost:6324/ --num-workers 2 --redis-connection"tcp://localhost:6379?db=9`

Here, the `--no-redis` option tells the process not to autostart a redis instance,
And the redis connection information is passed in via the command line

In general configuration is simple - each node only needs to know it's URL, the port
it should listen on (or it can infer this from the URL), the data directory, and
the address of the redis instance


### Work remotely as you would locally
Kitchen sink aims to make remote cluster work as easy as working on your laptop.  For ease of debugging, anything a remote function prints or logged, is redirected to your terminal.  We plan to support turning this off if you're running a large number of jobs, for example, but currently that is not implemented (But easy to do so)

```
### Asynchronous execution

The following code will execute a remote function


from kitchensink.clients.http import Client
c = Client("http://localhost:6323/", rpc_name='test')
import numpy as np
def test_func2(x, y):
    return np.dot(x, y)
jid = c.call(test_func2, np.array([1,2,3,4,5]), np.array([1,2,3,4,5]))
c.result(jid)

```


By default, all functions are executed asynchronously in a task queue.  You can execute
functions instantly on the web server if you want to - you can do this if you
know that the function is very fast, and won't bog down the webserver

```
result = c.call(test_func2, np.array([1,2,3,4,5]), np.array([1,2,3,4,5]), _async=False)
```


### Remote Data Pointers

The kitchen sink system supports server side data.

```
from kitchensink import settings
from kitchensink.clients.http import Client
from kitchensink.data import RemoteData

settings.setup_client("http://localhost:6323/")
c = Client(settings.rpc_url)

a = RemoteData(obj=dataframe1)
b = RemoteData(obj=dataframe2)
a.save()
b.save()

def test_func(a, b):
    result = a.obj() + b.obj()
    result = RemoteData(obj=result)
    result.save()
    return result

c = Client("http://localhost:6324/", rpc_name='test')
jid = c.call(test_func, a, b)
result = c.async_result(jid)

```
The remote data object is small object, which can either represent a local or a
global resource.  If you initialize it with either an object or a local file path,
you can save it, which will push it to the server.  Or, an object on the server
can be referenced via a data_url, and then accessed

```
a = RemoteData(data_url="mylarge/dataset")
jid = c.call(lambda data : data.obj().head(10))
c.result(jid)

```

A remote data object also has a pipeline function, which will stream the data to all nodes in the cluster

### Data Model

Remote data are just stored as files on the file system.  The local path of the file
on disk relative to the nodes data directory, is the data url.  In redis we store
some metadata about the file (just file size for now), and which hosts have it locally.

Support for global data (via NFS or S3) is not yet implemented, and might not ever be.
Because if you have global data, you don't need to use the remote data infrastructure, you might as well just turn it off.

We support very simple data locality.  Whenever a function is executed, which is either using remote data objects in the input arguments (we search function args and kwargs, but not very deeply)  We compute how much data needs to be transfered to each node.  All nodes are sorted to minimize data transfer.   There is also a threshold (currently, 200mb) where we will not allow a task to run on a given node.  The task is queued and in order of priority (based on the data transfer we computed) and the first node that picks it up gets it

### Future Work

- Support for multiple anaconda environments on the cluster
- Support for in memory data (so you can have a remote source loaded into memory, and then all jobs on that node can access it from memory, rather than reading it off of disk)
