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


### Asynchronous execution

The following code will execute a remote function

```
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

### Architecture
#### Server Processes
-  Currently each node runs one webserver, and N workers.
-  The webserver does a few things
  - responds to queries about metadata about server side data (who has what, how big the files are)
  - responds to queries about server side data - both downloading and uploading files
  - allows clients to enqueue function calls, and query for there status
-  Each worker operates in a loop, pulling tasks off of redis, executing them, and shoving the result back into redis
-  Every node is identical.  In practice, clients use one of the nodes as a head node - that is where all tasks are enqueued.  But any node could be used as a head node
-  requests for downloading and uploading data do not go to the head node, they go to whatever node has the data or where the data should be stored
-  Most operations executed on kitchensink end up being executed as long running tasks, these are queued in redis and executed by one of the workers.
  - The exception to this are generally IO bound
  - operations for uploading and downloading data
  - also admin operations for cancelling all pending jobs
  - also queries for metadata about remote data objects
-  Each worker can listen on N queues - by default, each worker listens on the data, default, and a host queue (which is specified as the url of the host).  The data queue serves performs long running operations on data (path search).  the default queue handles function calls which are not data-locality routed.  The host queue is used when tasks are intended to be dispatched to a specific host.  If one wanted to setup a special queue (let's say for GPU nodes)  you would need to setup a queue named GPU (to handle non-data local functions), and also setup a GPU-<host_url> named queue, so that we could do data locality based routing on the GPU queue

#### Task Queue
-  Based off of python-rq http://python-rq.org/
-  we've customized python-rq by extending their base classes
-  This customization includes allow jobs to push intermediate results over redis (task start, task complete, task fail, and stdoutput messages)
-  Some customization also enables data locality based routing - what we do for data locality based routing, is enqueue a task on N host queues.  Then the first worker which dequeues the job claims it in redis, using redis as a global lock

#### Remote Data Infrastructure
-  In redis, we store 3 things about data
  - That we've started writing data to a URL
  - a redis hashset, mapping a host to the local file path which stores the data (the same path as the data url)
  - a redis hashset which has metadata about the remote data (file size, md5 sums, serialization format, etc..)
-  Any file can be stored as remote data
-  Any remote data object, can be accessed as a file.  Some remote data objects can be deserialized into a python object (pickled files, json, dill, can be deserialized, things like csvs, and hdf5, cannot)

#### Walk through of a sample operations

Example:  User has a bunch of CSVs, they want to parse them, and then store the pickled dataframes on the server
-  User would upload CSVs to the server, using the remote data infrasturcture.  Or they could upload them to the sever using rsync, and use some operations to store the metadata into redis (currently not implemented)
-  User would execute functions to parse and store the result in redis.  This would look like the following

```
c = settings.Client()
def to_pickle(remote_object):
    # obtain the local path for the remote data object, and parse it
    df = pd.read_csv(remote_object.local_path())
    # turn the result into a remote data object
    obj = do(df)
    # save it
    obj.save()
    return obj
paths = c.path_search("data/*csv") # search for all csvs
remotes = [du(x) for x in paths] # wrap urls for csvs in remote data objects
# loop over remote data objects, and setup function calls for pickling each one of them
for r in remotes:
    c.bulk_call(to_pickle(r))
# execute all function calls
c.execute()
# poll for results
c.bulk_results()
```

-  When `c.bulk_call` is called, nothing happens, except that the function, args, and kwargs are stored in `client.calls`
-  when c.execute() is called, a few things happen. 
  -  the args and kwargs are inspected for remote data objects.  A list of data urls is created
  -  we query the server for metadata about each data url (hosts which have them, which hosts are active, file size)
  -  For each function, we determine how much data would need to be copied if it was executed on each host
  -  For each function, a priority list is constructed in order of which hosts involve the least amount of data copying
  -  There is a threshold, (200mb, but it's configureable) for how much data we will not copy.  Hosts which exceed this amount, will not be used for this function, and are cut out of the priority list
  -  All functions, args, kwargs, and host priority lists(in the form of queue names) are sent to the server
- When the server receives this message, it goes through each function, and enqueues it on the first priority
- host for each function.  Then we go through the list of funciton calls again, and enqueue it on the second priority host, and then the third priority host, etc..  The important thing is that all functions are enqueued on their first priority hosts before the second priority hosts start being enqueued
- Workers proceed to pull these jobs off of redis.  When a worker receives a job, it also sets a global lock for the job "claiming it" so that any other worker who pops that job off of the queue will discard it.  The worker executes the function, and stores the result in redis.  While the function executes, intermediate messages 
may be pushed into the intermediate results queue for the job.  This is task start, stop, fail, and any stdout
-  When `bulk_results` Is called, the client begins polling the server for results.  Every time the client polls the server, the server executes a redis blocking operation on the intermediate results queues for jobs.  This http request returns as soon as any data is available, or times out (5 second timeout by default) if nothing is available.  Since task start/stop are inside this, clients get prompt notification of task completion during this polling process.  the `bulk_results` will query for all job ids at first, and then as results come in, it will truncate the list of jobs it is asking for from the server.  During this `bulk_results` polling process, if a task is completed, it's result is returned to the client in the response, and the job (and it's results) are deleted from redis
