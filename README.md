### Kitchen Sink

Started to build an RPC server, ended up throwing in everything but the kitchen sink

The goal of this project is to make RPC easy to use - especially for people doing interactive work.  Here are the core features
- passing standard out/ std err back to the client, so that remote work is almost as seamless as local work
- passing exceptions back to the client
- http support for transport
- pluggable serialization (json/dill/pickle/cloudpickle)
  - we do this by having a multi-part message format.  Every message
  starts with a json block that describes the format and lengths of the blocks that
  follow
- support for remote data
- support for asynchronous and synchronous calls
- Currently supports both arbitrary function execution (pickled from the client),
  as well as named/registered functions on the server, which can be called
  by strings
- Support for banning arbitrary functions, and only executing registered functions
  (not implemented yet, but easy)

### Install

Currently only tested on linux with python2.7.  OS X is probably fine.

- conda install -c hugo kitchensink
- pip install kitchensinkRPC


### Quickstart
To get started, you only need one server.

`python -m kitchensink.scripts.start --datadir /tmp/data1`

That command will auto-start a redis instance for you.  And start one worker.  Remote data will be stored in /tmp/data1.  For multiple workers, execute

`python -m kitchensink.scripts.start --datadir /tmp/data1 --num-workers 4`

For a production deployment, you should run your own redis instance somewhere
Then on each box, start a node with a command such as the following.

`python -m kitchensink.scripts.start  --datadir <datadir> --no-redis --node-url=http://<hostname>:<port>/ --num-workers N --redis-connection"tcp://redishost:redisport?db=redisdb`

For example, I usually run

`python -m kitchensink.scripts.start  --datadir /tmp/data --no-redis --node-url=http://localhost:6323/ --num-workers 4 --redis-connection"tcp://localhost:6379?db=9`

Here, the `--no-redis` option tells the process not to autostart a redis instance,
And the redis connection information is passed in via the command line.  The port
the webserver runs on is informed by the `--node-url` parameter.  However if
you're doing something like proxying via nginx, or using virtual IPs, you can
specify the port with `--node-port`.  The important thing is that `--node-url` is
the http address that can be used to reach this process.

In general configuration is simple - each node only needs to know it's URL, the port
it should listen on (or it can infer this from the URL), the data directory, and
the address of the redis instance


### Work remotely as you would locally
Kitchen sink aims to make remote cluster work as easy as working on your laptop.  For ease of debugging, anything a remote function prints or logged, is redirected to your terminal.  You can turn this off on a per-function call basis by passing in `_intermediate_results=False`.  In the future we will probably turn this on automatically if you are executing a large number of functions

### Asynchronous execution

The following code will execute a remote function.  Here, `bc` is short for bulk_call.
kitchen sink does support executing one function at a time using `call`, however I've found that I almost want to execute many functions at a time, and `bc` is more convenient.
`bc` enqueues calls on the local client object.  `execute` sends those calls to the server.  and `br`, short for `bulk_results` waits on the results of completion, and returns them.

```
from kitchensink import setup_client, client
setup_client("http://localhost:6323/")
c = client()
import numpy as np
def test_func2(x, y):
    return np.dot(x, y)
c.bc(test_func2, np.array([1,2,3,4,5]), np.array([1,2,3,4,5]))
c.execute()
c.br()
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
from kitchensink import setup_client, client, RemoteData, Client
setup_client("http://localhost:6323/")
c = client()
a = RemoteData(obj=dataframe1)
b = RemoteData(obj=dataframe2)
a.save()
b.save()

def test_func(a, b):
    result = a.obj() + b.obj()
    result = RemoteData(obj=result)
    result.save()
    return result
c.bc(test_func, a, b)
c.execute()
result = c.br()[0]

```
The remote data object is small object, which can either represent a local or a
global resource.  If you initialize it with either an object or a local file path,
you can save it, which will push it to the server.  Or, an object on the server
can be referenced via a data_url, and then accessed

```
a = RemoteData(data_url="mylarge/dataset")
c.bc(lambda data : data.obj().head(10))
c.execute()
result = c.br()[0]

```

A remote data object also has a pipeline function, which will stream the data to all nodes in the cluster

### Data Model

Remote data are just stored as files on the file system.  The local path of the file
on disk relative to the nodes data directory, is the data url.  In redis we store
some metadata about the file (just file size for now), and which hosts have it locally.

Support for global data (via NFS or S3) is not yet implemented, and might not ever be.
Because if you have global data, you don't need to use the remote data infrastructure,
you might as well just turn it off.

We support very simple data locality.  Whenever a function is executed, which is either using remote data objects in the input arguments (we search function args and kwargs, but not deeply)  We compute how much data needs to be transfered to each node.  All nodes are sorted to minimize data transfer.   There is also a threshold (currently, 200mb) where we will not allow a task to run on a given node.  The task is queued and in order of priority (based on the data transfer we computed) and the first node that picks it up gets it

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
-  Each worker can listen on N queues - by default, each worker listens on the data, default, and a host queue (which is specified as the url of the host).  The data queue serves performs long running operations on data (path search).  the default queue handles function calls which are not data-locality routed.  The host queue is used when tasks are intended to be dispatched to a specific host.
Currently, support for heterogenous queues is a bit limited (but easy to fix).  Right now there is sort of an association between the
default queue, and the N host queues.  Most jobs would use the default queue if you weren't using data locality, and
would use the N host queues if you were doing data locality.  Additional user queues would have to be dispatched to separate
host queues in order to get the desired effect of running multiple queues, but we don't really have this implemented yet.

In more concrete terms

default:
no data locality : use default
with data locality : use <host1>, <host2>, ...<hostN>

let's say we wanted to create a GPU queue
no data locality : use GPU queue
with data localit : use <host1>-GPU, <host2>-GPU, <host3>-GPU

currently the ability to use another queue, like the GPU queue will not work with data locality


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

#### Issues

- reliability - we don't restart processes, if processes segfault, it is currently up to the user to restart them.  This can be handled by using things like supervisord in deployment
- reliability - we don't detect segfaults, if tasks die, the client polls for their result forever
- reliability - we don't do much to guarantee consistency across the distributed file system, if something goes wrong it is possible to have 2 copies of the same resource which are different
- reliability - we don't protect hosts from running out of disk space
- reliability - we don't protect redis from running out of ram

#### Testing
- `nosetests kitchensink ` will run all unittests (we don't have many unfortunately, that could definitely be improved)
- `nosetests integration_tests` will run all integration tests (setup 3 kitchensink nodes locally), and exercise
  most of the functionality.  There are few tests here, but they are quite thorough.
-  I plan to primarily rely on integration tests rather than unit tests at the beginning, because it is likely that the code will go through many changes and the integration tests won't have to change as frequently
