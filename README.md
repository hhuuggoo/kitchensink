### Kitchen Sink

python rpc made nice for interactive use

The goal of this project is to make RPC easy to use - especially for people doing interactive work.  Here are the core features

- pluggable authentication
- passing standard out/ std err back to the client
- passing exceptions back to the client
- supporting http and zeromq
- supporting json, with pointers to remote data which can be queried via more efficient means
- option to pass back a pointer to the data, so that it can be retrieved directly from where it is computed
- or left on the server if you want to use it as the input to another computation
  - you can imagine computing a large array, and then calling a function to slice the first 10 rows and return that
- all jobs will be dispatched into a task queue
- the task queue will be implemented in redis (for production) or shelve (for development)
- upon connecting a client to the server, the client will be able to retrieve all functions and docstrings so that users can view them interactively

### Configuration
- each RPC server will be configured via python dicts.

```
server = IPyRPC()
server.config({
  '*_json' : {'input_serialization' : json,
              'output_serialization' : json,
              'return_pointer' : False},
  '*_python' : {'input_serialization' : ipyrpc_binary,
                'output_seriazliation' : ipyrpc_binary,
                'return_pointer' : True}
})
server.register('compute_value_json', myfunction)
server.resgiter('compute_value_python', myfunction2)
server.serve_forever(host=0.0.0.0, port=7777)
```

The configuration will be expressed in globs, which will map up to registered functions.  Authentication functions can also be specified there.  the auth functions will be passed the function name, and the function arguments


### Pointers

```
We can return a pointer to the object as such:
{'type' : 'ipyrpc_data_pointer'
 'host' : host,
 'protocol' : http or zmq,
 'path' : path,
 'port' : port,
 'secret_key' : secret_key
 }
```

### Long running Queues

We will implement a long running queue in redis/shelve.  The RPC server will route standard out and results back to the client (or pointers to the results)

### Process Model
3 Components
- DataServer runs on each node, serves up server side data.  Each host should have one
- RPC Server pushes tasks onto redis, gateway for clients to communicate.  We could possibly combine this with the data server, but I'm not sure if that's a bad idea or not
- workers - workers pull tasks off of redis and execute them
  - workers can be spun up for different queues
  - workers will prioritize tasks based on data locality
  - workers can either complete the request in process, or fork a process
  - If the user wants to use a custom environment, We spin up workers for them which expire when their session closes
- data locality - when a job is queued, the size of the data it needs on each host is aggregated.  If the server side data is small,
  then we don't care, and we route the job to the central queue.  Otherwise the job has a list of hosts that can handle it
- There are named collections of hosts - these can be used to push out data.  there is also a job queue per named-set

### Data Model
#### Server Side Data
  - The idea would be that server side data would be replaced with blaze array server and catalogue in the future, with the exception of non-array data which would still be stored in the KitchenSink storage mechanism
  - Data is stored on disk in some directory, organized by user, with UUID filenames
  - The metadata for the data includes key, tags, type, connection information(hosts, ports, protocols), and size.
    - key is the primary key of the piece of data.  Will be a UUID
    - data is immutable with the exception of deletion
    - you don't get consistency guarantees with deletion
    - Serverside Data is stored in the following mechanism
      - There is a central redis string for each piece of data that keeps insertion date, and size of the data
      - There is a redis list for each piece of data that has all the connection information for where the data can be retrieved
      - size is denormalized into the connection information
      - There is a redis set for each dataset that contains all the tags
      - There is also a redis set for each tag that contains all the datasets
    -  Accessing a piece of data by key is a matter of retrieving connection information from the redis list, and hitting that host
    -  Searching data in redis involves possibly iterating through keys of the tag database, depending on the kind of search you
       want to do
#### Task Queue
  - Each job is stored as a hash set, with keyed off of job id
  - Jobs are added to queues which are redis lists, and can be popped off
  - For data locality, we compute which hosts can process the job based on the amount of data the job needs which resides on that host.
  - We then resolve that into N queues, attempting to use named queues to minimize the number of queues
  - The job is added to each queue (so yes, the job may be enqueued twice)
  - when jobs are popped, we do a final check using redis setnx to ensure that no other worker has claimed the job
