## KitchenSinkRPC - Interact with remote computation as if it were local


## Initial Motivation
-  Was working on client sites, building http data access endpoints on remote machines
where were hit by javascript web clients, as well as python web clients
-  Got tired of writing API endpoint cruft to add every new piece of functionality
-  Had to deal with gunicorn timeouts, as some of the tasks were long running
-  Customers hitting the endpoint from the python side had no way to diagnose issues or troubleshoot errors


## Follow up Motivation

-  Parallelizing work on large datasets is cumbersome.  Writing map reduce jobs is cumbersome, and often you really just want a map
-  IPython parallel is a great solution for this - however I am hesitant on using that for production batch jobs, as it's dependent on the runtime state of the kernel
-  Nice to have a solution that supported large datasets like what Disco and Hadoop currently support, but which is as easy to use as IPython parallel


## What if disco and IPython parallel had a baby?


## Initial Feature Set
-  Make it feel like you are working locally
  -  passing all stdout back to the client
  -  passing errors back to the client
-  Support for server side data
  -  Support for fast formats (hdf5, bcolz)
  -  Data Locality based task routing
-  Support for multiple serialization formats


## Live Demo #1


## Common Issues in parallel computation
-  Serialization/Deserialization issues, and overhead
  -  As your datasets become large, you may be able to parallelize your workflow, however     you still have to send your data to where it's being computed, so data copying, and seriazliation of that data becomes a large part of how fast you can compute
-  Difficulty in profiling


## Somewhat of a Solution
-  Transparent data formats
-  Move computation to where the data is
-  Distributed Profiling


## Remote Data Objects
-  returning an object (assuming the file on disk represents a serialized object)
-  returning the local path
-  returning the raw bytestream of the object
-  accessible by data_url


## Live Demo #2


## Summary
-  Current state is ready for use, but is only really used by me
-  looking for contributors
-  looking for users


## Current Issues
-  Writing code around remote data objects is a bit annoying
-  There is probably a decorator or some other abstraction which will make it more seamless, but I haven't figured it out yet
-  KitchenSink is built around working with files on disk - this means that IPython parallel and spark have a distinct advantage because they operate on data in memory.  There are some solutions for this, but none of them are easy
-  KitchenSink does not have automatic high availability or redundancy built int
