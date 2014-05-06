ipyrpc
======

python rpc made nice for interactive use

The goal of this project is to make RPC easy to use - especially for people doing interactive work.  Here are the core features

- pluggable authentication
- passing standard out/ std err back to the client
- passing exceptions back to the client
- supporting http and zeromq
- supporting json, and more efficient binary serialization methods (we will provide some by default, and you can also write your own)
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

### Serialization

Json serialization is fairly simple.  Any python output (assuming it's JSON serializeable will be dumped).  If your function returns python dicts/lists, we will handle it automatically.  We will also handle numpy arrays and pandas dataframes.

numpy array 

```
{'type' : 'np.ndarray',
'data' : {'dtype' : 'f8',
          'shape' : [5],
          'data' : [1,2,3,4,5]}}
```
PandasDataframe
```
{'type' : 'pandas',
'data' : {'row_index' : serialized_pandas index goes here
          'columns' : serialized_pandas index goes here
          'data' : serialized pandas goes here as a numpy record array}}
```

For Python clients, we can do more efficient binary serialization.  The format of that will be written in binary as:

size_of_metadata size_of_data_as_int64 metadata raw_data

the metadata if {'type' : typename} will refer to a positional argument or return value.
the metadata of {'type' : typename, 'key' : name} will be unpacked into a dictionary (such as kwargs)

if you return a list, we can interate over the results on the other side.  If you return a dict we'll read in the whole thing.  Nested objects aren't handled intelligently

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

