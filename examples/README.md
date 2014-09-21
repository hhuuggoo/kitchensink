### Examples

Some examples are labeled single-node setup.  These you should be able to start
kitchensink with

```
python -m kitchensink.scripts.start --datadir /tmp/data1 --num-workers 3
```

Some examples are labeled multi-node. For these, you must start redis separately, and startup 3 kitchensink nodes

```
start-redis

python -m kitchensink.scripts.start --datadir /tmp/data1 --no-redis --node-url http://localhost:6323/
python -m kitchensink.scripts.start --datadir /tmp/data2 --no-redis --node-url http://localhost:6324/
python -m kitchensink.scripts.start --datadir /tmp/data3 --no-redis --node-url http://localhost:6325/
```
