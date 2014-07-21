python -m kitchensink.scripts.start --reload --module kitchensink.testutils.testmodule --datadir /tmp/data2 --no-redis --node-port=6324 --node-url=http://localhost:6324/
python -m kitchensink.scripts.start --reload --module kitchensink.testutils.testmodule --datadir /tmp/data3 --no-redis --node-port=6325 --node-url=http://localhost:6325/
python -m kitchensink.scripts.start --reload --module kitchensink.testutils.testmodule --datadir /tmp/data1
