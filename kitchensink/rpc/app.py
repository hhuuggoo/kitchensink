from flask import Flask, Blueprint

app = Flask('kitchensink.node')
rpcblueprint = Blueprint('kitchensink.node',
                         'kitchensink.node')
rpcblueprint.rpcs = {}
