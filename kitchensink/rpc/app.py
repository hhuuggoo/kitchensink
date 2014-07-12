
from flask import Flask, Blueprint

app = flask.Flask('kitchensink.node')
rpcblueprint = flask.Blueprint('kitchensink.node',
                               'kitchensink.node')
