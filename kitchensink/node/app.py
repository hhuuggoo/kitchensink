from flask import Flask, Blueprint

app = Flask('kitchensink.node')
nodeblueprint = Blueprint('kitchensink.node',
                          'kitchensink.node')
