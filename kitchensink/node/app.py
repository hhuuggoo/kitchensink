from flask import Flask, Blueprint

app = flask.Flask('kitchensink.node')
nodeblueprint = flask.Blueprint('kitchensink.node',
                                'kitchensink.node')
