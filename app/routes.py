from app import app
from flask import jsonify, request

datasets = [
  { 'about': ' a catalog of datasets', 'storage': 'neo4j instance' }
]


@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"


@app.route('/api')
def get_datasets():
  return jsonify(datasets)