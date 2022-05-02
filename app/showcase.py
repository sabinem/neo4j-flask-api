import json
from flask import Response, request
from app import app
from queries import showcase as q_showcase
from utils import response_helpers as response_h
from .routes import get_db


@app.route("/showcase")
def detail():
    id = request.args.get('id')
    if not id:
        return response_h.error_response(
            help=request.url,
            type="Validation Error",
            value="id",
            msg="Missing Value"
        )
    db = get_db()
    showcase = db.read_transaction(
        q_showcase.get_showcase,
        id
    )
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": showcase
        }), mimetype="application/json")


@app.route("/showcase-datasets")
def datasets():
    id = request.args.get('id')
    if not id:
        return response_h.error_response(
            help=request.url,
            type="Validation Error",
            value="id",
            msg="Missing Value"
        )
    db = get_db()
    datasets = db.read_transaction(
        q_showcase.get_datasets_per_showcases,
        id
    )
    for dataset in datasets:
        dataset['organization'] = {'title': []}
        dataset['groups'] = {'title': []}
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": datasets
        }), mimetype="application/json")
