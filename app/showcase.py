import json
from flask import Response, request
from app import app
from queries import showcase as q_showcase
from utils import api_responses
from .routes import get_db


@app.route("/showcase")
def detail():
    id = request.args.get('id')
    if not id:
        return api_responses.error_response(
            help=request.url,
            type="Validation Error",
            value="id",
            msg="Missing Value"
        )
    db = get_db()
    output = db.read_transaction(
        q_showcase.get_showcase,
        id
    )
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": output,
        }), mimetype="application/json")
