import json
from flask import Response, request
from app import app
from queries import showcase as q_showcase
from utils import request_helpers as r_helpers
from utils import response_helpers as response_h
from .routes import get_db

@app.route("/showcase")
def detail():
    print(request.url)
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
        request.args.get('id')
    )
    return Response(json.dumps(
        {
            "help": "showcase_detail",
            "success": True,
            "result": showcase
        }), mimetype="application/json")