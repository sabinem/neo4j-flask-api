import json
from flask import Flask, g, Response, request
from app import app
from queries import groups as q_groups
from .routes import get_db


@app.route("/groups")
def get_categories_with_title():
    db = get_db()
    groups = db.read_transaction(q_groups.get_categories_with_titles_and_counts)
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": groups
        }), mimetype="application/json")
