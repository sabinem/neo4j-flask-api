import json
from flask import Flask, g, Response, request
from app import app
from queries.organizations import get_organization_list as query_for_organizations
from .routes import get_db


@app.route("/organizations")
def get_organization_list():
    db = get_db()
    organizations = db.read_transaction(query_for_organizations)
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": organizations
        }), mimetype="application/json")
