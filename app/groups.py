import json
from flask import Flask, g, Response, request
from app import app
from queries import groups as q_groups
from .routes import get_db


@app.route("/groups")
def get_groups():
    db = get_db()
    categories = db.read_transaction(q_groups.get_categories)
    list_groups = []
    for category in categories:
        name = category[0]
        package_count = db.read_transaction(q_groups.get_dataset_count, request.args.get("group", name))
        list_groups.append(
            {
                'name': name,
                'title': {
                    'de': category[1],
                    'fr': category[2],
                    'en': category[3],
                    'it': category[4],
                },
                'package_count': package_count,
            }
        )
    return Response(json.dumps(
        {
            "help": "group_list",
            "success": True,
            "result": list_groups
        }), mimetype="application/json")
