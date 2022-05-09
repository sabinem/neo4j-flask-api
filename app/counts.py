import json
from flask import Flask, g, Response, request
from app import app
from queries import counts as q_counts
from .routes import get_db


@app.route("/counts")
def get_counts():
    db = get_db()
    counts = db.read_transaction(q_counts.get_counts)
    groups = db.read_transaction(q_counts.get_groups)
    group_counts = {}
    for group in groups:
        group_count = db.read_transaction(q_counts.get_dataset_count_for_group, request.args.get("group", group))
        group_counts[group] = group_count
    return Response(json.dumps(
        {
            "help": "ckan counts",
            "success": True,
            "result": {
                'total_dataset_count': counts.get('Dataset'),
                'showcase_count': counts.get('Showcase'),
                'groups': group_counts,
                'organization_count': counts.get('Organization'),
            }
        }), mimetype="application/json")
