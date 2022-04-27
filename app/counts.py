import json
from flask import Flask, g, Response, request
from app import app
from queries import counts as q_counts
from .routes import get_db


@app.route("/counts")
def get_counts():
    db = get_db()
    groups = db.read_transaction(q_counts.get_groups)
    group_counts = {}
    for group in groups:
        group_count = db.read_transaction(q_counts.get_dataset_count_for_group, request.args.get("group", group))
        group_counts[group] = group_count
    dataset_count = db.read_transaction(q_counts.get_dataset_count)
    showcase_count = db.read_transaction(q_counts.get_showcase_count)
    organization_count = db.read_transaction(q_counts.get_organization_count)
    return Response(json.dumps(
        {
            "help": "ckan counts",
            "success": True,
            "result": {
                'total_dataset_count': dataset_count,
                'showcase_count': showcase_count,
                'groups': group_counts,
                'organization_count': organization_count,
            }
        }), mimetype="application/json")
