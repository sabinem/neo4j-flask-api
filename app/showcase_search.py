import json
from flask import Response, request
from app import app
from queries import showcase_search as q_showcase_search
from queries import counts as q_counts
from .routes import get_db

@app.route("/showcase-search")
def search():
    db = get_db()
    records = db.read_transaction(q_showcase_search.search, request.args)
    from pprint import pprint
    pprint(records)
    for record in records:
        pprint(record['count'])
        pprint(record['showcase'])
        pprint(record['group'])
    return Response(json.dumps(
        {
            "help": "showcase_list",
            "success": True,
            "result": "result",
        }), mimetype="application/json")
