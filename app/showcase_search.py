import json
from flask import Response, request
from app import app
from queries import showcase_search as q_showcase_search
from queries import counts as q_counts
from .routes import get_db

@app.route("/showcase-search")
def search():
    db = get_db()
    showcase_ids = db.read_transaction(q_showcase_search.search, request.args)
    showcases = db.read_transaction(q_showcase_search.get_showcases, showcase_ids)
    showcase_dataset_count = db.read_transaction(q_showcase_search.get_datasets_per_showcases_count, showcase_ids)
    for showcase in showcases:
        showcase['num_datasets'] = showcase_dataset_count.get(showcase['showcase_name'])
    return Response(json.dumps(
        {
            "help": "showcase_list",
            "success": True,
            "result": {
                "count": len(showcase_ids),
                "sort": "core desc, metadata_modified desc",
                "results": showcases,
            }
        }), mimetype="application/json")
