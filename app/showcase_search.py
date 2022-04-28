import json
from flask import Response, request
from app import app
from queries import showcase_search as q_showcase_search
from queries import counts as q_counts
from utils import request_helpers as helpers
from .routes import get_db

@app.route("/showcase-search")
def search():
    db = get_db()
    limit = helpers.PAGE_SIZE
    skip = helpers.get_pagination(request.args.get('page'))
    showcase_ids = db.read_transaction(
        q_showcase_search.search,
        request.args)
    showcases = db.read_transaction(
        q_showcase_search.get_showcases,
        showcase_ids,
        limit,
        skip)
    showcase_ids_page = [showcase.get('showcase_name') for showcase in showcases]
    showcase_dataset_count = db.read_transaction(
        q_showcase_search.get_datasets_per_showcases_count,
        showcase_ids_page)
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
