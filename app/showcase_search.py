import json
from flask import Response, request
from app import app
from queries import showcase_search as q_showcase_search
from utils import request_helpers as helpers
from .routes import get_db

@app.route("/showcase-search")
def search():
    db = get_db()
    limit = request.args.get('rows', 20)
    skip = request.args.get('start', 0)
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
    showcase_type_search_facets, showcase_type_facets = db.read_transaction(
        q_showcase_search.get_showcase_type_facets,
        showcase_ids
    )
    groups_search_facets, groups_facets = db.read_transaction(
        q_showcase_search.get_groups_facets,
        showcase_ids
    )
    tags_search_facets, tags_facets = db.read_transaction(
        q_showcase_search.get_tags_facets,
        showcase_ids
    )
    return Response(json.dumps(
        {
            "help": "showcase_list",
            "success": True,
            "result": {
                "count": len(showcase_ids),
                "sort": "core desc, metadata_modified desc",
                "facets": {
                    "tags": tags_facets,
                    "groups": groups_facets,
                    "showcase_type": showcase_type_facets,
                },
                "results": showcases,
                "search_facets": {
                    "showcase_type": {
                        "items": showcase_type_search_facets,
                        "title": "showcase_type",
                    },
                    "groups": {
                        "items": groups_search_facets,
                        "title": "groups",
                    },
                    "tags": {
                        "items": tags_search_facets,
                        "title": "tags",
                    },
                }
            }
        }), mimetype="application/json")
