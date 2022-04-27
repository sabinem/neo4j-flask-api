import json
from flask import Flask, g, Response, request
from app import app
from queries import showcases as q_showcases
from queries import counts as q_counts
from .routes import get_db


@app.route("/showcases")
def get_showcases():
    db = get_db()
    showcases = db.read_transaction(q_showcases.get_showcases)
    showcase_dataset_count = db.read_transaction(q_showcases.get_datasets_per_showcases_count)
    for showcase in showcases:
        showcase['num_datasets'] = showcase_dataset_count.get(showcase['name'])
    showcase_count = db.read_transaction(q_counts.get_showcase_count)
    applications = db.read_transaction(q_showcases.get_applications)
    groups = db.read_transaction(q_showcases.get_groups)
    tags_detailed = db.read_transaction(q_showcases.get_tags_detail)
    applications_detailed = db.read_transaction(q_showcases.get_applications_detail)
    groups_detailed = db.read_transaction(q_showcases.get_groups_detail)
    tags = db.read_transaction(q_showcases.get_tags)
    return Response(json.dumps(
        {
            "help": "showcase_list",
            "success": True,
            "result": {
                "count": showcase_count,
                "sort": "core desc, metadata_modified desc",
                "facets": {
                    "tags": tags,
                    "groups": groups,
                    "showcase_type": applications,
                },
                "results": showcases,
                "search_facets": {
                    "showcase_type": {
                        "items": applications_detailed,
                        "title": "showcase_type",
                    },
                    "groups": {
                        "items": groups_detailed,
                        "title": "groups",
                    },
                    "tags": {
                        "items": tags_detailed,
                        "title": "tags",
                    },
                }
            }
        }), mimetype="application/json")
