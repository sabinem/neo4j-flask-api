import json
from flask import Response, request
from app import app
from queries import dataset_search as q_dataset_search
from utils import analyze_lucene
from .routes import get_db

dataset_facets = ['groups', 'res_format', 'keywords_en', 'organization', 'political_level', 'res_rights']


@app.route("/dataset-search")
def dataset_search():
    db = get_db()
    limit = request.args.get('rows', 22)
    skip = request.args.get('start', 0)
    facet_dict = analyze_lucene.analyze_fq(request.args.get('fq'), dataset_facets)
    query_term = request.args.get('q')
    dataset_ids = db.read_transaction(
        q_dataset_search.dataset_search,
        facet_dict,
        query_term,
    )
    datasets_dict = db.read_transaction(
        q_dataset_search.get_datasets,
        dataset_ids,
        limit,
        skip
    )
    datasets_group_dict = db.read_transaction(
        q_dataset_search.get_groups_for_datasets,
        dataset_ids,
        datasets_dict,
    )
    datasets = list(datasets_dict.values())
    return Response(json.dumps(
        {
            "help": "request.url",
            "success": True,
            "result": {
                "count": len(dataset_ids),
                "sort": "core desc, metadata_modified desc",
                "facets": {
                    "res_format": {},
                    "political_level": {},
                    "groups": {},
                    "organizations": {},
                    "keywords_en": {},
                    "res_rights": {},
                },
                "results": datasets,
                "search_facets": {
                    "res_format": {
                        "items": [],
                        "title": "res_formats",
                    },
                    "keywords_en": {
                        "items": [],
                        "title": "keywords_en",
                    },
                    "groups": {
                        "items": [],
                        "title": "groups",
                    },
                    "organization": {
                        "items": [],
                        "title": "organization",
                    },
                    "political_level": {
                        "items": [],
                        "title": "political_level",
                    },
                    "res_rights": {
                        "items": [],
                        "title": "groups",
                    },
                }
            }
        }), mimetype="application/json")
