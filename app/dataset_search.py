import json
from flask import Response, request
from app import app
from queries import dataset_search as q_dataset_search
from utils import request_helpers as request_h
from .routes import get_db


@app.route("/dataset-search")
def dataset_search():
    db = get_db()
    limit = request.args.get('rows', 20)
    skip = request.args.get('start', 0)
    facet_dict = request_h.analyze_fq(request.args.get('fq'))
    query_term = request.args.get('q')
    dataset_ids = db.read_transaction(
        q_dataset_search.dataset_search,
        facet_dict,
        query_term,
    )
    #print("--------- first step")
    #print(dataset_ids)
    if query_term:
        dataset_ids = db.read_transaction(
            q_dataset_search.get_query_search,
            query_term,
            dataset_ids,
        )
    #print("--------- second step")
    #print(dataset_ids)
    datasets = db.read_transaction(
        q_dataset_search.get_datasets,
        dataset_ids,
        limit,
        skip)
    dataset_ids_page = [dataset.get('name') for dataset in datasets]
    groups_search_facets, groups_facets = db.read_transaction(
        q_dataset_search.get_groups_facets,
        dataset_ids
    )
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