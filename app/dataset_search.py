import json
from flask import Response, request
from app import app
from queries import dataset_search as query
from utils import analyze_lucene
from .routes import get_db

dataset_facet_keys = ['groups', 'res_format', 'keywords_en', 'organization', 'political_level', 'res_rights']


@app.route("/dataset-search")
def dataset_search():
    db = get_db()
    print("=========== new search for datasets")
    limit = request.args.get('rows', 22)
    skip = request.args.get('start', 0)
    print(request.args.get('fq'))
    facet_dict = analyze_lucene.analyze_fq(request.args.get('fq'), dataset_facet_keys)
    query_term = request.args.get('q')
    dataset_ids = db.read_transaction(
        query.dataset_search,
        facet_dict,
        query_term,
    )
    datasets_dict = db.read_transaction(
        query.get_datasets,
        dataset_ids,
        limit,
        skip
    )
    datasets_group_dict = db.read_transaction(
        query.get_groups_for_datasets,
        dataset_ids,
        datasets_dict,
    )
    search_facets = {}
    facets = {}
    for facet_key in  query.dataset_facet_keys:
        search_facets[facet_key], facets[facet_key] = db.read_transaction(
            query.get_facets_for_datasets,
            dataset_ids,
            facet_key
        )
    datasets = list(datasets_dict.values())
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": {
                "count": len(dataset_ids),
                "sort": "core desc, metadata_modified desc",
                "facets": {
                    "res_format": facets['res_format'],
                    "political_level": facets['political_level'],
                    "groups": facets['groups'],
                    "organizations": facets['organization'],
                    "keywords_en": {},
                    "res_rights": facets['res_rights'],
                },
                "results": datasets,
                "search_facets": {
                    "res_format": {
                        "items": search_facets['res_format'],
                        "title": "res_formats",
                    },
                    "keywords_en": {
                        "items": [],
                        "title": "keywords_en",
                    },
                    "groups": {
                        "items": search_facets['groups'],
                        "title": "groups",
                    },
                    "organization": {
                        "items": search_facets['organization'],
                        "title": "organization",
                    },
                    "political_level": {
                        "items": search_facets['political_level'],
                        "title": "political_level",
                    },
                    "res_rights": {
                        "items": search_facets['res_rights'],
                        "title": "groups",
                    },
                }
            }
        }), mimetype="application/json")
