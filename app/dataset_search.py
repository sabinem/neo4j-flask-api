import json
from flask import Response, request
from app import app
from queries import dataset_search as query
from utils import analyze_lucene
from .routes import get_db


@app.route("/dataset-search")
def dataset_search():
    db = get_db()
    print("=========== new search for datasets")
    limit = request.args.get('rows', 22)
    skip = request.args.get('start', 0)
    print(request.args.get('fq'))
    facet_keys = request.args.getlist('facet.field')
    fq_facet_dict = analyze_lucene.analyze_fq(request.args.get('fq'), facet_keys)
    query_term = request.args.get('q')
    dataset_ids = db.read_transaction(
        query.dataset_search,
        fq_facet_dict,
        query_term,
        facet_keys,
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
    facets = {}
    search_facets = {}
    for facet_key in facet_keys:
        search_facets[facet_key], facets[facet_key] = db.read_transaction(
            query.get_facets_for_datasets,
            dataset_ids,
            facet_key
        )
    for facet_key in facet_keys:
        search_facets[facet_key] = {'items': search_facets[facet_key],
                                    'title': facet_key}
    datasets = list(datasets_dict.values())
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": {
                "count": len(dataset_ids),
                "sort": "core desc, metadata_modified desc",
                "facets": facets,
                "results": datasets,
                "search_facets": search_facets
            }
        }), mimetype="application/json")
