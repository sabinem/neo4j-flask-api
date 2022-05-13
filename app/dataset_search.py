import json
import pandas as pd
from datetime import datetime
from flask import Response, request
from app import app
from queries import dataset_search as q_dataset_search
from utils import analyze_lucene, utils
from .routes import get_db

dataset_facet_keys = ['groups', 'res_format', 'keywords_de', 'organization', 'political_level', 'res_rights']


@app.route("/dataset-search")
def dataset_search():
    db = get_db()
    limit = int(request.args.get('rows', 22))
    skip = int(request.args.get('start', 0))
    facet_keys = request.args.getlist('facet.field')
    if not facet_keys:
        facet_keys = dataset_facet_keys

    fq_facet_dict = analyze_lucene.analyze_fq(request.args.get('fq'), facet_keys)
    query_term = request.args.get('q')
    count, filter_by_dataset_ids, filtered_search, = db.read_transaction(
        q_dataset_search.dataset_search,
        fq_facet_dict,
    )
    if query_term:
        count, filter_by_dataset_ids, filtered_search, = db.read_transaction(
            q_dataset_search.get_query_search,
            query_term,
            filter_by_dataset_ids,
            filtered_search,
        )
    dataset_ids_on_page = filter_by_dataset_ids[skip:skip+limit]
    datasets_dict = db.read_transaction(
        q_dataset_search.get_datasets,
        dataset_ids_on_page,
    )
    db.read_transaction(
        q_dataset_search.get_groups_for_datasets,
        dataset_ids_on_page,
        datasets_dict,
    )
    db.read_transaction(
        q_dataset_search.get_resources_for_datasets,
        dataset_ids_on_page,
        datasets_dict,
    )
    request_language = utils.get_request_language(facet_keys)
    db.read_transaction(
        q_dataset_search.get_keywords_for_datasets,
        dataset_ids_on_page,
        datasets_dict,
        request_language
    )
    datasets = list(datasets_dict.values())
    facets = {}
    search_facets = {}
    for facet_key in dataset_facet_keys:
        facets[facet_key] = db.read_transaction(
            q_dataset_search.get_facet_counts,
            filter_by_dataset_ids,
            filtered_search,
            facet_key
        )
        facet_items = db.read_transaction(
            q_dataset_search.get_facet_items,
            facet_key
        )
        search_facets[facet_key] = utils.get_search_facets(facets[facet_key], facet_items, facet_key)
    for facet_key in facet_keys:
        search_facets[facet_key] = {'items': search_facets[facet_key],
                                    'title': facet_key}
    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": {
                "count": count,
                "sort": "core desc, metadata_modified desc",
                "facets": facets,
                "results": datasets,
                "search_facets": search_facets
            }
        }), mimetype="application/json")
