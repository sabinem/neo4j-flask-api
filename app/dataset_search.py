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
    print("REQUEST-------------------------------------------------")
    print(request)
    print("--------------------------------------------------------")
    print(datetime.now())
    query_table = []
    db = get_db()
    limit = int(request.args.get('rows', 22))
    skip = int(request.args.get('start', 0))
    facet_keys = request.args.getlist('facet.field')
    if not facet_keys:
        facet_keys = dataset_facet_keys

    fq_facet_dict = analyze_lucene.analyze_fq(request.args.get('fq'), facet_keys)
    t = [datetime.now()]
    query_term = request.args.get('q')
    q, count, filter_by_dataset_ids, filtered_search, = db.read_transaction(
        q_dataset_search.dataset_search,
        fq_facet_dict,
    )
    t.append(datetime.now())
    print(t[-1] - t[-2])
    query_table.append({'title': 'facet_filter_query', 'time': t[-1] - t[-2], 'query': q})
    if query_term:
        q, count, filter_by_dataset_ids, filtered_search, = db.read_transaction(
            q_dataset_search.get_query_search,
            query_term,
            filter_by_dataset_ids,
            filtered_search,
        )
        t.append(datetime.now())
        print(t[-1] - t[-2])
        query_table.append({'title': 'search_query', 'time': t[-1] - t[-2], 'query': q})
    dataset_ids_on_page = filter_by_dataset_ids[skip:skip+limit]
    q, datasets_dict = db.read_transaction(
        q_dataset_search.get_datasets,
        dataset_ids_on_page,
    )
    t.append(datetime.now())
    print(t[-1] - t[-2])
    query_table.append({'title': 'get datasets for page', 'time': t[-1] - t[-2], 'query': q})
    q = db.read_transaction(
        q_dataset_search.get_groups_for_datasets,
        dataset_ids_on_page,
        datasets_dict,
    )
    t.append(datetime.now())
    print(t[-1] - t[-2])
    query_table.append({'title': 'get groups for datasets on page', 'time': t[-1] - t[-2], 'query': q})
    q = db.read_transaction(
        q_dataset_search.get_resources_for_datasets,
        dataset_ids_on_page,
        datasets_dict,
    )
    t.append(datetime.now())
    print(t[-1] - t[-2])
    query_table.append({'title': 'get resources for datasets on page', 'time': t[-1] - t[-2], 'query': q})
    request_language = utils.get_request_language(facet_keys)
    q = db.read_transaction(
        q_dataset_search.get_keywords_for_datasets,
        dataset_ids_on_page,
        datasets_dict,
        request_language
    )
    t.append(datetime.now())
    print(t[-1] - t[-2])
    query_table.append({'title': 'get keywords for datasets on page', 'time': t[-1] - t[-2], 'query': q})
    datasets = list(datasets_dict.values())
    facets = {}
    search_facets = {}
    for facet_key in dataset_facet_keys:
        q, facets[facet_key] = db.read_transaction(
            q_dataset_search.get_facet_counts,
            filter_by_dataset_ids,
            filtered_search,
            facet_key
        )
        print(facets[facet_key])
        t.append(datetime.now())
        print(t[-1] - t[-2])
        query_table.append({'title': f'get facet counts for facet {facet_key}', 'time': t[-1] - t[-2], 'query': q})
        q, facet_items = db.read_transaction(
            q_dataset_search.get_facet_items,
            facet_key
        )
        t.append(datetime.now())
        print(t[-1] - t[-2])
        query_table.append({'title': f'get facet items for facet {facet_key}', 'time': t[-1] - t[-2], 'query': q})
        search_facets[facet_key] = utils.get_search_facets(facets[facet_key], facet_items, facet_key)
    for facet_key in facet_keys:
        search_facets[facet_key] = {'items': search_facets[facet_key],
                                    'title': facet_key}
    df = pd.DataFrame(data=query_table)
    print(df)
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
