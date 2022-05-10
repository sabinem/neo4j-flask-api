import json
from flask import Response, request
from app import app
from queries import showcase_search as q_showcase_search
from utils import analyze_lucene
from .routes import get_db

showcase_facets = ['tags', 'groups', 'showcase_type']


@app.route("/showcase-search")
def showcase_search():
    db = get_db()
    limit = request.args.get('rows', 20)
    skip = request.args.get('start', 0)
    facet_dict = analyze_lucene.analyze_fq(request.args.get('fq'), showcase_facets)
    query_term = request.args.get('q')

    count, filter_by_showcase_ids, filtered_search = db.read_transaction(
        q_showcase_search.showcase_facet_search,
        facet_dict,
    )

    if query_term:
        count, filter_by_showcase_ids, filtered_search = db.read_transaction(
            q_showcase_search.get_query_search,
            query_term,
            filter_by_showcase_ids,
            filtered_search,
        )

    showcases = db.read_transaction(
        q_showcase_search.get_showcases,
        filter_by_showcase_ids,
        filtered_search,
        limit,
        skip)

    showcase_type_search_facets, showcase_type_facets = db.read_transaction(
        q_showcase_search.get_showcase_type_facets,
        filter_by_showcase_ids,
        filtered_search,
    )

    groups_search_facets, groups_facets = db.read_transaction(
        q_showcase_search.get_groups_facets,
        filter_by_showcase_ids,
        filtered_search,
    )

    tags_search_facets, tags_facets = db.read_transaction(
        q_showcase_search.get_tags_facets,
        filter_by_showcase_ids,
        filtered_search,
    )

    return Response(json.dumps(
        {
            "help": request.url,
            "success": True,
            "result": {
                "count": count,
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
