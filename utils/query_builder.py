import pandas as pd
from collections import namedtuple

Facet = namedtuple('Facet', ['query', 'ids', 'property', 'value'])


def prepare_facets(values, query, ids, property):
    facets = []
    for value in values:
        facet = Facet(query, [id + str(values.index(value)) for id in ids], property, value)
        facets.append(facet)
    return facets


def get_facet_match_clause(facets, node_id, node_label):
    if not facets:
        return f"MATCH ({node_id}:{node_label}) "
    q = "MATCH "
    if facets:
        facet_queries = [item.query.format(*[id for id in item.ids]) for item in facets]
        match_facets = ','.join(facet_queries)
        q += match_facets
    else:
        q += match_default
    return q


def get_facet_where_clause(facets):
    if not facets:
        return None
    where_conditions = ' AND '.join([f"{item.ids[-1]}.{item.property}='{item.value}'" for item in facets])
    return "WHERE " + where_conditions + " "


def get_filter_by_ids_where_clause(condition, filter_by_ids):
    ids_list = ','.join([f"'{id}'" for id in filter_by_ids])
    where_clause = f"WHERE {condition} IN [{ids_list}]"
    return where_clause + " "


def map_search_result(result, filtered_search=True):
    df = pd.DataFrame(result.data())
    count = str(df['count'].sum())
    filter_by_ids = df['id'].to_list()
    return count, filter_by_ids, filtered_search

