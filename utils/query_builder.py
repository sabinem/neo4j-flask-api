import json
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


def map_organization_to_api(organization):
    organization_dict = {}
    title_dict = {}
    for k, v in organization.items():
        if k.startswith('title_'):
            title_dict[k.replace('title_', '')] = v
        elif k == 'organization_name':
            organization_dict['name'] = v
    organization_dict['title'] = json.dumps(title_dict)
    return organization_dict


def map_group_to_api(group):
    group_dict = {}
    title_dict = {}
    for k, v in group.items():
        if k.startswith('title_'):
            title_dict[k.replace('title_', '')] = v
        elif k == 'group_name':
            group_dict['name'] = v
    group_dict['display_name'] = title_dict
    group_dict['title'] = title_dict
    return group_dict


def map_dataset_to_api(dataset):
    dataset_dict = {}
    title_dict = {}
    description_dict = {}
    for k, v in dataset.items():
        if k.startswith('title_'):
            title_dict[k.replace('title_', '')] = v
        elif k.startswith('description_'):
            description_dict[k.replace('description_', '')] = v
        else:
            dataset_dict[k] = v
        if k == 'dataset_identifier':
            dataset_dict['identifier'] = v
    dataset_dict['title'] = title_dict
    dataset_dict['description'] = description_dict
    return dataset_dict
