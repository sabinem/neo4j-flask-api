import json
from collections import defaultdict

import utils.utils
from utils import map_neo4j_to_api


def get_dataset_dict_from_result(result):
    datasets = defaultdict(dict)
    assert(result.keys()[0] == 'id')
    for record in result:
        for key, value in record.items():
            if key == 'id':
                id = value
            elif key == 'o':
                datasets[id]['organization'] = map_neo4j_to_api.map_organization(value)
            elif key == 'd':
                datasets[id] = map_neo4j_to_api.map_dataset(value)
    return datasets


def get_dataset_group_dict_from_result(result, dataset_dict):
    assert(result.keys()[0] == 'id')
    id = None
    for record in result:
        for key, value in record.items():
            if key == 'id' and value != id:
                id = value
                dataset_dict[id]['groups'] = []
            elif key == 'g':
                dataset_dict[id]['groups'].append(map_neo4j_to_api.map_group(value))
    return dataset_dict


def get_dataset_resource_dict_from_result(result, dataset_dict):
    id = None
    assert(result.keys()[0] == 'id')
    for record in result:
        for key, value in record.items():
            if key == 'id' and value != id:
                id = value
                dataset_dict[id]['resources'] = []
            elif key == 'resource_id':
                resource = {'distribution_id': value}
            elif key == 'format':
                resource['format'] = value
                dataset_dict[id]['resources'].append(resource)
    return dataset_dict


def map_facet_result(result, facet_key):
    search_facet_dict = defaultdict(dict)
    assert(result.keys()[0] == 'facet_id')
    for record in result:
        for key, value in record.items():
            if key == 'facet_id':
                facet_id = value
            if key == 'dataset_id':
                if search_facet_dict[facet_id].get('count'):
                    search_facet_dict[facet_id]['count'] += 1
                else:
                    search_facet_dict[facet_id]['count'] = 1
            elif key == 'facet':
                search_facet_dict[facet_id]['facet'] = value
    search_facets = []
    for facet_dict in search_facet_dict.values():
        facet_item_dict = _map_facet(facet_dict['facet'], facet_key)
        facet_item_dict['count'] = facet_dict['count']
        search_facets.append(facet_item_dict)
    return search_facets, _get_facets_from_search_facets(search_facets)


def _map_facet(facet, facet_key):
    facet_dict = {}
    title_dict = {}
    for k, v in facet.items():
        if facet_key in ['groups', 'organization']:
            if k in ['group_name', 'organization_name']:
                facet_dict['name'] = v
            if k.startswith('title_'):
                title_dict[k.replace('title_', '')] = v
        elif facet_key == 'res_format':
            if k == 'format':
                facet_dict['name'] = v
                facet_dict['display_name'] = v
        elif facet_key == 'political_level':
            if k == 'political_level_name':
                facet_dict['name'] = v
                facet_dict['display_name'] = v
        elif facet_key == 'res_rights':
            if k == 'right':
                facet_dict['name'] = v
                facet_dict['display_name'] = v
        elif facet_key.startswith('keywords'):
            if k.startswith('keyword'):
                facet_dict['name'] = v
                facet_dict['display_name'] = v
    if facet_key in ['groups', 'organization']:
        facet_dict['display_name'] = json.dumps(title_dict)
    return facet_dict


def get_dataset_keyword_dict_from_result(result, dataset_dict, language):
    id = None
    assert(result.keys()[0] == 'id')
    for dataset in dataset_dict.values():
        dataset['keywords'] = {lang: [] for lang in utils.utils.languages}
    for record in result:
        for key, value in record.items():
            if key == 'id' and value != id:
                id = value
            elif key == 'keyword':
                dataset_dict[id]['keywords'][language].append(value)
    return dataset_dict


def _get_facets_from_search_facets(search_facets):
    return {item['name']: item['count'] for item in search_facets}
