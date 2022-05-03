import json
from collections import defaultdict


def format_groups_facet_result(result):
    search_facets = []
    for record in result:
        facet_dict = {}
        title_dict = {}
        facet_dict['count'] = record['count_g']
        for k, v in record['g'].items():
            if k == 'group_name':
                facet_dict['name'] = v
            if k.startswith('title_'):
                title_dict[k.replace('title_', '')] = v
        facet_dict['display_name'] = json.dumps(title_dict)
        search_facets.append(facet_dict)
    facets = {item['name']: item['count']  for item in search_facets}
    return (search_facets, _get_facets_from_search_facets(search_facets))


def format_facet_result(result, label, count, id_property):
    search_facets = []
    for record in result:
        facet_dict = {}
        facet_dict['count'] = record[count]
        for k, v in record[label].items():
            if k == id_property:
                facet_dict['name'] = v
                facet_dict['display_name'] = v
        search_facets.append(facet_dict)
    return (search_facets, _get_facets_from_search_facets(search_facets))


def _get_facets_from_search_facets(search_facets):
    return {item['name']: item['count'] for item in search_facets}


def aggregate_per_result_key(result):
    records = [record for record in result]
    results_per_key = {}
    for key in result.keys():
        results_per_key[key] = list(set([record[key] for record in records]))
    return results_per_key


def map_showcase(showcase_record):
    showcase_dict = {}
    for k, v in showcase_record.items():
        showcase_dict[k] = v
        if k == 'showcase_name':
            showcase_dict['name'] = v
    return showcase_dict


def map_showcase_type(showcase_dict, showcase_type_records):
    showcase_type_record = showcase_type_records[0]
    for k, v in showcase_type_record.items():
        showcase_dict['showcase_type'] = v


def map_showcase_groups(showcase_dict, showcase_group_records):
    showcase_dict['groups'] = []
    for showcase_group_record in showcase_group_records:
        group = {}
        title_dict= {}
        for k,v in showcase_group_record.items():
            if k.startswith('title_'):
                title_dict[k.replace('title_', '')] = v
                continue
            elif k == 'group_name':
                group['name'] = k
            group[k] = v
        group['title'] = json.dumps(title_dict)
        data_dict['groups'].append(group)


def map_showcase_tags(showcase_dict, showcase_tag_records):
    showcase_dict['tags'] = []
    for showcase_tag_record in showcase_tag_records:
        for k,v in showcase_tag_record.items():
            showcase_dict['tags'].append({'name': v, 'display_name': v})


def get_dataset_dict_from_result(result):
    datasets = defaultdict(dict)
    assert(result.keys()[0] == 'id')
    for record in result:
        for key, value in record.items():
            if key == 'id':
                id = value
            elif key == 'o':
                datasets[id]['organization'] = map_organization(value)
            elif key == 'd':
                datasets[id] = map_dataset(value)
    return datasets


def map_organization(organization_value):
    organization_dict = {}
    title_dict = {}
    for k, v in organization_value.items():
        if k.startswith('title_'):
            title_dict[k.replace('title_', '')] = v
        elif k == 'organization_name':
            organization_dict['name'] = v
    organization_dict['title'] = title_dict
    return organization_dict


def map_dataset(dataset_value):
    dataset_dict = {}
    title_dict = {}
    description_dict = {}
    for k, v in dataset_value.items():
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


def get_dataset_group_dict_from_result(result, dataset_dict):
    assert(result.keys()[0] == 'id')
    id = None
    for record in result:
        for key, value in record.items():
            if key == 'id' and value != id:
                print(f"{key} for {id}")
                id = value
                dataset_dict[id]['groups'] = []
            elif key == 'g':
                print(f"{key} for {id}")
                dataset_dict[id]['groups'].append(map_group(value))
    print(dataset_dict.keys())
    return dataset_dict


def map_group(group_value):
    group_dict = {}
    title_dict = {}
    for k, v in group_value.items():
        if k.startswith('title_'):
            title_dict[k.replace('title_', '')] = v
        elif k == 'group_name':
            group_dict['name'] = v
    group_dict['display_name'] = title_dict
    return group_dict
