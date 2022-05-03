from collections import defaultdict
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
