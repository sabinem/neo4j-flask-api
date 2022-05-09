import json


def map_group(group_value):
    group_dict = {}
    title_dict = {}
    for k, v in group_value.items():
        if k.startswith('title_'):
            title_dict[k.replace('title_', '')] = v
        elif k == 'group_name':
            group_dict['name'] = v
    group_dict['display_name'] = title_dict
    group_dict['title'] = title_dict
    return group_dict


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


def map_organization(organization_value):
    organization_dict = {}
    title_dict = {}
    for k, v in organization_value.items():
        if k.startswith('title_'):
            title_dict[k.replace('title_', '')] = v
        elif k == 'organization_name':
            organization_dict['name'] = v
    organization_dict['title'] = json.dumps(title_dict)
    return organization_dict


def map_organizations_to_tree(organization_tree, organization_dict):
    organizations = []
    for org, suborgs in organization_tree.items():
        item = organization_dict.get(org)
        item['children'] = [
            organization_dict.get(suborg) for suborg in suborgs
        ]
        organizations.append(item)
    return organizations
