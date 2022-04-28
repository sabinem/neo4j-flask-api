import json


def format_groups_facet_result(result):
    search_facets = []
    for record in result:
        facet_dict = {}
        title_dict = {}
        facet_dict['count'] = record['count_g']
        for k, v in record['g'].items():
            if k == 'group_name':
                facet_dict['name'] = v
            if k.startswith('title'):
                title_dict[k.strip('title_')] = v
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
