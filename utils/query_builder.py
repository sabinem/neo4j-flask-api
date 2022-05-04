from collections import namedtuple

Facet = namedtuple('Facet', ['query', 'id', 'property', 'value'])


def prepare_facets(value_list, query, id, property):
    print("prepare facet")
    print(value_list)
    facets = []
    for value in value_list:
        facet = Facet(query, id + str(value_list.index(value)), property, value)
        facets.append(facet)
    return facets


def get_facet_match_clause(facets, match_default):
    print("==================FACETS match clause")
    match_clause = "MATCH "
    if facets:
        print(facets)
        match_facets = ','.join([
            item.query.format(item.id)
            for item in facets])
        print(match_facets)
        match_clause += match_facets
    else:
        match_clause += match_default
    return match_clause + " "


def get_facet_where_clause(facets):
    if not facets:
        return None
    where_conditions = ' AND '.join([f"{item.id}.{item.property}='{item.value}'" for item in facets])
    return "WHERE " + where_conditions + " "


def get_ids_match_clause(match_clause, match_node, where_property, ids):
    ids_list = ','.join([f"'{id}'" for id in ids])
    match_clause = f"{match_clause} WHERE {match_node}.{where_property} IN [{ids_list}]"
    return match_clause + " "
