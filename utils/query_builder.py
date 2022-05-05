from collections import namedtuple

Facet = namedtuple('Facet', ['query', 'ids', 'property', 'value'])


def prepare_facets(values, query, ids, property):
    facets = []
    for value in values:
        facet = Facet(query, [id + str(values.index(value)) for id in ids], property, value)
        facets.append(facet)
    return facets


def get_facet_match_clause(facets, match_default):
    print("===========================WATCH")
    match_clause = "MATCH "
    if facets:
        facet_queries = [item.query.format(*[id for id in item.ids]) for item in facets]
        match_facets = ','.join(facet_queries)
        match_clause += match_facets
    else:
        match_clause += match_default
    return match_clause + " "


def get_facet_where_clause(facets):
    if not facets:
        return None
    where_conditions = ' AND '.join([f"{item.ids[-1]}.{item.property}='{item.value}'" for item in facets])
    return "WHERE " + where_conditions + " "


def get_ids_match_clause(match_clause, match_node, where_property, ids):
    ids_list = ','.join([f"'{id}'" for id in ids])
    match_clause = f"{match_clause} WHERE {match_node}.{where_property} IN [{ids_list}]"
    return match_clause + " "
