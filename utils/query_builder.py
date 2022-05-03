from collections import namedtuple

Facet = namedtuple('Facet', ['label', 'relationship', 'q_id', 'property', 'value'])


def prepare_facets(value_list, q_id, label, relationship, property):
    facets = []
    for value in value_list:
        facet = Facet(label, relationship, q_id + str(value_list.index(value)), property, value)
        facets.append(facet)
    return facets


def get_facet_match_clause(match_label, match_id, facets):
    match_clause = "MATCH "
    if facets:
        match_facets = ','.join([f"({match_id}:{match_label})-[:{item.relationship}]->({item.q_id}:{item.label})" for item in facets])
        match_clause += match_facets
    else:
        match_clause += f"({match_id}:{match_label})"
    return match_clause + " "


def get_facet_where_clause(facets):
    if not facets:
        return None
    where_conditions = ' AND '.join([f"{item.q_id}.{item.property}='{item.value}'" for item in facets])
    return "WHERE " + where_conditions + " "


def get_ids_match_clause(match_clause, match_node, where_property, ids):
    ids_list = ','.join([f"'{id}'" for id in ids])
    match_clause = f"{match_clause} WHERE {match_node}.{where_property} IN [{ids_list}]"
    return match_clause + " "
