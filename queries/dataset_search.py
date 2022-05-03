from utils import query_helpers as q_helpers
from utils import result_helpers as r_helpers


def dataset_search(tx, facet_dict, query_term):
    print(f"\n{facet_dict}\n")
    facets = []
    facets.extend(q_helpers.prepare_facets(
        value_list=facet_dict.get('groups', []),
        q_id ='g',
        label = 'Group',
        relationship = 'HAS_THEME',
        property = 'group_name',
    ))
    q = q_helpers.get_facet_match_clause('Dataset', 'd', facets)
    where_clause = q_helpers.get_facet_where_clause(facets)
    if where_clause:
        q += where_clause
    return_clause = "RETURN d.dataset_identifier as id"
    q += return_clause
    print(f"\n{q}\n")
    result = tx.run(q)
    return result.value('id')


def get_datasets(tx, dataset_ids, limit, skip):
    match_clause = q_helpers.get_ids_match_clause(
        match_clause="MATCH (o:Organization)<-[:BELONGS_TO]-(d:Dataset) ",
        where_property='dataset_identifier',
        match_node='d',
        ids=dataset_ids)
    return_clause = "RETURN d.dataset_identifier as id, d, o "
    pagination_clause = f"ORDER BY d.dataset_identifier Skip {skip} LIMIT {limit}"
    q = match_clause + return_clause + pagination_clause
    result = tx.run(q)
    print(f"\n{q}\n")
    datasets = r_helpers.get_dataset_dict_from_result(result)
    return datasets


def get_groups_for_datasets(tx, dataset_ids, dataset_dict):
    match_clause = q_helpers.get_ids_match_clause(
        match_clause="MATCH (g:Group)<-[:HAS_THEME]-(d:Dataset) ",
        where_property='dataset_identifier',
        match_node='d',
        ids=dataset_ids)
    return_clause = "RETURN d.dataset_identifier as id, g "
    q = match_clause + return_clause
    result = tx.run(q)
    print(f"\n{q}\n")
    r_helpers.get_dataset_group_dict_from_result(result, dataset_dict)
    return dataset_dict
