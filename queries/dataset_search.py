from utils import query_helpers as q_helpers
from utils import result_helpers as r_helpers


def dataset_search(tx, facet_dict, query_term):
    print(facet_dict)
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
    result = tx.run(q)
    return result.value('id')


def get_datasets(tx, dataset_ids, limit, skip):
    match_clause = q_helpers.get_ids_match_clause(
        match_clause="MATCH (o:Organization)<-[:BELONGS_TO]-(d:Dataset)-[:HAS_THEME]->(g:Group)",
        where_property='dataset_identifier',
        match_node='d',
        ids=dataset_ids)
    return_clause = "RETURN d, o, g "
    pagination_clause = f"ORDER BY d.dataset_identifier Skip {skip} LIMIT {limit}"
    q = match_clause + return_clause + pagination_clause
    result = tx.run(q)
    result_grouped = r_helpers.aggregate_per_result_key(result)
    datasets = r_helpers.map_datasets(result_grouped['d'])
    return datasets
