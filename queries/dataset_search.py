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
    print(q)
    result = tx.run(q)
    return result.value('id')


def get_query_search(tx, term, dataset_ids):
    q = f"""CALL db.index.fulltext.queryNodes('dataset_de', '"{term}"') YIELD node, score"""
    where_clause = q_helpers.get_ids_match_clause(
        match_clause="",
        where_property='dataset_identifier',
        match_node='node',
        ids=dataset_ids
    )
    return_clause = "RETURN node.dataset_identifier as id, score as score"
    q += where_clause + return_clause
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
    result.peek()
    datasets = []
    for record in result:
        dataset_dict = {}
        title_dict = {}
        description_dict = {}
        for k, v in record['d'].items():
            if k.startswith('title_'):
                title_dict[k.replace('title_', '')] = v
            elif k.startswith('description_'):
                description_dict[k.replace('description_', '')] = v
            else:
                dataset_dict[k] = v
        dataset_dict['title'] = title_dict
        dataset_dict['description'] = description_dict
        title_dict = {}
        description_dict = {}
        for k, v in record['d'].items():
            if k.startswith('title_'):
                title_dict[k.replace('title_', '')] = v
            elif k.startswith('description_'):
                description_dict[k.replace('description_', '')] = v
            else:
                dataset_dict[k] = v
        dataset_dict['title'] = title_dict
        dataset_dict['description'] = description_dict
        datasets.append(dataset_dict)
    return datasets


def get_groups_facets(tx,dataset_ids):
    match_clause = q_helpers.get_ids_match_clause(
        match_clause="MATCH (g:Group)<-[:HAS_THEME]-(d:Dataset) ",
        where_property='dataset_identifier',
        match_node='d',
        ids=dataset_ids)
    return_clause = "RETURN g, count(g) as count_g"
    q = match_clause + return_clause
    result = tx.run(q)
    return r_helpers.format_groups_facet_result(result)
