from utils import query_builder
from utils import dataset_results


def dataset_search(tx, facet_dict, query_term):
    print(f"\n{facet_dict}\n")
    all_facets = []
    facet_keys = facet_dict.keys()
    if 'groups' in facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['groups'],
            query="(d:Dataset)-[:HAS_THEME]->({}:Group)",
            id ='g',
            property = 'group_name'
        )
        all_facets.extend(facets)
    if 'organization' in facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['organization'],
            query="(d:Dataset)-[:BELONGS_TO]->({}:Organization)",
            id ='o',
            property = 'organization_name',
        )
        all_facets.extend(facets)
    if 'organization' not in facet_keys and 'political_level' in facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['political_level'],
            query="(d:Dataset)-[:BELONGS_TO]->(o:Organization)-[:HAS_LEVEL]->({}:Level)",
            id ='l',
            property = 'political_level_name',
        )
        all_facets.extend(facets)
    q = query_builder.get_facet_match_clause(all_facets, "(d:Dataset)")
    where_clause = query_builder.get_facet_where_clause(all_facets)
    if where_clause:
        q += where_clause
    return_clause = "RETURN d.dataset_identifier as id"
    q += return_clause
    print(f"\n{q}\n")
    result = tx.run(q)
    return result.value('id')


def get_datasets(tx, dataset_ids, limit, skip):
    match_clause = query_builder.get_ids_match_clause(
        match_clause="MATCH (o:Organization)<-[:BELONGS_TO]-(d:Dataset) ",
        where_property='dataset_identifier',
        match_node='d',
        ids=dataset_ids)
    return_clause = "RETURN d.dataset_identifier as id, d, o "
    pagination_clause = f"ORDER BY d.dataset_identifier Skip {skip} LIMIT {limit}"
    q = match_clause + return_clause + pagination_clause
    result = tx.run(q)
    print(f"\n{q}\n")
    datasets = dataset_results.get_dataset_dict_from_result(result)
    return datasets


def get_groups_for_datasets(tx, dataset_ids, dataset_dict):
    match_clause = query_builder.get_ids_match_clause(
        match_clause="MATCH (g:Group)<-[:HAS_THEME]-(d:Dataset) ",
        where_property='dataset_identifier',
        match_node='d',
        ids=dataset_ids)
    return_clause = "RETURN d.dataset_identifier as id, g "
    q = match_clause + return_clause
    result = tx.run(q)
    print(f"\n{q}\n")
    dataset_results.get_dataset_group_dict_from_result(result, dataset_dict)
    return dataset_dict
