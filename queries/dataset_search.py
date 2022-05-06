from utils import query_builder
from utils import dataset_results
languages = ['de', 'fr', 'en', 'it']
dataset_facet_keys = ['groups', 'res_format', 'organization', 'political_level', 'res_rights']
map_facet_match_clause = {
    'groups': "MATCH (facet:Group)<-[:HAS_THEME]-(d:Dataset) ",
    'res_format': "MATCH (facet:Format)<-[:HAS_FORMAT]-(dist:Distribution)<-[:HAS_DISTRIBUTION]-(d:Dataset) ",
    'organization': "MATCH (facet:Organization)<-[:BELONGS_TO]-(d:Dataset) ",
    'political_level': "MATCH (facet:Level)<-[:HAS_LEVEL]-(o:Organization)<-[:BELONGS_TO]-(d:Dataset) ",
    'res_rights': "MATCH (facet:Termsofuse)<-[:HAS_RIGHTS]-(dist:Distribution)<-[:HAS_DISTRIBUTION]-(d:Dataset) ",
}

def dataset_search(tx, facet_dict, query_term):
    print("--------- facet_dict")
    print(f"\n{facet_dict}\n")
    all_facets = []
    request_facet_keys = facet_dict.keys()
    if 'groups' in request_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['groups'],
            query="(d:Dataset)-[:HAS_THEME]->({}:Group)",
            ids =['g'],
            property = 'group_name'
        )
        all_facets.extend(facets)
    if 'organization' in request_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['organization'],
            query="(d:Dataset)-[:BELONGS_TO]->({}:Organization)",
            ids =['o'],
            property = 'organization_name',
        )
        all_facets.extend(facets)
    if 'organization' not in request_facet_keys and 'political_level' in request_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['political_level'],
            query="(d:Dataset)-[:BELONGS_TO]->(o:Organization)-[:HAS_LEVEL]->({}:Level)",
            ids=['l'],
            property = 'political_level_name',
        )
        all_facets.extend(facets)
    if 'res_rights' in request_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['res_rights'],
            query="(d:Dataset)-[:HAS_DISTRIBUTION]->({}:Distribution)-[:HAS_RIGHTS]->({}:Termsofuse)",
            ids =['dist','r'],
            property = 'right',
        )
        all_facets.extend(facets)
    if 'res_format' in request_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['res_format'],
            query="(d:Dataset)-[:HAS_DISTRIBUTION]->({}:Distribution)-[:HAS_FORMAT]->({}:Format)",
            ids =['dist','f'],
            property = 'format',
        )
        all_facets.extend(facets)
    for lang in languages:
        if f'keyword_{lang}' in request_facet_keys:
            facets = query_builder.prepare_facets(
                values=facet_dict[f'keyword_{lang}'],
                query=f"(d:Dataset)-[:HAS_KEYWORDS_{lang.upper()}]" + "->({}" +f"Keyword_{lang})",
                ids=['dist', 'f'],
                property='format',
            )
            all_facets.extend(facets)
    q = query_builder.get_facet_match_clause(all_facets, "(d:Dataset)")
    where_clause = query_builder.get_facet_where_clause(all_facets)
    if where_clause:
        q += where_clause
    return_clause = "RETURN d.dataset_identifier as id"
    q += return_clause
    print("----cypher query-----------------------------")
    print(f"\n{q}\n")
    result = tx.run(q)
    return set(result.value('id'))


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
    dataset_results.get_dataset_group_dict_from_result(result, dataset_dict)
    return dataset_dict


def get_facets_for_datasets(tx, dataset_ids, facet_key):
    match_clause = query_builder.get_ids_match_clause(
        match_clause=map_facet_match_clause[facet_key],
        where_property='dataset_identifier',
        match_node='d',
        ids=dataset_ids)
    return_clause = "RETURN facet, count(facet) as count_facet"
    q = match_clause + return_clause
    print(q)
    result = tx.run(q)
    return dataset_results.format_facet_result(result, facet_key)
