from utils import query_builder
from utils import dataset_results as result_mapping
from utils import utils
languages = ['de', 'fr', 'en', 'it']

map_facet_match_clause = {
    'groups': "MATCH (facet:Group)<-[:HAS_THEME]-(d:Dataset) ",
    'res_format': "MATCH (facet:Format)<-[:HAS_FORMAT]-(dist:Distribution)<-[:HAS_DISTRIBUTION]-(d:Dataset) ",
    'organization': "MATCH (facet:Organization)<-[:BELONGS_TO]-(d:Dataset) ",
    'political_level': "MATCH (facet:Level)<-[:HAS_LEVEL]-(o:Organization)<-[:BELONGS_TO]-(d:Dataset) ",
    'res_rights': "MATCH (facet:Termsofuse)<-[:HAS_RIGHTS]-(dist:Distribution)<-[:HAS_DISTRIBUTION]-(d:Dataset) ",
    'keywords_de': "MATCH (facet:KeywordDe)<-[:HAS_KEYWORD]-(d:Dataset) ",
    'keywords_en': "MATCH (facet:KeywordEn)<-[:HAS_KEYWORD]-(d:Dataset) ",
    'keywords_it': "MATCH (facet:KeywordIt)<-[:HAS_KEYWORD]-(d:Dataset) ",
    'keywords_fr': "MATCH (facet:KeywordFr)<-[:HAS_KEYWORD]-(d:Dataset) ",
}


def dataset_search(tx, facet_dict, query_term, facet_keys):
    print("--------- facet_dict")
    print(f"\n{facet_dict}\n")
    all_facets = []
    fq_facet_keys = facet_dict.keys()
    if 'groups' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['groups'],
            query="(d:Dataset)-[:HAS_THEME]->({}:Group)",
            ids =['g'],
            property = 'group_name'
        )
        all_facets.extend(facets)
    if 'organization' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['organization'],
            query="(d:Dataset)-[:BELONGS_TO]->({}:Organization)",
            ids =['o'],
            property = 'organization_name',
        )
        all_facets.extend(facets)
    if 'organization' not in fq_facet_keys and 'political_level' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['political_level'],
            query="(d:Dataset)-[:BELONGS_TO]->(o:Organization)-[:HAS_LEVEL]->({}:Level)",
            ids=['l'],
            property = 'political_level_name',
        )
        all_facets.extend(facets)
    if 'res_rights' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['res_rights'],
            query="(d:Dataset)-[:HAS_DISTRIBUTION]->({}:Distribution)-[:HAS_RIGHTS]->({}:Termsofuse)",
            ids =['dist','r'],
            property = 'right',
        )
        all_facets.extend(facets)
    if 'res_format' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['res_format'],
            query="(d:Dataset)-[:HAS_DISTRIBUTION]->({}:Distribution)-[:HAS_FORMAT]->({}:Format)",
            ids =['dist','f'],
            property = 'format',
        )
        all_facets.extend(facets)
    keyword_facet = utils.get_keyword_facet_key(fq_facet_keys)
    if keyword_facet:
        facet = keyword_facet[0]
        lang = keyword_facet[1]
        facets = query_builder.prepare_facets(
            values=facet_dict[facet],
            query=f"(d:Dataset)-[:HAS_KEYWORDS_{lang.upper()}]" + "->({}" +f"{facet})",
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
    datasets = result_mapping.get_dataset_dict_from_result(result)
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
    result_mapping.get_dataset_group_dict_from_result(result, dataset_dict)
    return dataset_dict


def get_facets_for_datasets(tx, dataset_ids, facet_key):
    match_clause = query_builder.get_ids_match_clause(
        match_clause=map_facet_match_clause[facet_key],
        where_property='dataset_identifier',
        match_node='d',
        ids=dataset_ids)
    return_clause = "RETURN facet, count(facet) as count_facet"
    q = match_clause + return_clause
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    print(facet_key)
    print(map_facet_match_clause[facet_key])
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    print(q)
    result = tx.run(q)
    return result_mapping.format_facet_result(result, facet_key)
