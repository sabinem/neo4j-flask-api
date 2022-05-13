import json
import pandas as pd
from utils import query_builder
from utils.decorators import log_query
from utils.query_builder import QueryResult
from utils import utils
languages = ['de', 'fr', 'en', 'it']

map_facet_match_clause = {
    'groups': "MATCH (facet:Group)<-[:HAS_THEME]-(d:Dataset) ",
    'res_format': "MATCH (facet:Format)<-[:HAS_FORMAT]-(dist:Distribution)<-[:HAS_DISTRIBUTION]-(d:Dataset) ",
    'organization': "MATCH (facet:Organization)<-[:BELONGS_TO]-(d:Dataset) ",
    'political_level': "MATCH (facet:Level)<-[:HAS_LEVEL]-(o:Organization)<-[:BELONGS_TO]-(d:Dataset) ",
    'res_rights': "MATCH (facet:TermsOfUse)<-[:HAS_RIGHTS]-(dist:Distribution)<-[:HAS_DISTRIBUTION]-(d:Dataset) ",
    'keywords_de': "MATCH (facet:KeywordDe)<-[:HAS_KEYWORD_DE]-(d:Dataset) ",
    'keywords_en': "MATCH (facet:KeywordEn)<-[:HAS_KEYWORD_EN]-(d:Dataset) ",
    'keywords_it': "MATCH (facet:KeywordIt)<-[:HAS_KEYWORD_IT]-(d:Dataset) ",
    'keywords_fr': "MATCH (facet:KeywordFr)<-[:HAS_KEYWORD_FR]-(d:Dataset) ",
}

map_facet_to_property = {
    'groups': "group_name",
    'res_format': "format",
    'organization': "organization_name",
    'political_level': "political_level_name",
    'res_rights': "right",
    'keywords_de': "keyword_de",
    'keywords_en': "keyword_en",
    'keywords_it': "keyword_it",
    'keywords_fr': "keyword_fr",
}

map_facet_items_label = {
    'groups': "Group",
    'res_format': "Format",
    'organization': "Organization",
    'political_level': "Level",
    'res_rights': "TermsOfUse",
    'keywords_de': "KeywordDe",
    'keywords_en': "KeywordEn",
    'keywords_it': "KeywordIt",
    'keywords_fr': "KeywordFr",
}


@log_query
def dataset_search(tx, facet_dict):
    """get filtered search result by facets in request fq parameter"""
    search_facets = []
    fq_facet_keys = facet_dict.keys()
    if 'groups' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['groups'],
            query="(d:Dataset)-[:HAS_THEME]->({}:Group)",
            ids =['g'],
            property = 'group_name'
        )
        search_facets.extend(facets)
    if 'organization' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['organization'],
            query="(d:Dataset)-[:BELONGS_TO]->({}:Organization)",
            ids =['o'],
            property = 'organization_name',
        )
        search_facets.extend(facets)
    if 'organization' not in fq_facet_keys and 'political_level' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['political_level'],
            query="(d:Dataset)-[:BELONGS_TO]->(o:Organization)-[:HAS_LEVEL]->({}:Level)",
            ids=['l'],
            property = 'political_level_name',
        )
        search_facets.extend(facets)
    if 'res_rights' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['res_rights'],
            query="(d:Dataset)-[:HAS_DISTRIBUTION]->({}:Distribution)-[:HAS_RIGHTS]->({}:TermsOfUse)",
            ids =['dist','r'],
            property = 'right',
        )
        search_facets.extend(facets)
    if 'res_format' in fq_facet_keys:
        facets = query_builder.prepare_facets(
            values=facet_dict['res_format'],
            query="(d:Dataset)-[:HAS_DISTRIBUTION]->({}:Distribution)-[:HAS_FORMAT]->({}:Format)",
            ids =['dist','f'],
            property = 'format',
        )
        search_facets.extend(facets)
    keyword_facet = utils.get_keyword_facet_key(fq_facet_keys)
    if keyword_facet:
        key = f"{keyword_facet[0]}_{keyword_facet[1]}"
        lang = keyword_facet[1]
        facets = query_builder.prepare_facets(
            values=facet_dict[key],
            query=f"(d:Dataset)-[:HAS_KEYWORD_{lang.upper()}]" + "->({}" + f":Keyword{lang.capitalize()}) ",
            ids=['k'],
            property=f'keyword_{lang}',
        )
        search_facets.extend(facets)
    q = query_builder.get_facet_match_clause(search_facets, "d", "Dataset")
    where_clause = query_builder.get_facet_where_clause(search_facets)
    if where_clause:
        q += where_clause
    q += "RETURN d.dataset_identifier as id, count(distinct d) as count"
    result = tx.run(q)
    return QueryResult(query=q, result=query_builder.map_search_result(result, filtered_search=bool(search_facets)))


@log_query
def get_query_search(tx, term, filter_by_dataset_ids, filtered_search):
    """get search result for query term within filtered search result"""
    q = f"""
CALL db.index.fulltext.queryNodes('dataset_de', '"{term}"') 
YIELD node as dataset, score """
    q += _dataset_filter_clause(filter_by_dataset_ids, filtered_search)
    q += "RETURN dataset.dataset_identifier as id, count(dataset) as count"
    result = tx.run(q)
    return QueryResult(query=q, result=query_builder.map_search_result(result))


def _dataset_filter_clause(filter_by_dataset_ids, filtered_search=True, condition='d.dataset_identifier'):
    if not filtered_search:
        return ""
    return query_builder.get_filter_by_ids_where_clause(
        condition='d.dataset_identifier',
        filter_by_ids=filter_by_dataset_ids
    )


@log_query
def get_datasets(tx, dataset_ids_on_page):
    """get organizations for datasets on page"""
    q = f"""MATCH (o:Organization)<-[:BELONGS_TO]-(d:Dataset)"""
    q += _dataset_filter_clause(dataset_ids_on_page)
    q += f"""
RETURN d.dataset_identifier as dataset_id, d as dataset, o as organization
"""
    result = tx.run(q)
    dataset_dicts = _map_datasets_to_dict(result)
    return QueryResult(query=q, result=dataset_dicts)


def _map_datasets_to_dict(result):
    df = pd.DataFrame(result.data())
    df['dataset'] = df.apply(lambda x:_add_organization_to_dataset(organization=x['organization'], dataset=x['dataset']), axis=1)
    df = df.drop(columns=['organization']).set_index('dataset_id')
    datasets_dict = df['dataset'].to_dict()
    return datasets_dict


def _add_organization_to_dataset(organization, dataset):
    dataset_dict = query_builder.map_dataset_to_api(dataset)
    dataset_dict['organization'] = query_builder.map_organization_to_api(organization)
    return dataset_dict


@log_query
def get_facet_counts(tx, filter_by_dataset_ids, filtered_search, facet_key):
    """get facet counts for datasets in search result"""
    property = map_facet_to_property.get(facet_key)
    q = map_facet_match_clause[facet_key]
    q += _dataset_filter_clause(filter_by_dataset_ids, filtered_search)
    q += f"RETURN facet.{property} as label, count(facet) as count"
    result = tx.run(q)
    facet_dict = _map_facet_count_result(result)
    return QueryResult(query=q, result=facet_dict)


def _map_facet_count_result(result):
    df = pd.DataFrame.from_dict(result.data())
    if df.empty:
        return {}
    df.set_index('label', inplace=True)
    return df['count'].to_dict()


@log_query
def get_facet_items(tx, facet_key):
    label = map_facet_items_label.get(facet_key)
    property =map_facet_to_property.get(facet_key)
    q = f"MATCH (facet:{label}) RETURN facet, facet.{property} as facet_id"
    result = tx.run(q)
    facet_items_dict = _map_facet_items_result(result, facet_key)
    return QueryResult(query=q, result=facet_items_dict)


def _map_facet_items_result(result, facet_key):
    df = pd.DataFrame.from_dict(result.data())
    df['facet'] = df['facet'].apply(lambda x: _map_facet(x, facet_key))
    df = df.set_index('facet_id')
    facet_item_dict = df['facet'].to_dict()
    return facet_item_dict


def _map_facet(facet_dict, facet_key):
    facet = {}
    if facet_key == 'groups':
        facet['name'] = facet_dict['group_name']
    elif facet_key == 'organization':
        facet['name'] = facet_dict['organization_name']
    elif facet_key == 'res_format':
         facet['name'] = facet_dict['format']
    elif facet_key == 'political_level':
        facet['name'] = facet_dict['political_level_name']
    elif facet_key == 'res_rights':
        facet['name'] = facet_dict['right']
    elif facet_key.startswith('keywords'):
        item_key = facet_key.replace('s_', '_')
        facet['name'] = facet_dict[item_key]
    if facet_key not in ['groups', 'organization']:
        facet['display_name'] = facet['name']
    else:
        facet['display_name'] = json.dumps(
            {
                'de': facet_dict.get('title_de'),
                'fr': facet_dict.get('title_de'),
                'en': facet_dict.get('title_en'),
                'it': facet_dict.get('title_it'),
            }
        )
    return facet


@log_query
def get_groups_for_datasets(tx, dataset_ids_on_page, dataset_dict):
    """get categories for datasets on page"""
    q = f"MATCH (g:Group)<-[:HAS_THEME]-(d:Dataset) "
    q += _dataset_filter_clause(dataset_ids_on_page)
    q += f"""
RETURN d.dataset_identifier as dataset_id,
g as group, g.group_name as group_id
"""
    result = tx.run(q)
    _map_groups_to_datasets(result, dataset_dict)
    return QueryResult(query=q, result=None)


def _map_groups_to_datasets(result, dataset_dict):
    df = pd.DataFrame(result.data())
    dg = df.groupby('dataset_id').apply(_categories_to_list)
    dg = dg.drop(columns=['group_id'])
    groups_for_dataset_dict = dg.to_dict()
    for dataset_identifier in dataset_dict.keys():
        dataset_dict[dataset_identifier]['groups'] = groups_for_dataset_dict.get(dataset_identifier, [])


def _categories_to_list(df):
    df['group'] = df['group'].apply(query_builder.map_group_to_api)
    return df['group'].to_list()


@log_query
def get_resources_for_datasets(tx, dataset_ids_on_page, dataset_dict):
    """get distributions for datasets on page"""
    q = f"MATCH (f:Format)<-[:HAS_FORMAT]-(r:Distribution)<-[:HAS_DISTRIBUTION]-(d:Dataset) "
    q += _dataset_filter_clause(dataset_ids_on_page)
    q += f"""
RETURN d.dataset_identifier as dataset_id, r.distribution_id as resource_id, f.format as format,
r as resource
"""
    result = tx.run(q)
    _map_resources_to_datasets(result, dataset_dict)
    return QueryResult(query=q, result=None)


def _map_resources_to_datasets(result, dataset_dict):
    df = pd.DataFrame(result.data())
    dg = df.groupby('dataset_id').apply(_resources_to_list)
    resources_for_dataset_dict = dg.to_dict()
    for dataset_identifier in dataset_dict.keys():
        dataset_dict[dataset_identifier]['resources'] = resources_for_dataset_dict.get(dataset_identifier, [])


def _resources_to_list(df):
    df['resource'] = df.apply(lambda x:_add_format_to_resource(resource=x['resource'], format=x['format']), axis=1)
    return df['resource'].to_list()


def _add_format_to_resource(resource, format):
    resource['format'] = format
    return resource


@log_query
def get_keywords_for_datasets(tx, dataset_ids_on_page, dataset_dict, language):
    """get keywords in request language for datasets on page"""
    keyword_property = f"keyword_{language}"
    keyword_label = f"Keyword{language.capitalize()}"
    relationship = f":HAS_KEYWORD_{language.upper()}"
    q = f"MATCH (k:{keyword_label})<-[{relationship}]-(d:Dataset) "
    q += _dataset_filter_clause(dataset_ids_on_page)
    q += f"""
RETURN d.dataset_identifier as dataset_id, d as dataset,
k.{keyword_property} as keyword
"""
    result = tx.run(q)
    _map_keywords_to_datasets(result, dataset_dict, language)
    return QueryResult(query=q, result=None)


def _map_keywords_to_datasets(result, dataset_dict, language):
    df = pd.DataFrame(result.data())
    if df.empty:
        return
    dg = df.groupby('dataset_id').apply(_keywords_to_list)
    keywords_for_dataset_dict = dg.to_dict()
    for dataset_identifier in dataset_dict.keys():
        dataset_dict[dataset_identifier]['keywords'] = {
            language: keywords_for_dataset_dict.get(dataset_identifier, [])
        }


def _keywords_to_list(df):
    return df['keyword'].to_list()
