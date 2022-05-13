import json
import pandas as pd
from utils import query_builder
from utils.decorators import log_query
from utils.query_builder import QueryResult


@log_query
def showcase_facet_search(tx, facet_dict):
    """perform facet search on showcases to filter by facets"""
    search_facets = []
    facet_keys = facet_dict.keys()
    if 'groups' in facet_keys:
        facets = query_builder.prepare_facets(
            query="(s:Showcase)-[:HAS_GROUP]->({}:Group)",
            values=facet_dict['groups'],
            ids=['g'],
            property='group_name',
        )
        search_facets.extend(facets)
    if 'tags' in facet_keys:
        facets = query_builder.prepare_facets(
            query="(s:Showcase)-[:HAS_TAG]->({}:Tag)",
            values=facet_dict['tags'],
            ids=['t'],
            property='tag_name'
        )
        search_facets.extend(facets)
    if 'showcase_type' in facet_keys:
        facets = query_builder.prepare_facets(
            query="(s:Showcase)-[:HAS_APPLICATION_TYPE]->({}:Applicationtype)",
            values=facet_dict['showcase_type'],
            ids=['st'],
            property='application_type_name',
        )
        search_facets.extend(facets)
    q = query_builder.get_facet_match_clause(search_facets, "s", "Showcase")
    where_clause = query_builder.get_facet_where_clause(search_facets)
    if where_clause:
        q += where_clause
    q += "RETURN s.showcase_name as id, count(s) as count"
    result = tx.run(q)
    return QueryResult(query=q, result=query_builder.map_search_result(result, filtered_search=bool(search_facets)))


@log_query
def get_query_search(tx, term, filter_by_showcase_ids, filtered_search):
    """perform fulltext search on showcases with a search term"""
    q = f"""
CALL db.index.fulltext.queryNodes('showcase_de', '"{term}"') 
YIELD node as s, score
"""
    q += _showcase_filter_clause(filter_by_showcase_ids, filtered_search)
    q += "RETURN s.showcase_name as id, count(s) as count"
    result = tx.run(q)
    return QueryResult(query=q, result=query_builder.map_search_result(result))


@log_query
def get_showcases(tx, filter_by_showcase_ids, filtered_search, limit, skip):
    """get showcase items for a page"""
    q = """
MATCH (s:Showcase)
OPTIONAL MATCH (s:Showcase)-[:USES_DATASET]->(d:Dataset) 
"""
    q += _showcase_filter_clause(filter_by_showcase_ids, filtered_search)
    q += "RETURN s as showcase, count(d) as count_d "
    q += f"ORDER BY s.showcase_name Skip {skip} LIMIT {limit}"
    result = tx.run(q)
    return QueryResult(query=q, result=_map_showcase_result(result))


def _map_showcase_result(result):
    df = pd.DataFrame(result.data())
    df['showcase'] = df.apply(lambda x:_map_showcase(showcase=x['showcase'], count_d=x['count_d']), axis=1)
    showcases = df['showcase'].to_list()
    return showcases


def _map_showcase(showcase, count_d):
    showcase_dict = showcase
    showcase_dict['name'] = showcase['showcase_name']
    showcase_dict['num_datasets'] = count_d
    return showcase_dict


@log_query
def get_datasets_per_showcases_count(tx, filter_by_showcase_ids, filtered_search):
    """get showcase dataset counts for the showcases on the page"""
    q = "MATCH (s:Showcase) -[r:USES_DATASET]-> (d:Dataset) "
    q += _showcase_filter_clause(filter_by_showcase_ids, filtered_search)
    q += "RETURN s.showcase_name as name, count(d) as count "
    result = tx.run(q)
    return QueryResult(query=q, result={record['name']: record['count'] for record in result})


@log_query
def get_groups_facets(tx,filter_by_showcase_ids, filtered_search):
    """get group counts for the showcases"""
    q = "MATCH (g:Group)<-[:HAS_GROUP]-(s:Showcase) "
    q += _showcase_filter_clause(filter_by_showcase_ids, filtered_search)
    q += "RETURN g, count(g) as count_g"
    result = tx.run(q)
    return QueryResult(query=q, result=_format_groups_facet_result(result))


@log_query
def get_showcase_type_facets(tx, filter_by_showcase_ids, filtered_search):
    """get showcase types for the showcases"""
    q = "MATCH (a:Applicationtype)<-[r:HAS_APPLICATION_TYPE]-(s:Showcase) "
    q += _showcase_filter_clause(filter_by_showcase_ids, filtered_search)
    q += "RETURN a, count(a) as count_a"
    result = tx.run(q)
    return QueryResult(query=q, result=_format_facet_result(result, 'a', 'count_a', 'application_type_name'))


@log_query
def get_tags_facets(tx, filter_by_showcase_ids, filtered_search):
    """get tags for the showcases"""
    q = "MATCH (t:Tag)<-[r:HAS_TAG]-(s:Showcase) "
    q += _showcase_filter_clause(filter_by_showcase_ids, filtered_search)
    q += "RETURN t, count(t) as count_t"
    result = tx.run(q)
    return QueryResult(query=q, result=_format_facet_result(result, 't', 'count_t', 'tag_name'))


def _showcase_filter_clause(filter_by_showcase_ids, filtered_search):
    if not filtered_search:
        return ""
    return query_builder.get_filter_by_ids_where_clause(
        condition='s.showcase_name',
        filter_by_ids=filter_by_showcase_ids
    )


def _format_facet_result(result, label, count, id_property):
    search_facets = []
    for record in result:
        facet_dict = {}
        facet_dict['count'] = record[count]
        for k, v in record[label].items():
            if k == id_property:
                facet_dict['name'] = v
                facet_dict['display_name'] = v
        search_facets.append(facet_dict)
    return (search_facets, _get_facets_from_search_facets(search_facets))


def _format_groups_facet_result(result):
    search_facets = []
    for record in result:
        facet_dict = {}
        title_dict = {}
        facet_dict['count'] = record['count_g']
        for k, v in record['g'].items():
            if k == 'group_name':
                facet_dict['name'] = v
            if k.startswith('title_'):
                title_dict[k.replace('title_', '')] = v
        facet_dict['display_name'] = json.dumps(title_dict)
        search_facets.append(facet_dict)
    facets = {item['name']: item['count']  for item in search_facets}
    return (search_facets, _get_facets_from_search_facets(search_facets))


def _get_facets_from_search_facets(search_facets):
    return {item['name']: item['count'] for item in search_facets}
