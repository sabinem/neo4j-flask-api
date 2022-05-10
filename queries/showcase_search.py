import json
import pandas as pd
from utils import query_builder


def showcase_facet_search(tx, facet_dict):
    print(f"\n{facet_dict}\n")
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
    print(f"\nRetrieve showcases in facet search {facet_dict}\n")
    print(q)
    result = tx.run(q)
    return _map_search_result(result, return_ids=bool(search_facets))


def _map_search_result(result, return_ids=True):
    df = pd.DataFrame(result.data())
    print(df)
    count = str(df['count'].sum())
    if not return_ids:
        return count, None
    filter_by_ids = df['id'].to_list()
    return count, filter_by_ids


def get_query_search(tx, term, filter_by_showcase_ids):
    q = f"""
CALL db.index.fulltext.queryNodes('showcase_de', '"{term}"') 
YIELD node as showcase, score
"""
    q += _showcase_filter_clause(filter_by_showcase_ids)
    q += "RETURN showcase.showcase_name as id, count(showcase) as count"
    print(q)
    result = tx.run(q)
    return _map_search_result(result)


def get_showcases(tx, filter_by_showcase_ids, limit, skip):
    q = """
MATCH (s:Showcase)
OPTIONAL MATCH (s:Showcase)-[:USES_DATASET]->(d:Dataset) 
"""
    q += _showcase_filter_clause(filter_by_showcase_ids)
    q += "RETURN s as showcase, count(d) as count_d "
    q += f"ORDER BY s.showcase_name Skip {skip} LIMIT {limit}"
    print(q)
    result = tx.run(q)
    return _map_showcase_result(result)


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


def get_datasets_per_showcases_count(tx, filter_by_showcase_ids):
    q = "MATCH (s:Showcase) -[r:USES_DATASET]-> (d:Dataset) "
    q += _showcase_filter_clause(filter_by_showcase_ids)
    q += "RETURN s.showcase_name as name, count(d) as count "
    print(q)
    result = tx.run(q)
    return {record['name']: record['count'] for record in result}


def get_groups_facets(tx,filter_by_showcase_ids):
    q = "MATCH (g:Group)<-[:HAS_GROUP]-(s:Showcase) "
    q += _showcase_filter_clause(filter_by_showcase_ids)
    q += "RETURN g, count(g) as count_g"
    print(q)
    result = tx.run(q)
    return _format_groups_facet_result(result)


def get_showcase_type_facets(tx, filter_by_showcase_ids):
    q = "MATCH (a:Applicationtype)<-[r:HAS_APPLICATION_TYPE]-(s:Showcase) "
    q += _showcase_filter_clause(filter_by_showcase_ids)
    q += "RETURN a, count(a) as count_a"
    print(q)
    result = tx.run(q)
    return _format_facet_result(result, 'a', 'count_a', 'application_type_name')


def get_tags_facets(tx, filter_by_showcase_ids):
    q = "MATCH (t:Tag)<-[r:HAS_TAG]-(s:Showcase) "
    q += _showcase_filter_clause(filter_by_showcase_ids)
    q += "RETURN t, count(t) as count_t"
    print(q)
    result = tx.run(q)
    return _format_facet_result(result, 't', 'count_t', 'tag_name')


def _showcase_filter_clause(filter_by_showcase_ids):
    if not filter_by_showcase_ids:
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
