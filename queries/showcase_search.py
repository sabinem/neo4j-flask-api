from utils import query_builder
from utils import showcase_results


def showcase_search(tx, facet_dict, query_term):
    facets = []
    facets.extend(query_builder.prepare_facets(
        value_list=facet_dict.get('groups', []),
        q_id ='g',
        label = 'Group',
        relationship = 'HAS_GROUP',
        property = 'group_name',
    ))
    facets.extend(query_builder.prepare_facets(
        value_list=facet_dict.get('tags', []),
        q_id ='t',
        label = 'Tag',
        relationship = 'HAS_TAG',
        property='tag_name',
    ))
    facets.extend(query_builder.prepare_facets(
        value_list=facet_dict.get('showcase_type', []),
        q_id ='st',
        label = 'Applicationtype',
        relationship = 'HAS_APPLICATION_TYPE',
        property='application_type_name',
    ))
    q = query_builder.get_facet_match_clause('Showcase', 's', facets)
    where_clause = query_builder.get_facet_where_clause(facets)
    if where_clause:
        q += where_clause
    return_clause = "RETURN s.showcase_name as id"
    q += return_clause
    result = tx.run(q)
    return result.value('id')


def get_query_search(tx, term, showcase_ids):
    q = f"""CALL db.index.fulltext.queryNodes('showcase_de', '"{term}"') YIELD node, score"""
    where_clause = query_builder.get_ids_match_clause(
        match_clause="",
        where_property='showcase_name',
        match_node='node',
        ids=showcase_ids
    )
    return_clause = "RETURN node.showcase_name as id, score as score"
    q += where_clause + return_clause
    result = tx.run(q)
    return result.value('id')


def get_showcases(tx, showcase_ids, limit, skip):
    match_clause = query_builder.get_ids_match_clause(
        match_clause="MATCH (s:Showcase) ",
        where_property='showcase_name',
        match_node='s',
        ids=showcase_ids)
    return_clause = "RETURN s "
    pagination_clause = f"ORDER BY s.showcase_name Skip {skip} LIMIT {limit}"
    q = match_clause + return_clause + pagination_clause
    result = tx.run(q)
    showcases = []
    for record in result:
        showcase_dict = {}
        for k, v in record['s'].items():
            if k == 'showcase_name':
                showcase_dict['name'] = v
            showcase_dict[k] = v
        showcases.append(showcase_dict)
    return showcases


def get_datasets_per_showcases_count(tx, showcase_ids):
    match_clause = query_builder.get_ids_match_clause(
        match_clause = "MATCH (s:Showcase) -[r:USES_DATASET]-> (d:Dataset) ",
        where_property='showcase_name',
        match_node='s',
        ids=showcase_ids)
    return_clause = "RETURN s.showcase_name as name, count(d) as count "
    q = match_clause + return_clause
    result = tx.run(q)
    return {record['name']: record['count'] for record in result}


def get_groups_facets(tx,showcase_ids):
    match_clause = query_builder.get_ids_match_clause(
        match_clause="MATCH (g:Group)<-[:HAS_GROUP]-(s:Showcase) ",
        where_property='showcase_name',
        match_node='s',
        ids=showcase_ids)
    return_clause = "RETURN g, count(g) as count_g"
    q = match_clause + return_clause
    result = tx.run(q)
    return showcase_results.format_groups_facet_result(result)


def get_showcase_type_facets(tx, showcase_ids):
    match_clause = query_builder.get_ids_match_clause(
        match_clause="MATCH (a:Applicationtype)<-[r:HAS_APPLICATION_TYPE]-(s:Showcase) ",
        where_property='showcase_name',
        match_node='s',
        ids=showcase_ids)
    return_clause = "RETURN a, count(a) as count_a"
    q = match_clause + return_clause
    result = tx.run(q)
    return showcase_results.format_facet_result(result, 'a', 'count_a', 'application_type_name')


def get_tags_facets(tx, showcase_ids):
    match_clause = query_builder.get_ids_match_clause(
        match_clause="MATCH (t:Tag)<-[r:HAS_TAG]-(s:Showcase) ",
        where_property='showcase_name',
        match_node='s',
        ids=showcase_ids)
    return_clause = "RETURN t, count(t) as count_t"
    q = match_clause + return_clause
    result = tx.run(q)
    return showcase_results.format_facet_result(result, 't', 'count_t', 'tag_name')
