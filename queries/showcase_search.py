from utils import query_helpers as helpers

def search(tx, args):
    facets = []
    facets.extend(helpers.prepare_facets(
        value_list=args.getlist('groups'),
        q_id ='g',
        label = 'Group',
        relationship = 'HAS_GROUP',
        property = 'group_name',
    ))
    facets.extend(helpers.prepare_facets(
        value_list=args.getlist('tags'),
        q_id ='t',
        label = 'Tag',
        relationship = 'HAS_TAG',
        property='tag_name',
    ))
    facets.extend(helpers.prepare_facets(
        value_list=args.getlist('showcase_type'),
        q_id ='st',
        label = 'Applicationtype',
        relationship = 'HAS_APPLICATION_TYPE',
        property='application_type_name',
    ))
    q = helpers.get_facet_match_clause('Showcase', 's', facets)
    where_clause = helpers.get_facet_where_clause(facets)
    if where_clause:
        q += where_clause
    return_clause = "RETURN s.showcase_name as id"
    q += return_clause
    result = tx.run(q)
    return result.value('id')


def get_showcases(tx, showcase_ids):
    match_clause = helpers.get_ids_match_clause(
        match_clause="MATCH (s:Showcase) ",
        where_property='showcase_name',
        match_id='s',
        match_label='Showcase',
        ids=showcase_ids)
    return_clause = "RETURN s"
    q = match_clause + return_clause
    result = tx.run(q)
    showcases = []
    for record in result:
        showcase_dict = {}
        for k, v in record['s'].items():
            showcase_dict[k] = v
        showcases.append(showcase_dict)
    return showcases


def get_datasets_per_showcases_count(tx, showcase_ids):
    match_clause = helpers.get_ids_match_clause(
        match_clause = "MATCH (s:Showcase) -[r:USES_DATASET]-> (d:Dataset) ",
        where_property='showcase_name',
        match_id='s',
        match_label='Showcase',
        ids=showcase_ids)
    return_clause = "RETURN s.showcase_name as name, count(d) as count "
    q = match_clause + return_clause
    result = tx.run(q)
    return {record['name']: record['count'] for record in result}
