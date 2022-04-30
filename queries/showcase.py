from utils import result_helpers as r_helpers


def get_showcase(tx, id):
    q = f"MATCH (a:Applicationtype)<-[:HAS_APPLICATION_TYPE]-(s:Showcase)-[:HAS_GROUP]->(g:Group), " \
        "(s:Showcase)-[]->(t:Tag) " \
        f"WHERE s.showcase_name='{id}' " \
        f"RETURN s, a, g, t"
    result = tx.run(q)
    result_grouped = r_helpers.aggregate_per_result_key(result)
    showcase_dict = r_helpers.map_showcase(result_grouped['s'][0])
    r_helpers.map_showcase_type(showcase_dict, result_grouped['a'])
    r_helpers.map_showcase_groups(showcase_dict, result_grouped['g'])
    r_helpers.map_showcase_tags(showcase_dict, result_grouped['t'])
    return showcase_dict


def get_datasets_per_showcases(tx, id):
    match_clause = f"MATCH (s:Showcase) -[r:USES_DATASET]-> (d:Dataset) WHERE s.showcase_name='{id}' "
    return_clause = "RETURN d"
    q = match_clause + return_clause
    result = tx.run(q)
    datasets = []
    for record in result:
        dataset_dict = {}
        dataset_dict['title'] = {}
        dataset_dict['description'] = {}
        for k, v in record["d"].items():
            if k.startswith('title_'):
                dataset_dict['title'][k.replace('title_', '')] = v
            elif k.startswith('description_'):
                dataset_dict['description'][k.replace('description_', '')] = v
            else:
                dataset_dict[k] = v
        datasets.append(dataset_dict)
    return datasets
