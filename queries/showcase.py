def get_showcase(tx, id):
    q = f"MATCH (s:Showcase) WHERE s.showcase_name='{id}' RETURN s"
    result = tx.run(q)
    record = result.single()
    showcase_dict = {}
    for k,v in record["s"].items():
        if k == 'showcase_name':
            showcase_dict['name'] = v
        showcase_dict[k] = v
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
