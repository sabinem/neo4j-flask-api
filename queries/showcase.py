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
