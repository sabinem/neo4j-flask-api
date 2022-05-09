import pandas as pd


def get_group_counts(tx):
    q = """
MATCH (g:Group) <-[:HAS_THEME]-(d:Dataset) 
RETURN count(d) as count, g.group_name as label
"""
    print(q)
    result = tx.run(q)
    return _map_count_result(result)


def get_counts(tx):
    q = """
CALL db.labels() yield label 
CALL apoc.cypher.run('match (:`'+label+'`) return count(*) as count', null) yield value
return label, value.count as count
"""
    print(q)
    result = tx.run(q)
    return _map_count_result(result)


def _map_count_result(result):
    df = pd.DataFrame.from_dict(result.data())
    df.set_index('label', inplace=True)
    ds = df.squeeze()
    return ds.to_dict()
