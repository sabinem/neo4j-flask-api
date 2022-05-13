import pandas as pd
from utils.decorators import log_query
from utils.query_builder import QueryResult


@log_query
def get_group_counts(tx):
    """get categories with counts of datasets per category"""
    q = """
MATCH (g:Group) <-[:HAS_THEME]-(d:Dataset) 
RETURN count(d) as count, g.group_name as label
"""
    result = tx.run(q)
    return QueryResult(query=q, result=_map_count_result(result))


@log_query
def get_counts(tx):
    """get overall label counts for displaying them on the homepage"""
    q = """
CALL db.labels() yield label 
CALL apoc.cypher.run('match (:`'+label+'`) return count(*) as count', null) yield value
return label, value.count as count
"""
    result = tx.run(q)
    return QueryResult(query=q, result=_map_count_result(result))


def _map_count_result(result):
    df = pd.DataFrame.from_dict(result.data())
    df.set_index('label', inplace=True)
    ds = df.squeeze()
    return ds.to_dict()
