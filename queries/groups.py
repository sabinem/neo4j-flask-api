import pandas as pd
from utils import map_neo4j_to_api


def get_categories_with_titles_and_counts(tx):
    q = """
MATCH (g:Group) <-[:HAS_THEME]-(d:Dataset) 
RETURN count(d) as count, g, g.group_name as name
"""
    print(q)
    result = tx.run(q)
    return _map_count_result(result)


def _map_count_result(result):
    df = pd.DataFrame.from_dict(result.data())
    df['group'] = df.apply(lambda x:_transform_group(g=x['g'], count=x['count']), axis=1)
    df = df.drop(columns=['g', 'count'])
    df.set_index('name', inplace=True)
    ds = df.squeeze()
    return ds.to_list()


def _transform_group(g, count):
    group = map_neo4j_to_api.map_group(g)
    group['package_count'] = count
    return group
