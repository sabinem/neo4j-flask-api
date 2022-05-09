import pandas as pd


def get_dataset_count_for_group(tx, group):
    result = tx.run("MATCH (Group {group_name: $group})<-[:HAS_THEME]-(d:Dataset) "
                    "RETURN count(d) ", group=group)
    count = result.single()[0]
    return count


def get_groups(tx):
    result = tx.run("MATCH (g: Group) "
                    "RETURN g.group_name as name ")
    return result.value('name', 'title_de')


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

