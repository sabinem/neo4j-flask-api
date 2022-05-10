import json
import pandas as pd
from utils import showcase_results


def get_showcase(tx, id):
    q = f"""
MATCH (a:Applicationtype)<-[:HAS_APPLICATION_TYPE]-(s:Showcase)-[:HAS_GROUP]->(g:Group),
(s:Showcase)-[]->(t:Tag), (s:Showcase)-[:USES_DATASET]-(d:Dataset)
WHERE s.showcase_name='{id}'
RETURN s as showcase, a.application_type_name as showcase_type, g as group, g.group_name as group_id,
t.tag_name as tag, count(d) as num_datasets, d as dataset, s.showcase_name as showcase_id, 
d.dataset_identifier as dataset_id
"""
    print(q)
    result = tx.run(q)
    return _map_showcase_result(result)


def _map_showcase_result(result):
    df = pd.DataFrame.from_dict(result.data())
    showcase_type = df['showcase_type'].unique()[0]
    tags = _map_tag_to_api(df['tag'].unique())
    num_datasets = df['num_datasets'].unique()[0]
    df_group = df[['group_id', 'group']].drop_duplicates("group_id").drop(columns=['group_id'])
    df_group['group'] = df_group['group'].apply(_map_group_to_api)
    groups = df_group['group'].to_list()
    df = df.drop(columns=['showcase_type', 'tag', 'num_datasets', 'group', 'group_id'])
    df_showcase = df[['showcase_id', 'showcase']].drop_duplicates("showcase_id")
    df_showcase['showcase'] = df_showcase['showcase'].apply(_map_showcase_to_api)
    showcase = df['showcase'][0]
    df_datasets = df[['dataset_id', 'dataset']].drop_duplicates("dataset_id")
    df_datasets['dataset'] = df_datasets['dataset'].apply(_map_dataset_to_api)
    df_datasets = df_datasets.drop(columns=['dataset_id'])
    datasets = df_datasets['dataset'].to_list()
    return _map_to_api(showcase_type, tags, groups, showcase, datasets, num_datasets)


def _map_group_to_api(group):
    group_dict = {}
    title_dict = {}
    for k, v in group.items():
        if k.startswith('title_'):
            title_dict[k.replace('title_', '')] = v
        elif k == 'group_name':
            group_dict['name'] = v
    title = json.dumps(title_dict)
    group_dict['display_name'] = title
    group_dict['title'] = title
    return group_dict


def _map_tag_to_api(tags):
    return [{'name': tag,
              'display_name': tag}
            for tag in tags]


def _map_showcase_to_api(showcase):
    showcase_dict = {}
    for k, v in showcase.items():
        showcase_dict[k] = v
        if k == 'showcase_name':
            showcase_dict['name'] = v
    return showcase_dict


def _map_dataset_to_api(dataset):
    dataset_dict = {'title': {}, 'description': {} }
    for k, v in dataset.items():
        if k.startswith('title_'):
            dataset_dict['title'][k.replace('title_', '')] = v
        elif k.startswith('description_'):
            dataset_dict['description'][k.replace('description_', '')] = v
        else:
            dataset_dict[k] = v
    return dataset_dict


def _map_to_api(showcase_type, tags, groups, showcase, datasets, num_datasets):
    showcase_dict = showcase
    showcase_dict['tags'] = tags
    showcase_dict['groups'] = groups
    showcase_dict['showcase_type'] = showcase_type
    showcase_dict['num_datasets'] = str(num_datasets)
    print(showcase_dict)
    return {
        'showcase': showcase_dict,
        'datasets': datasets,
    }
