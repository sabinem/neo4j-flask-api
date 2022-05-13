import pandas as pd
from utils import query_builder
from utils.decorators import log_query
from utils.query_builder import QueryResult


@log_query
def get_organization_list(tx):
    """get organization tree"""
    q = """
MATCH(o: Organization) 
OPTIONAL MATCH(o: Organization)-[: HAS_PARENT]->(po:Organization)
RETURN po.organization_name as po_id, o.organization_name as o_id, o as organization
"""
    result = tx.run(q)
    return _map_organization_list_result(result)


def _map_organization_list_result(result):
    df = pd.DataFrame.from_dict(result.data())
    df_org = df.copy()
    df_org = df_org.drop(columns=['po_id']).set_index('o_id')
    df_org['organization'] = df_org['organization'].apply(query_builder.map_organization_to_api)
    ds_org = df_org.squeeze()
    organization_dict = ds_org.to_dict()
    df_tree = df[['po_id', 'o_id']]
    df_tree['po_id'] = df_tree.apply(lambda x:_transform_po_id(po_id=x['po_id'], o_id=x['o_id']), axis=1)
    df_tree['o_id'] = df_tree.apply(lambda x:_transform_o_id(po_id=x['po_id'], o_id=x['o_id']), axis=1)
    df_grouped = df_tree.groupby('po_id').apply(_list_sub_organizations)
    organization_tree = df_grouped.to_dict()
    return _map_organizations_to_tree_to_api(organization_tree, organization_dict)


def _list_sub_organizations(df, col='o_id'):
    filtered_df = df[df['o_id'].notnull()]
    if not filtered_df.empty:
        return filtered_df[col].to_list()
    return []


def _transform_po_id(po_id, o_id):
    if po_id:
        return po_id
    return o_id


def _transform_o_id(po_id, o_id):
    if po_id == o_id:
        return None
    return o_id


def _map_organizations_to_tree_to_api(organization_tree, organization_dict):
    organizations = []
    for org, suborgs in organization_tree.items():
        item = organization_dict.get(org)
        item['children'] = [
            organization_dict.get(suborg) for suborg in suborgs
        ]
        organizations.append(item)
    return organizations
