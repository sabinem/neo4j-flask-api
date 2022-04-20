from app import app

import os
import json
from flask import Flask, g, Response, request
from neo4j import GraphDatabase
from dotenv import load_dotenv
from queries import counts as q_counts
from queries import groups as q_groups
from queries import organizations as q_organizations

from dotenv import dotenv_values, load_dotenv

from app import app

load_dotenv()
config = dotenv_values(".env")
url = config.get('NEO4J_URI')
username =config.get('NEO4J_USERNAME')
password = config.get('NEO4J_PASSWORD')
database = config.get('NEO4J_DATABASE')
driver = GraphDatabase.driver(url, auth=(username, password))
driver.verify_connectivity()

def get_db():
    if not hasattr(g, "neo4j_db"):
        g.neo4j_db = driver.session()
    return g.neo4j_db


@app.route("/counts")
def get_counts():
    db = get_db()
    groups = db.read_transaction(q_counts.get_groups)
    group_counts = {}
    for group in groups:
        group_count = db.read_transaction(q_counts.get_dataset_count_for_group, request.args.get("group", group))
        group_counts[group] = group_count
    dataset_count = db.read_transaction(q_counts.get_dataset_count)
    showcase_count = db.read_transaction(q_counts.get_showcase_count)
    organization_count = db.read_transaction(q_counts.get_organization_count)
    return Response(json.dumps(
        {
            "help": "ckan counts",
            "success": True,
            "result": {
                'total_dataset_count': dataset_count,
                'showcase_count': showcase_count,
                'groups': group_counts,
                'organization_count': organization_count,
            }
        }), mimetype="application/json")


@app.route("/groups")
def get_groups():
    db = get_db()
    categories = db.read_transaction(q_groups.get_categories)
    list_groups = []
    for category in categories:
        name = category[0]
        package_count = db.read_transaction(q_groups.get_dataset_count, request.args.get("group", name))
        list_groups.append(
            {
                'name': name,
                'title': {
                    'de': category[1],
                    'fr': category[2],
                    'en': category[3],
                    'it': category[4],
                },
                'package_count': package_count,
            }
        )
    return Response(json.dumps(
        {
            "help": "group_list",
            "success": True,
            "result": list_groups
        }), mimetype="application/json")


@app.route("/organizations")
def get_organizations():
    db = get_db()
    organizations = db.read_transaction(q_organizations.get_organizations)
    list_organizations = []
    dict_organizations = {
        organization[0]: {
            'name': organization[0],
            'title': {
                'de': organization[1],
                'fr': organization[2],
                'en': organization[3],
                'it': organization[4],
            },
        }
        for organization in organizations
    }
    for organization in organizations:
        name = organization[0]
        has_parent = db.read_transaction(q_organizations.get_parent, request.args.get("organization", name))
        if not has_parent:
            sub_organizations = db.read_transaction(q_organizations.get_sub_organizations, request.args.get("organization", name))
            parent_organization_dict = dict_organizations[name]
            children = []
            for sub_organization in sub_organizations:
                children.append(dict_organizations[sub_organization[0]])
            parent_organization_dict['children'] = children
            list_organizations.append(
                parent_organization_dict
            )
    return Response(json.dumps(
        {
            "help": "organization_list",
            "success": True,
            "result": list_organizations
        }), mimetype="application/json")
