import os
import json
from flask import Flask, g, Response, request
from neo4j import GraphDatabase
from dotenv import load_dotenv

from dotenv import dotenv_values, load_dotenv

app = Flask(__name__)

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
    def get_dataset_count(tx):
        result = tx.run("MATCH (d:Dataset) "
                        "RETURN count(d)")
        count = result.single()[0]
        return count
    def get_dataset_count_for_group(tx, group):
        result = tx.run("MATCH (Group {group_name: $group})<-[:HAS_THEME]-(d:Dataset) "
                        "RETURN count(d) ", group=group)
        count = result.single()[0]
        return count
    def get_groups(tx):
        result = tx.run("MATCH (g: Group) "
                        "RETURN g.group_name as name ")
        return result.value('name', 'title_de')
    def get_showcase_count(tx):
        result = tx.run("MATCH (s: Showcase) "
                        "RETURN count(s) ")
        count = result.single()[0]
        return count
    def get_organization_count(tx):
        result = tx.run("MATCH (o: Organization) "
                        "RETURN count(o) ")
        count = result.single()[0]
        return count

    db = get_db()
    groups = db.read_transaction(get_groups)
    group_counts = {}
    for group in groups:
        group_count = db.read_transaction(get_dataset_count_for_group, request.args.get("group", group))
        group_counts[group] = group_count
    dataset_count = db.read_transaction(get_dataset_count)
    showcase_count = db.read_transaction(get_showcase_count)
    organization_count = db.read_transaction(get_organization_count)
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
    def get_categories(tx):
        result = tx.run("MATCH (g:Group) "
                        "RETURN g.group_name as name, g.title_de as title_de, "
                        "g.title_fr as title_fr, g.title_en as title_en, "
                        "g.title_it as title_it")
        return result.values('name', 'title_de', 'title_fr', 'title_en', 'title_it')
    def get_dataset_count(tx, group):
        result = tx.run("MATCH (Group {group_name: $group})<-[:HAS_THEME]-(d:Dataset) "
                        "RETURN count(d) ", group=group)
        count = result.single()[0]
        return count
    db = get_db()
    categories = db.read_transaction(get_categories)
    list_groups = []
    for category in categories:
        name = category[0]
        package_count = db.read_transaction(get_dataset_count, request.args.get("group", name))
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
