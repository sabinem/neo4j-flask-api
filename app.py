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


@app.route('/')
def hello():
    return f'Hello to the neo4j api'


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
        return result.value('name')
    def get_showcase_count(tx):
        result = tx.run("MATCH (s: Showcase) "
                        "RETURN count(s) ")
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
    return Response(json.dumps(
        {
            "help": "ckan counts",
            "success": True,
            "result": {
                'total_dataset_count': dataset_count,
                'showcase_count': showcase_count,
                'groups': group_counts,
            }
        }), mimetype="application/json")
