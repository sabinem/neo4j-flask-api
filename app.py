import os
import json
from flask import Flask, g, Response, request
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)


url = os.getenv('NEO4J_URI')
username = os.getenv('NEO4J_USERNAME')
password = os.getenv('NEO4J_PASSWORD')
database = os.getenv('NEO4J_DATABASE')
driver = GraphDatabase.driver(url, auth=(username, password))
driver.verify_connectivity()


def get_db():
    if not hasattr(g, "neo4j_db"):
        g.neo4j_db = driver.session()
    return g.neo4j_db


@app.route('/')
def hello():
    return f'Hello, World!'


@app.route("/counts")
def get_counts():
    def get_dataset_count(tx):
        result = tx.run("MATCH (d:Dataset) "
                        "RETURN count(d)")
        count = result.single()[0]
        return count
    def get_dataset_count_for_group(tx, group):
        result = tx.run("MATCH (Group {name: $group})<-[:hasTheme]-(d:Dataset) "
                        "RETURN count(d) ", group=group)
        count = result.single()[0]
        return count
    def get_groups(tx):
        result = tx.run("MATCH (g: Group) "
                        "RETURN g.name as name ")
        return result.value('name')

    db = get_db()
    groups = db.read_transaction(get_groups)
    group_counts = {}
    for group in groups:
        group_count = db.read_transaction(get_dataset_count_for_group, request.args.get("group", group))
        group_counts[group] = group_count
    dataset_count = db.read_transaction(get_dataset_count)
    return Response(json.dumps(
        {
            "help": "ckan counts",
            "success": True,
            "result": {
                'total_dataset_count': dataset_count,
                'groups': group_counts,
            }
        }), mimetype="application/json")
