from flask import Flask, g, Response, request
from neo4j import GraphDatabase
from dotenv import dotenv_values, load_dotenv

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
