from flask import Flask

app = Flask(__name__)

from app import routes, counts, groups, organizations, showcase_search

app.run(debug=True)
