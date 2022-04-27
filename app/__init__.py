from flask import Flask

app = Flask(__name__)

from app import routes, counts, groups, organizations, showcases

app.run(debug=True)
