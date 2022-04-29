# neo4j-flask-api

## install 

```commandline
pipenv install

cp .env.example .env
add the noe4j credentials
```

## use

```commandline
pipenv shell
export FLASK_APP=app.py
export FLASK_ENV=development
flask run
```

### full text search

https://www.cnblogs.com/jpfss/p/11394385.html

customizing analyzers

https://graphaware.com/neo4j/2019/09/06/custom-fulltext-analyzer.html

on github

https://github.com/graphaware/fulltext-analyzer-neo4j-4

https://community.neo4j.com/t/how-to-add-custom-lucene-analyzer/3923/2
