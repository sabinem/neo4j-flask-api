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

```commandline
SHOW INDEXES
DROP INDEX index_name
CALL db.index.fulltext.createNodeIndex('showcase_de', ['Showcase'], ['title_de', 'description_de'], {analyzer: "german"})
CALL db.index.fulltext.listAvailableAnalyzers
CALL db.index.fulltext.queryNodes('showcase_de', 'energy')
```

Links for further reading:

- https://www.cnblogs.com/jpfss/p/11394385.html
- https://graphaware.com/neo4j/2019/09/06/custom-fulltext-analyzer.html
- https://github.com/graphaware/fulltext-analyzer-neo4j-4
- https://community.neo4j.com/t/how-to-add-custom-lucene-analyzer/3923/2
