import json

def get_showcases(tx):
    result = tx.run("MATCH (s:Showcase) "
                    "RETURN s.showcase_name as name, s.title as title, "
                    "s.url as url,"
                    "s.notes as notes,"
                    "s.showcase_notes_formatted as showcase_notes_formatted,"
                    "s.image_display_url as image_display_url,"
                    "s.image_url as image_url ")
    return [{
        'name': record['name'],
        'url': record['url'],
        'title': record['title'],
        'showcase_notes_formatted': record['showcase_notes_formatted'],
        'notes': record['notes'],
        'image_display_url': record['image_display_url'],
    } for record in result]


def get_datasets_per_showcases_count(tx):
    result = tx.run("MATCH (s:Showcase) -[r:USES_DATASET]-> (d:Dataset) "
                    "RETURN s.showcase_name as name, count(d) as count ")
    return {record['name']: record['count'] for record in result}


def get_applications(tx):
    result = tx.run("MATCH (a:Applicationtype)<-[r:HAS_APPLICATION_TYPE]-(s:Showcase) "
                    "RETURN a.application_type_name as application, count(a) as count ")
    return { record["application"]: record['count'] for record in result }


def get_groups(tx):
    result = tx.run("MATCH (g:Group)<-[r:HAS_GROUP]-(s:Showcase) "
                    "RETURN g.group_name as group, count(g) as count ")
    return { record["group"]: record['count'] for record in result }


def get_tags(tx):
    result = tx.run("MATCH (t:Tag)<-[r:HAS_TAG]-(s:Showcase) "
                    "RETURN t.tag_name as tag, count(t) as count ")
    return { record["tag"]: record['count'] for record in result }


def get_groups_detail(tx):
    result = tx.run("MATCH (g:Group)<-[r:HAS_GROUP]-(s:Showcase) "
                    "RETURN g.group_name as name, count(g) as count, "
                    "g.title_fr as title_fr ,"
                    "g.title_de as title_de ,"
                    "g.title_en as title_en ,"
                    "g.title_it as title_it ")
    return [ {
        'count': record['count'],
        'name': record['name'],
        'display_name': json.dumps({
           'fr': record['title_fr'],
           'de': record['title_fr'],
           'en': record['title_en'],
           'it': record['title_it'],
        })
    } for record in result ]


def get_applications_detail(tx):
    result = tx.run("MATCH (a:Applicationtype)<-[r:HAS_APPLICATION_TYPE]-(s:Showcase) "
                    "RETURN a.application_type_name as name, count(a) as count ")
    return [ {
        'count': record['count'],
        'name': record['name'],
        'display_name': record['name'],
    } for record in result ]


def get_tags_detail(tx):
    result = tx.run("MATCH (t:Tag)<-[r:HAS_TAG]-(s:Showcase) "
                    "RETURN t.tag_name as name, count(t) as count ")
    return [ {
        'count': record['count'],
        'name': record['name'],
        'display_name': record['name'],
    } for record in result ]
