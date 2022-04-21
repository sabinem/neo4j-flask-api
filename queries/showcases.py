def get_showcases(tx):
    result = tx.run("MATCH (s:Showcase) "
                    "RETURN s.showcase_name as name, s.title as title, "
                    "s.url as url ")
    return result.values('name', 'title', 'url')


def get_applications(tx):
    result = tx.run("MATCH (a:Applicationtype)<-[r:HAS_APPLICATION_TYPE]-(s:Showcase) "
                    "RETURN a.application_type_name as application, count(a) as count ")
    return { record["application"]: record['count'] for record in result }


def get_groups(tx):
    result = tx.run("MATCH (g:Group)<-[r:HAS_GROUP]-(s:Showcase) "
                    "RETURN g.group_name as group, count(g) as count ")
    return { record["group"]: record['count'] for record in result }
