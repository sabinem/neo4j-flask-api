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
