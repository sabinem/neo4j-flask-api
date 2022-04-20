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
    return result.value('name', 'title_de')


def get_showcase_count(tx):
    result = tx.run("MATCH (s: Showcase) "
                    "RETURN count(s) ")
    count = result.single()[0]
    return count


def get_organization_count(tx):
    result = tx.run("MATCH (o: Organization) "
                    "RETURN count(o) ")
    count = result.single()[0]
    return count