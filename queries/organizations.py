def get_organizations(tx):
    result = tx.run("MATCH (o:Organization) "
                    "RETURN o.organization_name as name, o.title_de as title_de, "
                    "o.title_fr as title_fr, o.title_en as title_en, "
                    "o.title_it as title_it")
    return result.values('name', 'title_de', 'title_fr', 'title_en', 'title_it')


def get_sub_organizations(tx, organization):
    result = tx.run("MATCH (o:Organization)-[:HAS_PARENT]->(p:Organization {organization_name: $name}) "
                    "RETURN o.organization_name as organization_name ", name=organization)
    return result.values('organization_name')


def get_parent(tx, organization):
    result = tx.run("MATCH (o:Organization {organization_name: $name})-[:HAS_PARENT]->(p:Organization) "
                    "RETURN count(p) ", name=organization)
    count = result.single()[0]
    return count
