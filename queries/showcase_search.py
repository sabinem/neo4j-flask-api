def search(tx, args):
    groups = args.getlist('groups')
    tags = args.getlist('tags')
    showcase_types = args.getlist('showcase_type')
    print(groups)
    print(tags)
    print(showcase_types)
    q = """MATCH (s:Showcase)-[:HAS_GROUP]->(g1:Group), (s:Showcase)-[:HAS_GROUP]->(g2:Group), 
    (s:Showcase) - [: HAS_GROUP]->(g3:Group)
    WHERE g1.group_name = 'work' AND g2.group_name = 'education'
    RETURN s as showcase, count(g3) as count, g3.group_name as group
    """
    result = tx.run(q)
    return [record for record in result]