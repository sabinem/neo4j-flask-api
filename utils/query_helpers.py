def lists_to_query_parameters(q_list, identifier):
    return [{(identifier) + str(q_list.index(item)): item} for item in q_list]
