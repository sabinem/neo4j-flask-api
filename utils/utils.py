languages = ['de', 'fr', 'en', 'it']


def get_keyword_facet_key(facet_keys):
    keyword_facets = [tuple(facet_key.split('_')) for facet_key in facet_keys if facet_key.startswith('keywords')]
    if keyword_facets:
        return keyword_facets[0]


def get_request_language(facet_keys):
    return get_keyword_facet_key(facet_keys)[1]


def get_search_facets(facet_count, facet_item, facet_key):
    search_facets = []
    for label, count in facet_count.items():
        item = facet_item[label]
        item['count'] = count
        print(item)
        search_facets.append(item)
    return search_facets
