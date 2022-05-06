languages = ['de', 'fr', 'en', 'it']


def get_keyword_facet_key(facet_keys):
    keyword_facets = [tuple(facet_key.split('_')) for facet_key in facet_keys if facet_key.startswith('keywords')]
    if keyword_facets:
        return keyword_facets[0]


def get_request_language(facet_keys):
    return get_keyword_facet_key(facet_keys)[1]
