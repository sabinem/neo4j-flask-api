from luqum.parser import parser
from luqum import tree as luqum_vocabulary


def analyze_fq(fq_lucene, facets):
    if not fq_lucene:
        return {}
    print(f"\n{fq_lucene}\n")
    fq_tree = parser.parse(fq_lucene)
    fq_dict = {}
    if hasattr(fq_tree, 'name'):
        facet_item = fq_tree.name
        if facet_item in facets:
            fq_dict[facet_item] = _get_operands_for_facet(fq_tree)
            return fq_dict
    for item in fq_tree.children:
        if hasattr(item, 'name'):
            facet_item = item.name
            if facet_item in facets:
                fq_dict[facet_item] = _get_operands_for_facet(item)
    return fq_dict


def _get_operands_for_facet(item):
    op = item.expr.expr
    if type(op) == luqum_vocabulary.Word:
        return [op.value]
    elif type(op) == luqum_vocabulary.AndOperation:
        operands = [y.value for y in op.operands]
        return operands
