from luqum.parser import parser
from luqum import tree


showcase_facets = ['tags', 'groups', 'showcase_type']

def analyze_fq(fq_lucene):
    if not fq_lucene:
        return {}
    fq_dict = {}
    fq_tree = parser.parse(fq_lucene)
    for item in fq_tree.children:
        if hasattr(item, 'name'):
            facet = item.name
            if facet in showcase_facets:
                op = item.expr.expr
                if type(op) == tree.Word:
                    fq_dict[facet] = [op.value]
                elif type(op) == tree.AndOperation:
                    operands = [y.value for y in op.operands]
                    fq_dict[facet] = operands
    return fq_dict
