from luqum.parser import parser
from luqum import tree


showcase_facets = ['tags', 'groups', 'showcase_type']

def analyze_fq(fq):
    fq_tree = parser.parse(fq)
    for item in fq_tree.children:
        if item.name in showcase_facets:
            op = item.expr.expr
            if type(op) == tree.AndOperation:
                operands = [y.value for y in op.operands]
                print(operands)
    return fq
