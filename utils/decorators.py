import re
import inspect
from datetime import datetime
from neo4j.work.transaction import Transaction
from utils.query_builder import QueryResult


def log_query(decorated):
    def inner(*args, **kwargs):
        print("\nQUERY ===========================================================================")
        print("---- args")
        print(inspect.signature(decorated))
        for arg in args:
            if type(arg) == Transaction:
                pass
            elif type(arg) == dict:
                print(f"dictionary with # of keys: {len(arg.keys())}")
            elif type(arg) == list:
                print(f"{arg[:5]} ... (list truncated)")
            else:
                print(arg)
        print("---- docs")
        print(f"Purpose: {decorated.__doc__}")
        t_start = datetime.now()
        query_result = decorated(*args, **kwargs)
        t_finish = datetime.now()
        print("---- performance (runtime)")
        print(f"{t_finish - t_start}")
        print("---- query")
        print(re.sub('IN \[[\S]*\]', 'IN [...]', query_result.query))
        return query_result.result
    return inner