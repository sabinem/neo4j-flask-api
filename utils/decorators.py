from datetime import datetime
from utils.query_builder import QueryResult


def log_query(decorated):
    def inner(*args, **kwargs):
        t_start = datetime.now()
        query_result = decorated(*args, **kwargs)
        t_finish = datetime.now()
        print("---------------- Query ------------------------")
        print(f"Purpose: {decorated.__doc__}")
        print(f"Runtime: {t_finish - t_start}")
        print(query_result.query)
        return query_result.result
    return inner