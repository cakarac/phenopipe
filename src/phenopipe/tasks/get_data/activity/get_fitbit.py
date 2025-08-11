from phenopipe.tasks.get_data.fixed_query import FixedQuery
from phenopipe.query_builders.fixed_queries import FITBIT_QUERY


class GetFitbit(FixedQuery):
    #: if query is large according to google cloud api
    large_query: bool = True

    query: str = FITBIT_QUERY
