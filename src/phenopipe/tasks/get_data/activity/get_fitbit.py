from phenopipe.tasks.get_data.fixed_query import FixedQuery
from phenopipe.query_builders.fixed_queries import FITBIT_QUERY


class GetFitbit(FixedQuery):
    query: str = FITBIT_QUERY
