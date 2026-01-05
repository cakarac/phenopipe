from phenopipe.tasks.get_data.fixed_query import FixedQuery
from phenopipe.query_builders.fixed_queries import HR_QUERY


class GetHr(FixedQuery):
    query: str = HR_QUERY
