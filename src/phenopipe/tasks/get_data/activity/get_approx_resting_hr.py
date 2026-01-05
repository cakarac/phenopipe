from phenopipe.tasks.get_data.fixed_query import FixedQuery
from phenopipe.query_builders.fixed_queries import APPROX_RESTING_HR_QUERY


class GetApproxRestingHr(FixedQuery):
    query: str = APPROX_RESTING_HR_QUERY
