from phenopipe.tasks.get_data.fixed_query import FixedQuery
from phenopipe.query_builders.fixed_queries import WEAR_TIME_HR_QUERY


class GetWearTimeHr(FixedQuery):
    query: str = WEAR_TIME_HR_QUERY
