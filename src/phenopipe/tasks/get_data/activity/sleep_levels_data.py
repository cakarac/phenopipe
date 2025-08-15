from typing import List
import datetime
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion
from phenopipe.query_builders import sleep_level_query


class SleepLevelsData(GetData):
    large_query: bool = True
    sleep_levels: List[str]
    sql_aggregation: str = "all"
    min_output_schema = {
        "person_id": int,
        "is_main_sleep": bool,
        "sleep_date": datetime.date,
        "sleep_datetime":datetime.datetime,
        "sleep_level":str
    }
    @completion
    def complete(self):
        """
        Generic icd condition occurance query phenotype
        """
        sleep_query_to_run = sleep_level_query(self.sleep_levels, self.sql_aggregation)
        self.output = self.env_vars["query_conn"].get_query_df(
            sleep_query_to_run, self.task_name, self.lazy, self.cache, self.cache_local
        )