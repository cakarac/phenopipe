from typing import Optional
import polars as pl
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion

class GetSleep(GetData):

    #: if query is large according to google cloud api
    large_query: Optional[bool] = True
    
    @completion
    def complete(self):
        '''
        Query fitbit summary and update self.output with resulting dataframe
        '''
        query = '''
                SELECT
                  sleep_daily_summary.person_id
                , sleep_daily_summary.sleep_date as date
                , sleep_daily_summary.is_main_sleep
                , sleep_daily_summary.minute_in_bed
                , sleep_daily_summary.minute_asleep
                , sleep_daily_summary.minute_after_wakeup
                , sleep_daily_summary.minute_awake
                , sleep_daily_summary.minute_restless
                , sleep_daily_summary.minute_deep
                , sleep_daily_summary.minute_light
                , sleep_daily_summary.minute_rem
                , sleep_daily_summary.minute_wake
                FROM
                `sleep_daily_summary` sleep_daily_summary
                '''
        self.output = self.env_vars["query_conn"].get_query(query, self.task_name, self.large_query)
        if isinstance(self.output.collect_schema().get("date"), pl.String):
            self.output = self.output.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
        
