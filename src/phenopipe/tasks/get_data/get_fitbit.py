from typing import Optional
import polars as pl
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion

class GetFitbit(GetData):

    #: if query is large according to google cloud api
    large_query: Optional[bool] = True
    
    @completion
    def complete(self):
        '''
        Query fitbit summary and update self.output with resulting dataframe
        '''
        query = '''
                SELECT
                    activity_summary.person_id,
                    activity_summary.date,
                    activity_summary.steps,
                    activity_summary.fairly_active_minutes,
                    activity_summary.lightly_active_minutes,
                    activity_summary.sedentary_minutes,
                    activity_summary.very_active_minutes
                FROM
                    `activity_summary` activity_summary
                '''
        self.output = self.env_vars["query_conn"].get_query(query, self.task_name, self.large_query)
        if isinstance(self.output.collect_schema().get("date"), pl.String):
            self.output = self.output.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
        
