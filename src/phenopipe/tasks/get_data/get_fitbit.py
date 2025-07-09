from typing import Optional
import polars as pl
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion

class GetFitbit(GetData):
    
    #: name of the data query to run
    query_name: Optional[str] = "fitbit"

    #: if query is large according to google cloud api
    large_query: Optional[bool] = True
    
    @completion
    def complete(self):
        '''
        Query fitbit summary and update self.output with resulting dataframe
        '''
        local = self.cacher.get_local(self.query_name, self.location, self.large_query)
        if self.cache and self.cacher.get_cache(self.query_name, local, self.lazy):
            self.output = self.cacher.cached_output
        else:
            self.output = self.run_fitbit_query(local=local)
        if isinstance(self.output.collect_schema().get("date"), pl.String):
            self.output = self.output.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
    def run_fitbit_query(self, local):
        '''Runs fitbit query'''
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
        return self.query_func(query, bucket_id=self.bucket_id, location = local)
