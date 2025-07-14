from typing import Optional
import polars as pl
from phenopipe.tasks.get_data.get_data import GetData
from phenopipe.tasks.task import completion

class GetWearTime(GetData):
    #: name of the data query to run
    query_name: Optional[str] = "wear_time"

    #: if query is large according to google cloud api
    large_query: Optional[bool] = False
    
    @completion
    def complete(self):
        '''
        Query wear time data and update self.output with resulting dataframe
        '''
        '''runs wear time query'''
        query = '''
                    SELECT person_id, date, SUM(has_hour) AS wear_time
                    FROM (SELECT person_id, CAST(datetime AS DATE) AS date, IF(SUM(steps)>0, 1, 0) AS has_hour
                          FROM `steps_intraday`
                          GROUP BY CAST(datetime AS DATE), EXTRACT(HOUR FROM datetime), person_id) t
                    GROUP BY date, person_id 
                '''
        self.output = self.env_vars["query_conn"].get_query(query, self.query_name, self.large_query)      
        if isinstance(self.output.collect_schema().get("date"), pl.String):
            self.output = self.output.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))