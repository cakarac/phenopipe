import os
from typing import Optional, Callable, TypeVar
from subprocess import CalledProcessError
from google.cloud import bigquery
import polars as pl
from phenopipe.bucket import ls_bucket, read_csv_from_bucket
from .query_connection import QueryConnection

PolarsDataFrame = TypeVar('polars.dataframe.frame.DataFrame')
PolarsLazyFrame = TypeVar('polars.lazyframe.frame.LazyFrame')

class BigQueryConnection(QueryConnection):
    #: bucket folder to save the output
    location: Optional[str] = "phenopipe_wd/datasets" 

    #: bucket id to save the result
    bucket_id: Optional[str] = os.getenv("WORKSPACE_BUCKET") 
        
    #: default dataset
    default_dataset: Optional[str] = os.getenv("WORKSPACE_CDR")
    
    #: either to check for cache in bucket
    cache: bool = True 

    #: either to read or scan dataframe
    lazy: bool = False
    
    #: function to check cache availability
    cache_ls_func: Optional[Callable] = ls_bucket
    
    #: function to cache data
    cache_func: Optional[Callable] = read_csv_from_bucket
    
    #: cached output
    cached_output: PolarsDataFrame|PolarsLazyFrame = None

    def get_query(self,
                query: str,
                query_name:str,
                large_query:bool) -> pl.DataFrame:
        '''
        Runs the given query and saves the resulting dataframe to the given bucket and location and returns the dataframe
        :param query: Query string to run with google big query.
        '''
        if large_query:
            local = f"{self.location}/{query_name}/{query_name}_*.csv"
        else:
            local = f"{self.location}/{query_name}.csv"
        
        if self.cache:
            try:
                self.cache_ls_func(local, return_list=True)
                self.cached_output  = self.cache_func(local, lazy=self.lazy)
                print(f"{query_name} cached from {local} data")
                return self.cached_output
            except CalledProcessError as e:
                print(f"No cache found for {query_name} at {local}")
            
        client = bigquery.Client()
        res = client.query_and_wait(query, job_config = bigquery.job.QueryJobConfig(default_dataset=self.default_dataset))
        ex_res = client.extract_table(res._table, f'{self.bucket_id}/{local}')
        if ex_res.result().done():
            print(f"Given query is successfully saved into {local}")
        if self.lazy:
            return pl.scan(local)
        else:
            return pl.from_arrow(res.to_arrow())
