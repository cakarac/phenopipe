import os
from typing import Optional, Callable, TypeVar
from subprocess import CalledProcessError
from google.cloud import bigquery
import polars as pl
from phenopipe.bucket import ls_bucket, read_csv_from_bucket
from .query_connection import QueryConnection

#data type mapping between big query tables and polars. not intended to be full list only to cover most recent aou dataset.
BQ_DATA_MAPPING = {
    "STRING":pl.String,
    "FLOAT64":pl.Float64,
    "FLOAT32":pl.Float32,
    "INT8":pl.Int8,
    "INT16":pl.Int16,
    "INT32":pl.Int32,
    "INT64":pl.Int64,
    "INT128":pl.Int128,
    "TIMESTAMP":pl.Datetime(),
    "DATETIME":pl.Datetime(),
    "DATE":pl.Date,
    "BOOL":pl.Boolean,
    "NUMERIC":pl.Float64,
    "ARRAY<INT64>":pl.List(pl.Int64),
    "ARRAY<STRING>":pl.List(pl.String),
}

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

    def get_query_rows(self, query: str, return_df: bool = False):
        '''
        Runs the given query and returns the client and bigquery iterator
        :param query: Query string to run with google big query.
        '''
        client = bigquery.Client()
        res = client.query_and_wait(query, job_config = bigquery.job.QueryJobConfig(default_dataset= self.default_dataset))
        if return_df:
            return res
        else:
            return pl.from_arrow(res.to_arrow())
    
    def get_query_df(self,
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

    def get_table_names(self):
        '''
        Get table names from the default dataset
        '''
        query = '''SELECT table_name FROM `INFORMATION_SCHEMA.TABLES`;'''
        tables = self.get_query_rows(query)
        return list(map(lambda x: x.get("table_name"), tables))
    
    
    def get_table_schema(self, table:str):
        '''
        Gets te column names and datatypes of the given table
        :param table: Table name to get columns from
        '''
        query = '''SELECT * FROM `INFORMATION_SCHEMA.COLUMNS`;'''
        columns = self.get_query_rows(query)
        return pl.Schema({col.get("column_name"):BQ_DATA_MAPPING[col.get("data_type")] for col in columns if col.get("table_name") == table})
