import os
from typing import Optional
from subprocess import CalledProcessError
from google.cloud.bigquery import Client
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from pydantic import ConfigDict
import polars as pl
from polars.exceptions import ComputeError
import warnings
from phenopipe.bucket import remove_from_bucket
from .query_connection import QueryConnection
import sqlparse

# data type mapping between big query tables and polars. not intended to be full list only to cover most recent aou dataset.
BQ_DATA_MAPPING = {
    "STRING": pl.String,
    "FLOAT64": pl.Float64,
    "FLOAT32": pl.Float32,
    "INT8": pl.Int8,
    "INT16": pl.Int16,
    "INT32": pl.Int32,
    "INT64": pl.Int64,
    "INT128": pl.Int128,
    "TIMESTAMP": pl.Datetime(),
    "DATETIME": pl.Datetime(),
    "DATE": pl.Date,
    "BOOL": pl.Boolean,
    "NUMERIC": pl.Float64,
    "ARRAY<INT64>": pl.List(pl.Int64),
    "ARRAY<STRING>": pl.List(pl.String),
}


class BigQueryConnection(QueryConnection):
    #: bucket id to save the result
    bucket_id: Optional[str] = os.getenv("WORKSPACE_BUCKET")

    #: default dataset
    default_dataset: Optional[str] = os.getenv("WORKSPACE_CDR")

    #: location of the cache in bucket
    cache_loc: str = "__phenopipe"

    #: either to cache query results
    cache: bool = True

    #: either to run caching verbose
    verbose: bool = True

    query_platform: str = "aou"
        
    log_dat: pl.DataFrame = None
        
    model_config = ConfigDict(arbitrary_types_allowed = True)
    
    def get_most_recent_cache(self):
        try:
            self.log_dat = (pl.read_csv(f'{self.bucket_id}/{self.cache_loc}/log_dat.csv').with_columns(pl.col("query_id").cast(pl.Int32)))
        except FileNotFoundError:
            self.log_dat = pl.DataFrame({"query_str":[], "query_id":[], "query_path":[]}, schema_overrides={"query_str":pl.String,"query_id":pl.Int32, "query_path":pl.String}) 
    
    def clear_cache(self):
        remove_from_bucket(self.cache_loc, recursive=True, bucket_id=self.bucket_id)
        
    def remove_cached_query(self, query):
        query = sqlparse.format(query, keyword_case = "lower", reindent=True)
        cache_exists = self.check_cache(query)
        if not cache_exists:
            return None
        else:
            cache_id = self.log_dat.filter(pl.col("query_str") == query)[0, "query_id"]
            print(self.log_dat.filter(pl.col("query_id") != cache_id))
            remove_from_bucket(f'{self.cache_loc}/{cache_id}.csv', recursive=True, bucket_id=self.bucket_id)
            self.log_dat.filter(pl.col("query_id") != cache_id).write_csv(f'{self.bucket_id}/{self.cache_loc}/log_dat.csv', bucket_id=self.bucket_id)
    def check_cache(self, query):
        self.get_most_recent_cache()
        return self.log_dat.filter(pl.col("query_str") == query)
    
    def get_cache(self, query, lazy):
        cache_exists = self.check_cache(query)
        if cache_exists.shape[0] > 0:
            cache = cache_exists.to_dicts()[0]
            dat = pl.scan_csv(f'{self.bucket_id}/{self.cache_loc}/{cache["query_path"]}')
            if not lazy:
                try:
                    dat = dat.collect()
                except ComputeError:
                    print("An issue occured while inferring the schema of cached files so schema inference is omitted!")
                    dat = pl.scan_csv(f'{self.bucket_id}/{self.cache_loc}/{cache["query_path"]}', infer_schema = False).collect()
            return dat
        else:
            return None 
    def save_cache(self, dat, query, cache_id):
        self.get_most_recent_cache()
        dat.write_csv(file = f'{self.bucket_id}/{self.cache_loc}/{cache_id}.csv')
        (self.log_dat.vstack(
            pl.DataFrame(
                {"query_str":[query], "query_id":[cache_id], "query_path": [f"{cache_id}.csv"]}, 
                    schema_overrides={"query_str":pl.String,"query_id":pl.Int32, "query_path":pl.String}))
                .write_csv(f'{self.bucket_id}/{self.cache_loc}/log_dat.csv')
        )
    def get_query(self, query: str, lazy: bool = False):
        query = sqlparse.format(query, keyword_case = "lower", reindent=True)
        
        if self.cache:
            df = self.get_cache(query, lazy)
            if df is not None:
                return df
        client = Client()
        res = client.query_and_wait(
                    query,
                    job_config=bigquery.job.QueryJobConfig(
                        default_dataset=self.default_dataset
                    ),
        )
        dat = pl.from_arrow(res.to_arrow())
        if self.cache:
            if self.log_dat.shape[0] == 0:
                cache_id = 0
            else:
                cache_id = self.log_dat["query_id"].max()+1
            if res._table:
                try:
                    ex_res = client.extract_table(
                        res._table, f"{self.bucket_id}/{self.cache_loc}/{cache_id}.csv"
                    )
                    if ex_res.result().done():
                        print(f"Given query is successfully saved into {self.cache_loc}")
                        self.log_dat.vstack(
                            pl.DataFrame({"query_str":[query], "query_id":[cache_id], "query_path": [f"{cache_id}.csv"]}, 
                                         schema_overrides={"query_str":pl.String,"query_id":pl.Int32, "query_path":pl.String})
                            ).write_csv(f'{self.bucket_id}/{self.cache_loc}/log_dat.csv')
                except BadRequest as e:
                    ex_res = client.extract_table(
                        res._table, f"{self.bucket_id}/{self.cache_loc}/{cache_id}/*.csv"
                    )
                    if ex_res.result().done():
                        print(f"Given query is successfully saved into {self.cache_loc}")
                        self.log_dat.vstack(
                            pl.DataFrame({"query_str":[query], "query_id":[cache_id], "query_path": [f"{cache_id}/*.csv"]}, 
                                         schema_overrides={"query_str":pl.String,"query_id":pl.Int32, "query_path":pl.String})
                            ).write_csv(f'{self.bucket_id}/{self.cache_loc}/log_dat.csv')
                    warnings.warn(
                        f"Query cached in shards due to large size."
                    )
            else:
                self.save_cache(dat, query, cache_id)
                warnings.warn(
                    f"Query didn't return any table. Given result is saved into {self.cache_loc}"
                )
        if not lazy:
            return dat
        else:
            return dat.lazy()
    
    def get_table_names(self):
        """
        Get table names from the default dataset
        """
        query = """SELECT table_name FROM `INFORMATION_SCHEMA.TABLES`;"""
        tables = self.get_query(query)
        return list(map(lambda x: x.get("table_name"), tables))

    def get_table_schema(self, table: str):
        """
        Gets te column names and datatypes of the given table
        :param table: Table name to get columns from
        """
        query = """SELECT * FROM `INFORMATION_SCHEMA.COLUMNS`;"""
        columns = self.get_query(query)
        return pl.Schema(
            {
                col.get("column_name"): BQ_DATA_MAPPING[col.get("data_type")]
                for col in columns
                if col.get("table_name") == table
            }
        )
