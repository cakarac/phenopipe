import os
from typing import Optional
from subprocess import CalledProcessError
from google.cloud.bigquery import Client
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from pydantic import ConfigDict
import polars as pl
import warnings
from phenopipe.bucket import read_csv_from_bucket, write_csv_to_bucket, remove_from_bucket
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
            write_csv_to_bucket(self.log_dat.filter(pl.col("query_id") != cache_id),
                               f'{self.cache_loc}/log_dat.csv', bucket_id=self.bucket_id)
    def check_cache(self, query):
        try:
            self.log_dat = (read_csv_from_bucket(f'{self.cache_loc}/log_dat.csv', cache=False, lazy=False, bucket_id=self.bucket_id)
                           .with_columns(pl.col("query_id").cast(pl.Int32)))
        except CalledProcessError:
            self.log_dat = pl.DataFrame({"query_str":[], "query_id":[]}, schema_overrides={"query_str":pl.String,"query_id":pl.Int32}) 
            return False
        return self.log_dat.filter(pl.col("query_str") == query).shape[0] != 0
    def get_cache(self, query, lazy):
        cache_exists = self.check_cache(query)
        if cache_exists:
            return read_csv_from_bucket(self.cache_loc + "/" + str(self.log_dat[0, "query_id"]) + ".csv", cache=False, lazy = lazy)
        else:
            return None 
    def save_cache(self, dat, query, cache_id):
        write_csv_to_bucket(dat=dat, file = f'{self.cache_loc}/{cache_id}.csv', bucket_id = self.bucket_id)
        write_csv_to_bucket(dat=self.log_dat.vstack(pl.DataFrame({"query_str":[query], "query_id":[cache_id]}, schema_overrides={"query_str":pl.String,"query_id":pl.Int32})), 
                            file = f'{self.cache_loc}/log_dat.csv', bucket_id = self.bucket_id)
    def get_query(self, query: str, lazy: bool = False, cache: bool = True):
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
                        write_csv_to_bucket(dat=self.log_dat.vstack(pl.DataFrame({"query_str":[query], "query_id":[cache_id]}, schema_overrides={"query_str":pl.String,"query_id":pl.Int32})), 
                                file = f'{self.cache_loc}/log_dat.csv', bucket_id = self.bucket_id)
                except BadRequest as e:
                    ex_res = client.extract_table(
                        res._table, f"{self.bucket_id}/{self.cache_loc}/{cache_id}/*.csv"
                    )
                    if ex_res.result().done():
                        print(f"Given query is successfully saved into {self.cache_loc}")
                        write_csv_to_bucket(dat=self.log_dat.vstack(pl.DataFrame({"query_str":[query], "query_id":[cache_id]}, schema_overrides={"query_str":pl.String,"query_id":pl.Int32})), 
                                file = f'{self.cache_loc}/log_dat.csv', bucket_id = self.bucket_id)
                    warnings.warn(
                        f"Query cached in shards due to large size."
                    )
            else:
                self.save_cache(pl.from_arrow(res.to_arrow()), query, cache_id)
                warnings.warn(
                    f"Query didn't return any table. Given result is saved into {self.cache_loc}"
                )
        if not lazy:
            return pl.from_arrow(res.to_arrow())
        else:
            return pl.from_arrow(res.to_arrow()).lazy()
        
    def get_table_names(self):
        """
        Get table names from the default dataset
        """
        query = """SELECT table_name FROM `INFORMATION_SCHEMA.TABLES`;"""
        tables = self.get_query_rows(query)
        return list(map(lambda x: x.get("table_name"), tables))

    def get_table_schema(self, table: str):
        """
        Gets te column names and datatypes of the given table
        :param table: Table name to get columns from
        """
        query = """SELECT * FROM `INFORMATION_SCHEMA.COLUMNS`;"""
        columns = self.get_query_rows(query)
        return pl.Schema(
            {
                col.get("column_name"): BQ_DATA_MAPPING[col.get("data_type")]
                for col in columns
                if col.get("table_name") == table
            }
        )
