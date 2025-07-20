import os
import polars as pl
from databricks.connect import DatabricksSession
from .query_connection import QueryConnection

from typing import TypeVar, Optional
from subprocess import CalledProcessError

PolarsDataFrame = TypeVar('polars.dataframe.frame.DataFrame')
PolarsLazyFrame = TypeVar('polars.lazyframe.frame.LazyFrame')

class DatabricksQueryConnection(QueryConnection):
    #: databricks cluster name
    cluster_id: Optional[str] = os.getenv("DATABRICKS_CLUSTER")

    #: databricks host
    host: Optional[str] = os.getenv("DATABRICKS_HOST")
        
    #: databricks token
    token:Optional[str] = os.getenv("DATABRICKS_TOKEN")

    #defult databricks catalog
    default_catalog:Optional[str] = os.getenv("DATABRICKS_DEFAULT_CATALOG")

    #defult databricks catalog
    default_database:Optional[str] = os.getenv("DATABRICKS_DEFAULT_DATABASE")

    #limit the query rows
    limit:Optional[int] = -1
    def get_query_df(self,
                query: str,
                *args,**kwargs) -> pl.DataFrame:
        '''
        Runs the given query on databricks remote connection and returns the output as polars dataframe
        :param query: Query string to on databricks
        '''
        res = self.get_query_rows(query, return_df=False)
        if self.limit != -1:
            res = res.limit(self.limit)
        return pl.from_pandas(res.toPandas())
    
    def get_query_rows(self,
                query: str,
                return_df: bool = False,
                *args,**kwargs) -> pl.DataFrame:
        '''
        Runs the given query on databricks remote connection and returns the output as polars dataframe
        :param query: Query string to on databricks
        '''
        spark = DatabricksSession.builder.remote(
            host=self.host,
            token=self.token,
            cluster_id=self.cluster_id).getOrCreate()
        spark.catalog.setCurrentCatalog(self.default_catalog)
        spark.catalog.setCurrentDatabase(self.default_database)
        res = spark.sql(query)
        if self.limit != -1:
            res = res.limit(self.limit)
        if return_df:
            return pl.from_pandas(res.toPandas())
        else:
            return res
    