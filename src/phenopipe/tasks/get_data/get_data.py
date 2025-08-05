from typing import Optional, TypeVar
from phenopipe.query_connections import BigQueryConnection
from phenopipe.tasks.task import Task


PolarsDataFrame = TypeVar("polars.dataframe.frame.DataFrame")
PolarsLazyFrame = TypeVar("polars.lazyframe.frame.LazyFrame")


class GetData(Task):
    """
    Generic class to retrieve data from database.
    """

    #: bucket folder to save the output
    location: Optional[str] = "phenopipe_wd/datasets"

    large_query: bool = False

    #: either to check for cache in bucket
    cache: Optional[bool] = True

    #: either to check for cache in bucket
    cache_local: Optional[str] = ""

    #: cache type
    cache_type: Optional[str] = "bq"

    #: either to read or scan dataframe
    lazy: Optional[bool] = False

    def model_post_init(self, __context__=None):
        super().model_post_init()
        if self.env_vars.get("query_conn", None) is None:
            self.env_vars["query_conn"] = BigQueryConnection(
                lazy=self.lazy, cache=self.cache
            )
        if hasattr(self, "large_query"):
            if self.large_query:
                self.cache_local = (
                    f"{self.location}/{self.task_name}/{self.task_name}_*.csv"
                )
            else:
                self.cache_local = f"{self.location}/{self.task_name}.csv"
