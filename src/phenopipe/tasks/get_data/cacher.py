from typing import Optional, Callable, TypeVar
from subprocess import CalledProcessError
from pydantic import BaseModel
from phenopipe.bucket import ls_bucket, read_csv_from_bucket

PolarsDataFrame = TypeVar('polars.dataframe.frame.DataFrame')
PolarsLazyFrame = TypeVar('polars.lazyframe.frame.LazyFrame')

class Cacher(BaseModel):
        
    #: function to check cache availability
    cache_ls_func: Optional[Callable] = ls_bucket
    
    #: function to cache data
    cache_func: Optional[Callable] = read_csv_from_bucket
    
    #: cached output
    cached_output: PolarsDataFrame|PolarsLazyFrame = None
    
    def get_local(self, query_name, location, large_query):
        if large_query:
            local = f"{location}/{query_name}/{query_name}_*.csv"
        else:
            local = f"{location}/{query_name}.csv"
        return local
    
    def get_cache(self, query_name, local, lazy):
        try:
            self.cache_ls_func(local, return_list=True)
            self.cached_output  = self.cache_func(local, lazy=lazy)
            print(f"{query_name} cached from {local} data")
            return True
        except CalledProcessError as e:
            return False
        