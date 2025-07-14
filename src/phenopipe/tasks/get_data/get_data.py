import os
from typing import Optional, Callable
from pydantic import InstanceOf
from phenopipe.query_connections.query_connection import QueryConnection
from phenopipe.query_connections import BigQueryConnection
from phenopipe.tasks.task import Task


class GetData(Task):
    '''
    Generic class to retrieve data from database.
    '''    
    #: either to check for cache in bucket
    cache: Optional[bool] = True 

    #: either to read or scan dataframe
    lazy: Optional[bool] = False

    def model_post_init(self, __context__=None):
        super().model_post_init()
        if self.env_vars.get("query_conn", None) is None:
            self.env_vars["query_conn"] =  BigQueryConnection(lazy=self.lazy, cache = self.cache)
