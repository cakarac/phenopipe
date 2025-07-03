import os
from typing import Optional, Callable
from phenopipe.query_connections import get_big_query
from phenopipe.tasks.get_data.cacher import Cacher
from phenopipe.tasks.task import Task


class GetData(Task):
    '''
    Generic class to retrieve data from database.
    '''    
    #: bucket folder to save the output
    location: str = "phenopipe_wd/datasets" 

    #: bucket id to save the result
    bucket_id: Optional[str] = os.getenv("WORKSPACE_BUCKET") 

    #: query connector to retrive data
    query_func: Optional[Callable] = get_big_query 

    #: either to check for cache in bucket
    cache: Optional[bool] = True 

    #: cacher object to check and retrieve cached data
    cacher: Optional[Cacher] = Cacher()

    #: either to read or scan dataframe
    lazy: Optional[bool] = False
    