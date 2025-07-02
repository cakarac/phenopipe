import polars as pl
from typing import Optional, TypeVar
from pydantic import BaseModel


PolarsDataFrame = TypeVar('polars.dataframe.frame.DataFrame')

class Task(BaseModel):
    '''Generic task class representing one step in analysis.'''
    
    #: input dataframes
    inputs: Optional[dict[str, PolarsDataFrame]] 

    #: environment variables applied to each task in analysis plan
    env_vars: Optional[dict[str, str]]

    #: output of complete method representing result of task 
    output: Optional[PolarsDataFrame] 
        
    def complete():
        pass