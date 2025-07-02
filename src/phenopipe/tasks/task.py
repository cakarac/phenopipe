import polars as pl
from typing import Optional, TypeVar
from pydantic import BaseModel


PolarsDataFrame = TypeVar('polars.dataframe.frame.DataFrame')

class Task(BaseModel):
    '''Generic task class representing one step in analysis.'''
    
    #: input dataframes
    inputs: dict[str, PolarsDataFrame] | None

    #: environment variables applied to each task in analysis plan
    env_vars: dict[str, str] | None

    #: output of complete method representing result of task 
    output: PolarsDataFrame | None
        
    def complete():
        pass