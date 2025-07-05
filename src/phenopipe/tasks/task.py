import polars as pl
from typing import Optional, TypeVar
import inflection
from pydantic import BaseModel, computed_field
import random
import string

PolarsDataFrame = TypeVar('polars.dataframe.frame.DataFrame')


class Task(BaseModel):
    '''Generic task class representing one step in analysis.'''
    
    #: task id
    task_id:str = None

    #: input dataframes
    inputs: Optional[dict[str, PolarsDataFrame]] = None

    #: environment variables applied to each task in analysis plan
    env_vars: Optional[dict[str, str]] = None

    #: output of complete method representing result of task 
    output: Optional[PolarsDataFrame] = None
    
    @computed_field
    @property
    def task_name(self) -> str:
        return inflection.underscore(self.__class__.__name__)
    
    def model_post_init(self, __context__=None):
        if self.task_id is None:
            self.task_id =  "".join(random.choices(string.ascii_letters + string.digits, k=10))

    def complete():
        pass
