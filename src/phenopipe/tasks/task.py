import polars as pl
from typing import Optional, TypeVar
import inflection
from pydantic import BaseModel, computed_field, field_validator
import random
import string

PolarsDataFrame = TypeVar('polars.dataframe.frame.DataFrame')
PolarsLazyFrame = TypeVar('polars.lazyframe.frame.LazyFrame')

class Task(BaseModel):
    '''Generic task class representing one step in analysis.'''
    
    #: task id
    task_id:str = None

    #: input dataframes
    inputs: dict = {}
        
    #: minimum requirements on inputs schema to avoid any errors
    min_inputs_schemas: Optional[dict[str, dict]] = {}

    #: environment variables applied to each task in analysis plan
    env_vars: Optional[dict[str, str]] = None

    #: output of complete method representing result of task 
    output: PolarsDataFrame|PolarsLazyFrame = None

    #: either the task is completed
    completed: bool = False
    
    
    @field_validator('inputs', mode='after')
    @classmethod
    def validate_task_inputs(cls, inputs: dict) -> dict:
        if not isinstance(inputs, dict):
            raise ValueError("inputs must be None or a dictionary")
        elif not all([isinstance(k, str) for k in list(inputs.keys())]):
            raise ValueError("all keys of inputs dictionary must be strings")
        elif not (all([isinstance(v, pl.DataFrame) for v in list(inputs.values())]) or
                 all([isinstance(v, pl.LazyFrame) for v in list(inputs.values())])):
            raise ValueError("all values of inputs dictionary must be polars dataframe or polars lazyframe")
        return inputs
    
    @computed_field
    @property
    def task_name(self) -> str:
        return inflection.underscore(self.__class__.__name__)
    
    def model_post_init(self, __context__=None):
        if self.task_id is None:
            self.task_id =  "".join(random.choices(string.ascii_letters + string.digits, k=10))

    def validate_min_inputs_schemas(self):
        for k in self.min_inputs_schemas.keys():
            sc = self.inputs[k].collect_schema().to_python()
            try:
                if dict(sc, **self.min_inputs_schemas[k]) != sc:
                    raise ValueError("minimal inputs schemas are not satisfied!")
            except KeyError as e:
                raise ValueError("missing input dataframe")
        return True
        
    class Config:
        validate_assignment = True
    def complete():
        pass
