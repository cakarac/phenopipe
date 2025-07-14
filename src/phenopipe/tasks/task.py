import string
import random
from functools import wraps
from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Any
import polars as pl
import inflection
from pydantic import (BaseModel, 
                      computed_field, 
                      field_validator,
                      model_serializer)

PolarsDataFrame = TypeVar('polars.dataframe.frame.DataFrame')
PolarsLazyFrame = TypeVar('polars.lazyframe.frame.LazyFrame')

def completion(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        self.complete_input_tasks()
        print(f"Starting completion of {self.task_name} with id {self.task_id}")
        self.validate_min_inputs_schemas()
        func(*args, **kwargs)
        self.validate_min_output_schema()
        self.completed = True
    return wrapper

class Task(BaseModel, ABC):
    '''Generic task class representing one step in analysis.'''
    
    #: task id
    task_id:str = None

    #: input dataframes
    inputs: dict = {}

    #: input tasks
    input_tasks: dict = {}
        
    #: minimum requirements on inputs schema to avoid any errors
    min_inputs_schemas: Optional[dict[str, dict]] = {}
    
    #: minimum requirements on output schema
    min_output_schema: Optional[dict[str, str]] = {}

    #: environment variables applied to each task in analysis plan
    env_vars: Optional[dict[str, Any]] = {}

    #: task variables specific to each task
    task_vars: Optional[dict[str, Any]] = {}

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
    
    @model_serializer()
    def serialize_task(self):
        mod = {
            "task_name":self.task_name,
            "task_id":self.task_id,
            "env_vars":self.env_vars,
            "input_tasks":self.input_tasks
        }
        return {
            k:v for k, v in mod.items() if v
        }
    
    def model_post_init(self, __context__=None):
        if self.task_id is None:
            self.task_id =  "".join(random.choices(string.ascii_letters + string.digits, k=10))

    def validate_min_inputs_schemas(self):
        print("Validating the inputs...")
        for k in self.min_inputs_schemas.keys():
            sc = self.inputs[k].collect_schema().to_python()
            try:
                if dict(sc, **self.min_inputs_schemas[k]) != sc:
                    raise ValueError("minimal inputs schemas are not satisfied!")
            except KeyError as e:
                raise ValueError("missing input dataframe")
        return True
        
    def validate_min_output_schema(self):
        print("Validating the output...")
        sc = self.output.collect_schema().to_python()
        if dict(sc, **self.min_output_schema) != sc:
            raise ValueError("minimal output schemas are not satisfied!")
        return True
        
    class Config:
        validate_assignment = True
    
    def complete_input_tasks(self):
        for task in self.input_tasks.values():
            if not task.completed:
                task.complete()
        self.inputs.update(**{k:v.output for k,v in self.input_tasks.items()})

    @abstractmethod
    def complete():
        pass
