import importlib
import inflection

def dict_to_plan(d):
    task = getattr(importlib.import_module("__main__"), inflection.camelize(d["task_name"]))
    if not d.get("input_tasks", None):
        return task(**d)
    else:
        return task(**{**d, **{"input_tasks":{k:dict_to_plan(v) for k,v in d["input_tasks"].items()}}})