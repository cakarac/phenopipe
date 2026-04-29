import yaml
from importlib import import_module
from pathlib import Path
from functools import reduce

def plan_from_dict(plan_dict):
    plan = dict()
    env_vars = plan_dict.get("env_vars", {})
    
    env_vars["query_conn"] = getattr(
        import_module("phenopipe.query_connections"), plan_dict.get("query_conn", {}).get("module", "BigQueryConnection")
    )(**plan_dict.get("query_conn", {}).get("params", {}))
    
    input_mapping = {}
    modules = {k: import_module(v) for k, v in plan.get("modules", {}).items()}
    lazy = plan_dict.get("lazy", False)
    tasks = plan_dict["tasks"]
    for task_id, task in tasks.items():
        if task["name"].split(".")[0] == "modules":
            task_address = task["name"].split(".")
            task_class = getattr(modules[task_address[1]], task_address[2])
        else:
            task_class = getattr(
                import_module(".".join(task["name"].split(".")[:-1])),
                task["name"].split(".")[-1],
            )
        if "inputs" in list(task.keys()):
            input_mapping[task_id] = task["inputs"]
        task_obj = task_class(
            task_id=task_id,
            env_vars=env_vars,
            **{k: v for k, v in task.items() if k not in ["inputs", "name"]},
        )
        if hasattr(task_obj, "lazy"):
            task_obj.lazy = lazy
        plan.update({task_id: task_obj})
    if input_mapping:
        all_inputs = reduce(lambda x,y: x+y, [list(i.values()) for i in input_mapping.values()])
        input_mapping = {
            k: {el: plan[w] for el, w in v.items()} for k, v in input_mapping.items()
        }
        for tsk, inputs in input_mapping.items():
            plan[tsk].input_tasks.update(inputs)
        plan = {k:v for k,v in plan.items() if k not in all_inputs}
    return plan

def plan_from_yaml_str(plan_str):
    plan = yaml.safe_load(plan_str)
    return plan_from_dict(plan)

def plan_from_yaml(file_name):
    return plan_from_yaml_str(Path(file_name).read_text())
