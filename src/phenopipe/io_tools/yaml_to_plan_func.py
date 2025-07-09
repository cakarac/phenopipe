from pathlib import Path
import yaml
from .dict_to_plan_func import dict_to_plan

def yaml_to_plan(yaml_file):
    return dict_to_plan(yaml.safe_load(Path(yaml_file).read_text()))