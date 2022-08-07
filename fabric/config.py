from typing import List, Union
from pydantic import BaseModel as _Base
import yaml
from pathlib import Path


class BaseConf(_Base):
    class Config:
        validate_all = True
        allow_mutation = False


def SingleOrList(inner_type):
    return Union[inner_type, List[inner_type]]


def optional_load_config(fname="config.yml"):
    cfg = {}
    conf_fname = Path.cwd() / fname
    if conf_fname.is_file():
        with conf_fname.open("r") as f:
            raw = f.read()
            print("loaded config\n ")
            print(raw)  # yaml raw itself is well formatted
            cfg = yaml.safe_load(raw)
    return cfg


def write_full_config(cfg_obj, fname="full_config.yml"):
    cfg = cfg_obj.dict()
    with (Path.cwd() / fname).open("w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)
