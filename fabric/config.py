from typing import List, Union
from pathlib import Path
import argparse
from argparse import RawDescriptionHelpFormatter
import yaml

from pydantic import BaseModel as _Base
from .deploy.sow import ConfigMaker


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
    cfg = _dict_to_yaml(cfg)
    print(f"\n--- full config ---\n\n{cfg}\n")
    with (Path.cwd() / fname).open("w") as f:
        f.write(cfg)


def argparse_cfg_template(curr_cfgs):
    parser = argparse.ArgumentParser(
        description='Manual spec of configs',
        epilog=f'curr cfgs:\n\n{_dict_to_yaml(curr_cfgs)}',
        formatter_class=RawDescriptionHelpFormatter
    )
    _, args = parser.parse_known_args()
    clauses = []
    for i in range(0, len(args), 2):
        assert args[i][:2] == "--", "please start args with --"
        clauses.append({args[i][2:]: args[i+1]})
    print(f"cmdline clauses: {clauses}")

    maker = ConfigMaker(curr_cfgs)
    for clu in clauses:
        maker.execute_clause(clu)

    final = maker.state.copy()
    return final


def _dict_to_yaml(arg):
    return yaml.safe_dump(arg, sort_keys=False)
