from pathlib import Path
import json
import yaml


def dir_of_this_file(__filevar__):
    '''
    this is not cwd; this is the directory of the __file__
    '''
    curr_file_dir = Path(__filevar__).resolve().parent
    return curr_file_dir


def json_read(fname):
    with open(fname, "r") as f:
        return json.load(f)


def json_write(fname, payload, **kwargs):
    with open(fname, "w") as f:
        json.dump(payload, f, **kwargs)


def yaml_read(fname):
    with open(fname, "r") as f:
        return yaml.safe_load(f)


def yaml_write(fname, payload, **kwargs):
    with open(fname, "w") as f:
        yaml.safe_dump(payload, f, **kwargs)
