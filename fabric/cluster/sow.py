import argparse
import yaml
import os
import os.path as osp
import shutil
from copy import deepcopy
from collections import Mapping

from termcolor import colored, cprint


# '_' prefix so that it is sorted to the top when dumping yaml
_META_FIELD_NAME = '__meta__'

_LAUNCH_FIELDS_SPEC = {
    'required': ['group', 'particular'],
    'optional': ['base_modify'],
    'either': ['import_base', 'base']  # one of the field must be present
}

_PARTICULAR_FIELDS_SPEC = {
    'required': ['name'],
    'optional': ['modify', 'expand'],
    'either': []
}

for _field_spec in (_LAUNCH_FIELDS_SPEC, _PARTICULAR_FIELDS_SPEC):
    _field_spec['all'] = _field_spec['required'] + _field_spec['optinal'] \
        + _field_spec['either']


def plant_files(launch_dir, dir_name, config_dict):
    '''
    plant the config and related files (run.py eval.ipynb) into the specified directory
    If these files already exist, compare edit timestamp and only copy if outdated.
    For simplicity, this bahavior is not implemented. Just copy
    '''
    with open(osp.join(dir_name, 'config.yml'), 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False)
    if osp.isfile( osp.join(launch_dir, 'run.py') ):
        # shutil.copy(osp.join(launch_dir, 'run.py'), dir_name)
        src = osp.join(launch_dir, 'run.py')
        target = osp.join(dir_name, 'run.py')
        if osp.islink(target):
            os.unlink(target)
        elif osp.isfile(target):
            os.remove(target)
        # note that use relative path for maintainability.
        # also it is dirname that is needed
        os.symlink(osp.relpath(src, osp.abspath(dir_name)), target)


class ConfigMaker():
    def __init__(self, base_cfg_dict):
        self.state = base_cfg_dict
        self.clauses = []

    def clone(self):
        pass

    def execute_clause(self, raw_clause_str):
        pass

    def parse_clause(self, raw_clause_str):
        pass

    @staticmethod
    def _trace_cfg_path(src_dict, path):
        pass

    @staticmethod
    def replace(src_dict):
        pass

    @staticmethod
    def add(src_dict):
        pass

    @staticmethod
    def delete(src_dict):
        pass

    def __str__(self):
        pass

    def __repr__(self):
        pass


def validate_dict_fields(src_dict, field_spec):
    assert isinstance(src_dict, dict)
    # check required fields are present
    for k in field_spec['required']:
        assert k in src_dict and src_dict[k], \
            "field {} is required and must be truthy. Given {}".format(
                k, src_dict.keys()
        )
    # check 'or' fields are present
    present = False
    for k in field_spec['either']:
        if k in src_dict and src_dict[k]:
            present = True
    if not present:
        raise ValueError(
            "one of {} is required and must be truthy. Given {}".format(
                field_spec['either'], src_dict.keys()
            )
        )
    # check no extra fields present
    for k in src_dict.keys():
        if k not in field_spec['all']:
            raise ValueError("field {} is not allowed among {}".format(
                k, src_dict['all']
            ))


def dfs_expand(level, name, maker, deposit, grids):
    if level == len(grids):
        name = name[:-1]  # throw away the '_' at the end of say 'lr_lo_'
        deposit[name] = maker.clone()

    tier = grids[level]
    iter_clauses = list(tier.keys())
    size = len(tier[iter_clauses[0]])
    for clause in iter_clauses:
        assert len(tier[clause]) == size

    if 'alias' in tier:
        alias = tier['alias']
        iter_clauses.remove('alias')
    else:
        alias = range(size)

    for nick_name, inx in zip(alias, range(size)):
        nick_name = '{}{}_'.format(name, nick_name)
        curr_maker = maker.clone()
        for clause in iter_clauses:
            arg = tier[clause][inx]
            clause = {clause: arg}
            curr_maker.execute_clause(clause)
        dfs_expand(level + 1, nick_name, curr_maker, deposit, grids)


def parse_launch_config(launch_config):
    validate_dict_fields(launch_config, _LAUNCH_FIELDS_SPEC)
    acc = {}
    # WARNING assume for now that base is filled. No import yet
    group_name = launch_config['group']
    base_maker = ConfigMaker(launch_config['base'])

    # execute base modifications
    if 'base_modify' in launch_config:
        clauses = launch_config['base_modify']
        for _clau in clauses:
            base_maker.execute_clause(_clau)

    # execute particular configs
    for part in launch_config['particular']:
        validate_dict_fields(part, _PARTICULAR_FIELDS_SPEC)
        curr_maker = base_maker.clone()
        part_name = part['name']
        # exec modifications
        if 'modify' in part:
            clauses = part['modify']
            for _clau in clauses:
                curr_maker.execute_clause(_clau)
        # expand the field
        if 'expand' in part:
            assert isinstance(part['expand'], list)
            dfs_expand(
                level=0, name='{}_'.format(part_name),
                maker=curr_maker.clone(), deposit=acc, grids=part['expand']
            )
        else:
            acc[part_name] = curr_maker

    return acc


def main():
    parser = argparse.ArgumentParser(description='deploy the experiments')
    parser.add_argument(
        '-f', '--file', type=str, required=True,
        help='a yaml based on our convention describing the experiments to run'
    )
    parser.add_argument(
        '-m', '--mock', nargs='*', type=str, required=False,
        help='specify a list of configs to be printed in full details'
    )
    args = parser.parse_args()

    launch_fname = args.file
    launch_dir_path = osp.dirname( osp.abspath(launch_fname) )

    with open(launch_fname, 'r') as f:
        launch_config = yaml.load(f)

    cfgs_name_2_maker = parse_launch_config(launch_config)

    for name, maker in cfgs_name_2_maker.items():
        cprint(name, color='blue')
        print(maker.state)

    # # check the launch dir has consistent naming on group
    # if 'group' not in configDict:
    #     raise ValueError("group configuration requires a name")

    # dir_name = launch_dir_abs_path.split('/')[-1]
    # if dir_name != configDict['group']:
    #     raise ValueError("config group name: {}, but launch dir name: {}. Match them"
    #                      .format(configDict['group'], dir_name))
    # del dir_name

    # # go into runs/exp_group_name/
    # os.chdir(launch_dir_abs_path)
    # if not osp.isdir( './runs' ):
    #     os.mkdir('./runs')
    #     print("making run directory inside launch")
    # os.chdir('./runs')

    # base_config = configDict['base']
    # particular_options_list = configDict['particular']
    # default_keys = nested_dict_locate_field(
    #     base_config, lambda x: isinstance(x, Mapping) and 'default' in x
    # )


if __name__ == '__main__':
    main()
