import argparse
import yaml
import os
import os.path as osp
import shutil
import itertools
from copy import deepcopy
from collections import Mapping
from fabric.algo.nested_dict import (
    nested_dict_touch,
    nested_dict_locate_field,
    nested_dict_merge
)


meta_naming = '_meta'  # '_' prefix so that it is sorted to the top when dumping yaml


def clean_defaults(src_dict, list_default_keys, set_none=False):
    '''
    return a deepcopy of src_dict with default fields cleaned.
    set all the nested fields keyed by 'default' to actual values
    if set_none, then set it to none, for the purpose of comparison
    else set it to the contained default value.
    This helps to resolve non-crucial optional param like batch size.
    This doc is horrible and I cry for my writing.
    '''
    src_dict = deepcopy(src_dict)
    for l_key in list_default_keys:
        if set_none:
            nested_dict_touch(src_dict, l_key, None)
        else:
            # could be a dict if not over-written by merge, or a simple val after merge
            _val = nested_dict_touch(src_dict, l_key)
            default_val = _val['default'] if isinstance(_val, Mapping) else _val
            nested_dict_touch(src_dict, l_key, default_val)
    return src_dict


def check_duplicate(target, default_keys):
    '''
    default keys are non-critical, and their fields should be set to None before comparison
    '''
    dirs_list = os.listdir()
    cleaned_target = clean_defaults(target, default_keys, set_none=True)
    for exp_dir in dirs_list:
        with open(osp.join(exp_dir, 'config.yml')) as f:
            exp_cfg = yaml.load(f)
        index = exp_cfg[meta_naming]['index']
        del exp_cfg[meta_naming]
        cleaned_exp_cfg = clean_defaults(exp_cfg, default_keys, set_none=True)
        if (cleaned_exp_cfg == cleaned_target):
            return index
    return None


def create_dir():
    '''
    create a new directory by incrementing the largest of dir_index
    '''
    existing_dirs = os.listdir()
    dir_index = 1 + (max(map(int, existing_dirs)) if len(existing_dirs) > 0 else 0)
    dir_name = '{:0>3}'.format(dir_index)
    print('planting {}'.format(dir_name))
    os.mkdir(dir_name)
    return dir_name


def plant_files(dir_name, config, launch_dir):
    '''
    plant the config and related files (run.py eval.ipynb) into the specified directory
    If these files already exist, compare edit timestamp and only copy if outdated.
    For simplicity, this bahavior is not implemented. Just copy
    '''
    with open(osp.join(dir_name, 'config.yml'), 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
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
    if osp.isfile( osp.join(launch_dir, 'eval.ipynb') ):
        shutil.copy(osp.join(launch_dir, 'eval.ipynb'), dir_name)


def main():
    parser = argparse.ArgumentParser(description='deploy the experiments')
    parser.add_argument('-f', '--file', type=str, required=True,
                        help='a yaml based on our convention describing the experiments to run')
    # parser.add_argument('--overwrite',
    #                     help='do not compare for duplicates and directly overwrite',
    #                     action='store_true')
    args = parser.parse_args()

    target_file = args.file
    launch_dir_abs_path = osp.dirname( osp.abspath(target_file) )

    with open(target_file, 'r') as f:
        configDict = yaml.load(f)

    # check the launch dir has consistent naming on group
    if 'group' not in configDict:
        raise ValueError("group configuration requires a name")

    dir_name = launch_dir_abs_path.split('/')[-1]
    if dir_name != configDict['group']:
        raise ValueError("config group name: {}, but launch dir name: {}. Match them"
                         .format(configDict['group'], dir_name))
    del dir_name

    # go into runs/exp_group_name/
    os.chdir(launch_dir_abs_path)
    if not osp.isdir( './runs' ):
        os.mkdir('./runs')
        print("making run directory inside launch")
    os.chdir('./runs')

    # This is bad code and confusing. You will forget quickly
    # if not osp.isdir( '../../runs' ):
    #     os.mkdir('../../runs')
    #     print("making run directory beside launch")
    # os.chdir('../../runs')

    # if osp.isdir(configDict['group']):
    #     print('within runs, dir: {} already exists'.format(configDict['group']))
    # else:
    #     os.mkdir(configDict['group'])
    #     print('within runs, make dir: {}'.format(configDict['group']))
    # os.chdir(configDict['group'])

    base_config = configDict['base']
    particular_options_list = configDict['particular']
    default_keys = nested_dict_locate_field(
        base_config, lambda x: isinstance(x, Mapping) and 'default' in x
    )

    touched_dirs_acc = []
    for parti_opt in particular_options_list:
        # stop at the first encountering of a list value.
        grid_keys = nested_dict_locate_field(parti_opt, lambda x: isinstance(x, list))
        # each grid_value is a list of configurations e.g. [0.2, 0.4, 0.6]
        grid_values = [ nested_dict_touch(parti_opt, l_key) for l_key in grid_keys ]
        merged = nested_dict_merge(base_config, parti_opt)
        for config in itertools.product(*grid_values):
            # config is a list of to-update values, corresponding to the grid_keys
            merged_cpy = deepcopy(merged)
            for i, l_key in enumerate(grid_keys):
                nested_dict_touch(merged_cpy, l_key, config[i])
            del config  # prevent mistaken re-use

            indexed_dir_name = check_duplicate(merged_cpy, default_keys)
            # if the dir does not already exist, create it.
            if indexed_dir_name:
                print('duplicate found in : {}'.format(indexed_dir_name))
            else:
                indexed_dir_name = create_dir()
            touched_dirs_acc.append( osp.abspath(indexed_dir_name) )

            # now plant config, run.py, eval notebook in the dir.
            merged_cpy[meta_naming] = {}
            merged_cpy[meta_naming]['project'] = configDict['project']
            merged_cpy[meta_naming]['group'] = configDict['group']
            merged_cpy[meta_naming]['index'] = indexed_dir_name
            merged_cpy = clean_defaults(merged_cpy, default_keys, set_none=False)
            plant_files(indexed_dir_name, merged_cpy, launch_dir_abs_path)

    # save the touched files into a list
    with open(osp.join(launch_dir_abs_path, 'touched_exps.yml'), 'w') as f:
        yaml.dump(touched_dirs_acc, f, default_flow_style=False)


if __name__ == '__main__':
    main()
