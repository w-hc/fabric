import argparse
import os
import os.path as osp
import yaml
from collections import Sequence
from collections import defaultdict
import pandas as pd

from ..algo.nested_dict import (
    nested_dict_touch,
    nested_dict_values_extract
)


linkable = ['.', 'logging', 'checkpoint', 'output']


def _is_typical_sequence(obj):
    '''
    Regrettably, str is a sequence. But for common purposes it is an integral value
    '''
    return isinstance(obj, Sequence) and not isinstance(obj, str)


def recursive_node_splitting(acc_dict, remaining_keys, candidate_list):
    '''
    leaf struct {'leaf': [cfg1, cfg2, ...]}
    '''
    if len(remaining_keys) == 0:
        # attach the candidate list
        acc_dict['leaf'] = candidate_list
        return acc_dict
    curr_split_key = remaining_keys[0]
    val_dict = defaultdict(list)  # different val of the current key.
    for candidate in candidate_list:
        try:
            val = nested_dict_touch(candidate, curr_split_key)
        except KeyError:
            val = None
        # the only sequence object hashable is tuple. Note that str is a seq
        if _is_typical_sequence(val):
            val = tuple(val)
        val_dict[val].append(candidate)
    for val, candidates in val_dict.items():
        acc_dict[val] = {}
        recursive_node_splitting(acc_dict[val], remaining_keys[1:], candidates)
    return acc_dict


def leaf_extractor(leaf):
    # take the group name and index of each cfg
    ret = list( map(lambda cfg: "{meta[group]}/{meta[index]}".format(meta=cfg['_meta']),
                    leaf['leaf']) )
    if len(ret) == 1:
        return ret[0]
    else:
        return ret


def bloom(dir_abs_path, rank_file):
    with open(rank_file, 'r') as f:
        opt_rank = yaml.load(f)

    acc_configs = []
    for exp in os.listdir(dir_abs_path):
        f_name = osp.join(dir_abs_path, exp, 'config.yml')
        with open(f_name, 'r') as f:
            acc_configs.append(yaml.load(f))

    # note nested_dict_values_extract returns a list-packaged result. get with [0]
    l_keys = list( map(lambda x: nested_dict_values_extract(x)[0], opt_rank) )
    split_tree = recursive_node_splitting(dict(), l_keys, acc_configs)
    return l_keys, split_tree


def bloom_into_df(dir_abs_path, rank_file):
    backbone_keys, tree = bloom(dir_abs_path, rank_file)
    res = nested_dict_values_extract(
        tree,
        halt_predicate=lambda x: 'leaf' in x,
        extract_predicate=leaf_extractor
    )
    res = sorted(res, reverse=True)

    df = pd.DataFrame.from_records(
        res, index=None,
        # pick the last of each config tuple as col name (train, lr, decay) -> decay
        columns=list(map(lambda x: x[-1], backbone_keys)) + ['experiment', ]
    )
    return df


def bloom_into_dir(dir_abs_path, rank_file, target_dir, what_to_link='.'):
    '''
    This rank file must be able to split the node into single element.
    '''
    if what_to_link not in linkable:
        raise ValueError("Only {} linkable, given {}".format(linkable, what_to_link))

    _, tree = bloom(dir_abs_path, rank_file)
    res = nested_dict_values_extract(
        tree,
        halt_predicate=lambda x: 'leaf' in x,
        extract_predicate=leaf_extractor
    )

    for root_to_leaf in res:
        configs = list(root_to_leaf[:-1])
        group_and_index_name = root_to_leaf[-1]
        if _is_typical_sequence(group_and_index_name):
            raise ValueError("the node must be split down to single element, given {}"
                             .format(group_and_index_name))
        print(
            "sym-linking {}".format(
                osp.normpath(osp.join(group_and_index_name, what_to_link))
            )
        )
        index_name = group_and_index_name.split('/')[-1]
        exp_abs_path = osp.join( dir_abs_path, index_name )
        sym_dir_path = osp.join( target_dir, "\\".join( list(map(str, configs)) ) )

        link_src = osp.join( exp_abs_path, what_to_link )
        del exp_abs_path
        os.symlink(link_src, sym_dir_path)


def main():
    parser = argparse.ArgumentParser(description='bloom the flowers')
    parser.add_argument('-d', '--dir', type=str, required=True,
                        help='the directory of the group of experiments to bloom')
    parser.add_argument('-r', '--rank', type=str,
                        help='The option ranking guidance file')
    parser.add_argument('-t', '--target', type=str,
                        help='relative osp to the target directory at which one '
                             'wants to bloom the symbolic links')
    parser.add_argument('-w', '--what', type=str, default='.',
                        help='what to link from exp dir. One of {}'.format(linkable))
    args = parser.parse_args()

    dir_name = args.dir
    rank_file = args.rank
    target_dir = args.target if args.target else ''
    target_dir = osp.join( os.getcwd(), target_dir )

    dir_abs_path = osp.abspath(dir_name)
    exp_group_name = osp.split(dir_abs_path)[-1]
    print(dir_abs_path, exp_group_name)
    if not rank_file:
        # I assume a certain hierarchy layout.
        rank_file = osp.normpath( osp.join(dir_abs_path,
                                             '../../launch/{}/opt_rank.yml'.format(exp_group_name)) )

    bloom_into_dir(dir_abs_path, rank_file, target_dir, args.what)
