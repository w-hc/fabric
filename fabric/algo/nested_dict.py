from collections import Mapping
from copy import deepcopy

"""
these are good util methods
You can write some tests for these functions since they are so commonly used
"""


def nested_dict_touch(src_dict, key_list, new_val=()):
    '''
    return the old val and set the new val.
    if new_val is () default, then only return the old val. don't set.
    since None is a valid value to be set, I use empty tuple to signal read
    intention.
    I assume no one wants to set any option to empty tuple.
    '''
    if len(key_list) == 0:
        raise ValueError("key_list cannot be empty")
    if len(key_list) == 1:
        old_val = src_dict[key_list[0]]
        if not new_val == ():
            src_dict[key_list[0]] = new_val
        return old_val
    else:
        return nested_dict_touch(src_dict[key_list[0]], key_list[1:], new_val)


def nested_dict_values_extract(src_dict,
                               halt_predicate=lambda x: False,
                               extract_predicate=lambda x: x):
    '''
    Use back-tracking to generate all the root-to-leaf traces of the nested
    dict to a list of tuple.
    halt_predicate allows early stopping of back-tracking.
    Upon halting (either due to reaching leaf, or half_pred activates),
    use extrac_predicate to get val.
    '''
    ret = []

    def gather_dict(target, acc):
        # halt if not a dict or halt_predicate activates
        if (not isinstance(target, Mapping)) or halt_predicate(target):
            acc.append( extract_predicate(target) )
            ret.append( tuple(acc) )
            acc.pop()
            return
        else:
            for k, v in target.items():
                acc.append(k)
                gather_dict(v, acc)
                acc.pop()

    gather_dict(src_dict, list())
    return ret


def nested_dict_locate_field(src_dict, predicate):
    '''
    return a list of all paths in the nested dict that satisfy the predicate. (2-dim)
    '''
    acc = []

    def search_down_dict(target, stack):
        if predicate(target):
            acc.append(stack.copy())
        if isinstance(target, Mapping):
            for k, v in target.items():
                stack.append(k)
                search_down_dict(v, stack)
                stack.pop()

    search_down_dict(src_dict, list())
    return acc


def nested_dict_merge(acc, add):
    '''
    :param acc: the accumulating dictionary
    :param add: the new dict to be merged into acc
    :return: a deep copy of updated acc
    # note that extra fields which only 'add' has will be ignored.
    # this merge walks down acc and find corresponding fields in add.
    '''
    acc = deepcopy(acc)
    if add is None:
        return acc
    for k, v in add.items():
        if k in acc and \
                isinstance(acc[k], Mapping) and isinstance(add[k], Mapping):
            acc[k] = nested_dict_merge(acc[k], add[k])
        else:
            acc[k] = add[k]
    return acc
