'''
This module is useful to query experiment results over nested configs
'''
from typing import Tuple, Dict, Any

__all__ = ['query_by_key']


def assert_valid_tuple_dict(src_dict: Dict[Tuple, Any]):
    if len(src_dict) == 0:
        return
    keys = src_dict.keys()
    key_lengths = { len(k) for k in keys }
    assert len(key_lengths) == 1, 'keys are not of the same length'
    for k in keys:
        assert None not in k, 'all tuple elements must be valid, got {}'.format(k)


def compare_tuples(left: Tuple, query: Tuple):
    assert len(left) == len(query)
    skipped_keys = []
    for l_k, q_k in zip(left, query):
        if q_k is None:
            skipped_keys.append(l_k)
            continue
        if l_k != q_k:
            return False, None
    return True, tuple(skipped_keys)


def query_by_key(src_dict: Dict[Tuple, Any], query_key: Tuple):
    assert_valid_tuple_dict(src_dict)
    chosen = []
    for k, v in src_dict.items():
        is_same, skipped = compare_tuples(k, query_key)
        if is_same:
            chosen.append((skipped, v))
    keys, vals = tuple(zip(*chosen))
    return keys, vals
