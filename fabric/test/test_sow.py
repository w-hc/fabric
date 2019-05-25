import pytest
from pudb import set_trace

import yaml
from fabric.cluster.sow import (
    parse_launch_config, ConfigMaker
)

a1 = """
group: test_fabric
base:
    train:
        epochs: 40
        lr: 0.1
        fun: 1
base_modify:
  - train.epochs: 50
  - train.lr: 0.2

particular:
  - name: exp1
    modify:
      - train.epochs: 70
    expand:
      - alias: [slow, medium, fast]
        train.lr: [0.5, 0.6, 0.7]
      - alias: [lazy, hard]
        train.fun: [2, 3]
"""

"""
 del train
"""

# make base a list. No assumption on what it could be!!!!
"""
group: testing
base:
    - train:
        epochs: 40
        lr: 0.1
        fun: 1
base_modify:
    - train.epochs: 50
    - train.lr: 0.2

particular:
  - name: exp1
    modify:
      - train.epochs: 70
    exp:
      - alias: [slow, medium, fast]
        train.lr: [0.5, 0.6, 0.7]
      - alias: [lazy, hard]
        train.fun: [2, 3]
"""


def parse_yml_str(x):
    return yaml.safe_load(x)


def test_dummy():
    # set_trace()
    cfg = parse_yml_str(a1)
    gname, acc = parse_launch_config(cfg)
    for name, maker in acc.items():
        print(name)
        print(maker.state)
    assert len(acc) == 6


def test_verb_scanning():
    a = 'sd add xc ak ks'.split()
    b = 's a s d ds'.split()
    c = 'a replace add sdf'.split()
    d = 'a replace replace replace d'.split()
    verb, inx = ConfigMaker.scan_for_verb(a)
    assert verb == 'add' and inx == 1

    verb, inx = ConfigMaker.scan_for_verb(b)
    assert verb is None and inx == -1

    a = 'T del a b'.split()
    verb, inx = ConfigMaker.scan_for_verb(a)
    assert verb == 'del' and inx == 1

    with pytest.raises(ValueError, match='multiple'):
        ConfigMaker.scan_for_verb(c)

    with pytest.raises(ValueError, match='repeated'):
        ConfigMaker.scan_for_verb(d)


def test_cmd_parsing():
    cmd = ConfigMaker.CMD
    cmd(sub='', verb='replace', objs=[])
    test_pairs = [
        ('',  cmd(sub='', verb='replace', objs=[])),
        (' ', cmd(sub='', verb='replace', objs=[])),
        ('add', cmd(sub='', verb='add', objs=[])),
        ('a.b', cmd(sub='a.b', verb='replace', objs=[])),
        (' a.b ', cmd(sub='a.b', verb='replace', objs=[])),
        ('a.b c.d', 'more than 1 subject'),
        ('T add ', cmd(sub='T', verb='add', objs=[]) ),
        ('T del a b', cmd(sub='T', verb='del', objs=['a', 'b'])),
        ('P Q add a', 'more than 1 subject'),
        ('P Q addd a', 'more than 1 subject'),  # wrong spelling
    ]
    for i, (input, expected) in enumerate(test_pairs):
        if isinstance(expected, cmd):
            output = ConfigMaker.parse_clause_cmd(input)
            assert output == expected, 'test case {} failed'.format(i)
        elif isinstance(expected, str):  # match for exceptions
            with pytest.raises(AssertionError, match=expected):
                ConfigMaker.parse_clause_cmd(input)


sample1 = """
a:
    b: 3
"""


def test_cfg_parsing1():
    cfg = yaml.safe_load(sample1)
    primary = ConfigMaker(cfg)

    pairs = [
        ( {'': {'m': 2}}, {'m': 2} ),
        ( {'a.b': 2}, {'a': {'b': 2}} ),
        ( {'a add c': 2}, {'a': {'b': 3, 'c': 2}} ),
        ( {'a add ': {'c': 2}}, {'a': {'b': 3, 'c': 2}} ),
        ( ' del a', {} ),
        ( ' del', {'a': {'b': 3}} )
    ]

    for i, (clause, expected) in enumerate(pairs):
        maker = primary.clone()
        if not isinstance(expected, str):
            maker.execute_clause(clause)
            assert maker.state == expected, 'err at clause {}'.format(i)
        else:
            try:
                with pytest.raises(AssertionError, match=expected):
                    maker.execute_clause(clause)
            except:
                with pytest.raises(ValueError, match=expected):
                    maker.execute_clause(clause)


sample2 = """
- a: 1
- b:
    c: 2
"""


def test_cfg_parsing2():
    cfg = yaml.safe_load(sample2)
    primary = ConfigMaker(cfg)
    pairs = [
        ( {'': {'m': 2}}, {'m': 2} ),
        ( {'0': 9}, [9, {'b': {'c': 2}}] ),
        ( {'0.a': 7}, [{'a': 7}, {'b': {'c': 2}}] ),
        ( {'1.b.c': 7}, [{'a': 1}, {'b': {'c': 7}}] ),
        ( 'del 1', [{'a': 1}, ] ),
        ( 'del -1', [{'a': 1}, ] ),
        ( {'1.b.c add': 3}, 'not a dict' ),
        ( {'1.b.c add 2': 3}, 'not a container' ),
        ( {'1.b.c add 2 3 4': 3}, '1 obj everytime' ),
    ]

    for i, (clause, expected) in enumerate(pairs):
        maker = primary.clone()
        if not isinstance(expected, str):
            maker.execute_clause(clause)
            assert maker.state == expected, 'err at clause {}'.format(i)
        else:
            try:
                with pytest.raises(AssertionError, match=expected):
                    maker.execute_clause(clause)
            except:
                with pytest.raises(ValueError, match=expected):
                    maker.execute_clause(clause)
