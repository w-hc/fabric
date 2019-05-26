import os
import os.path as osp
import argparse
from copy import deepcopy
from collections import namedtuple
import yaml
from termcolor import cprint


# '_' prefix so that it is sorted to the top when dumping yaml
_INFO_COLOR = 'blue'
_WARN_COLOR = 'red'

_META_FIELD_NAME = '__meta__'

_LAUNCH_FIELDS_SPEC = {
    'required': ['group', 'particular'],
    'optional': ['base_modify', 'desc'],
    'either': ['import_base', 'base']  # one of the field must be present
}

_PARTICULAR_FIELDS_SPEC = {
    'required': ['name'],
    'optional': ['modify', 'expand'],
    'either': []
}

for _field_spec in (_LAUNCH_FIELDS_SPEC, _PARTICULAR_FIELDS_SPEC):
    _field_spec['all'] = _field_spec['required'] + _field_spec['optional'] \
        + _field_spec['either']


def main():
    parser = argparse.ArgumentParser(description='deploy the experiments')
    parser.add_argument(
        '-f', '--file', type=str, required=True,
        help='a yaml based on our convention describing the experiments to run'
    )
    parser.add_argument(
        '-d', '--dir', type=str, default='runs',
        help='the directory in which to plant configs'
    )
    parser.add_argument(
        '-l', '--log', type=str, default='touched_exps.yml',
        help='a yml containing a list of abspaths to touched exps'
    )
    parser.add_argument(
        '--overwrite', action='store_true',
        help='in case of duplicate exp names, overwrite their options'
    )
    parser.add_argument(
        '-m', '--mock', nargs='*', type=str, required=False,
        help='specify a list of configs to be printed in full details'
    )
    args = parser.parse_args()

    LAUNCH_FNAME = args.file
    LAUNCH_DIR_ABSPATH = osp.dirname(osp.abspath(LAUNCH_FNAME))
    RUN_DIR_NAME = args.dir
    SOW_LOG_FNAME = osp.join(LAUNCH_DIR_ABSPATH, args.log)
    with open(LAUNCH_FNAME, 'r') as f:
        launch_config = yaml.safe_load(f)

    # parse the config
    # chdir first. cfg import might assume relpath from launch dir
    # this statement must come after reading launch_cfg
    os.chdir(LAUNCH_DIR_ABSPATH)
    group_name, cfg_name_2_maker = parse_launch_config(launch_config)

    # if mocking, print requested configs and quit
    if args.mock is not None:
        to_display = args.mock if len(args.mock) > 0 else cfg_name_2_maker.keys()
        for i, exp_name in enumerate(to_display):
            maker = cfg_name_2_maker[exp_name]
            cprint('{}: {}'.format(i, exp_name), color=_INFO_COLOR)
            print(yaml.dump(maker.state, default_flow_style=False))
        return

    # sow the cfgs
    # 1. check that group namd and launch dir name match
    dir_name = LAUNCH_DIR_ABSPATH.split('/')[-1]
    assert dir_name == group_name, \
        "group name: {}, but launch dir name: {}. Match them"\
        .format(group_name, dir_name)
    del dir_name

    # 2. create exp folder and plant configs
    if not osp.isdir(RUN_DIR_NAME):
        os.mkdir(RUN_DIR_NAME)
        print("making {} inside launch".format(RUN_DIR_NAME))
    os.chdir(RUN_DIR_NAME)

    sow_acc = []
    for i, (exp_name, maker) in enumerate(cfg_name_2_maker.items()):
        maker.state[_META_FIELD_NAME] = {
            'group': group_name,
            'name': exp_name
        }
        cprint("sowing {}: {}".format(i, exp_name), color=_INFO_COLOR)
        success = plant_files(
            LAUNCH_DIR_ABSPATH, exp_name, maker.state, overwrite=args.overwrite)
        if success:
            sow_acc.append(osp.abspath(exp_name))

    # 3. save a log file for other utils to use
    with open(SOW_LOG_FNAME, 'w') as f:
        yaml.dump(sow_acc, f, default_flow_style=False)


def parse_launch_config(launch_config):
    validate_dict_fields(launch_config, _LAUNCH_FIELDS_SPEC)
    acc = {}
    group_name = launch_config['group']

    # import pudb
    # pudb.set_trace()

    # construct base config through import or from 'base'
    if 'import_base' in launch_config and launch_config['import_base']:
        import_path = launch_config['import_base']
        assert 'base' not in launch_config or not launch_config['base'],\
            'importing from {}, don\'t supply base config'.format(import_path)
        with open(import_path, 'r') as f:
            base_maker = ConfigMaker(yaml.safe_load(f))
    else:
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

    return group_name, acc


def validate_dict_fields(src_dict, field_spec):
    assert isinstance(src_dict, dict)
    # check required fields are present
    for k in field_spec['required']:
        assert k in src_dict and src_dict[k], \
            "field '{}' is required and must be truthy. Given {}".format(
                k, src_dict.keys()
        )
    # check 'or' fields are present
    present = False or len(field_spec['either']) == 0
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
            raise ValueError("field '{}' is not allowed among {}".format(
                k, field_spec['all']
            ))


def dfs_expand(level, name, maker, deposit, grids):
    if level == len(grids):
        name = name[:-1]  # throw away the '_' at the end of say 'lr_lo_'
        deposit[name] = maker.clone()
        return

    tier = grids[level]
    iter_clauses = list(tier.keys())
    size = len(tier[iter_clauses[0]])
    for clause in iter_clauses:
        # broadcast config if of length 1
        if len(tier[clause]) == 1 and clause != 'alias':
            tier[clause] = tier[clause] * size
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


class ConfigMaker():
    CMD = namedtuple('cmd', field_names=['sub', 'verb', 'objs'])
    VERBS = ('add', 'replace', 'del')

    def __init__(self, base_node):
        self.state = base_node
        self.clauses = []

    def clone(self):
        return deepcopy(self)

    def execute_clause(self, raw_clause):
        cls = self.__class__
        assert isinstance(raw_clause, (str, dict))
        if isinstance(raw_clause, dict):
            assert len(raw_clause) == 1, \
                "a clause can only have 1 statement: {} clauses in {}".format(
                    len(raw_clause), raw_clause
            )
            cmd = list(raw_clause.keys())[0]
            arg = raw_clause[cmd]
        else:
            cmd = raw_clause
            arg = None
        cmd = self.parse_clause_cmd(cmd)
        tracer = NodeTracer(self.state)
        tracer.advance_pointer(path=cmd.sub)
        if cmd.verb == cls.VERBS[0]:
            tracer.add(cmd.objs, arg)
        elif cmd.verb == cls.VERBS[1]:
            tracer.replace(cmd.objs, arg)
        elif cmd.verb == cls.VERBS[2]:
            assert isinstance(raw_clause, str)
            tracer.delete(cmd.objs)
        self.state = tracer.state

    @classmethod
    def parse_clause_cmd(cls, input):
        """
        Args:
            input: a string to be parsed
        1. First test whether a verb is present
        2. If not present, then str is a single subject, and verb is replace
           This is a syntactical sugar that makes writing config easy
        3. If a verb is found, whatever comes before is a subject, and after the
           objects.
        4. Handle the edge cases properly. Below are expected parse outputs
        input       sub     verb        obj
        --- No verb
        ''          ''      replace     []
        'a.b'       'a.b'   replace     []
        'add'       ''      add         []
        'P Q' err: 2 subjects
        --- Verb present
        'T add'     'T'     add         []
        'T del a b' 'T'     del         [a, b]
        'P Q add a' err: 2 subjects
        'P add del b' err: 2 verbs
        """
        assert isinstance(input, str)
        input = input.split()
        objs = []
        sub = ''
        verb, verb_inx = cls.scan_for_verb(input)
        if verb is None:
            assert len(input) <= 1, "no verb present; more than 1 subject: {}"\
                .format(input)
            sub = input[0] if len(input) == 1 else ''
            verb = cls.VERBS[1]
        else:
            assert not verb_inx > 1, 'verb {} at inx {}; more than 1 subject in: {}'\
                .format(verb, verb_inx, input)
            sub = input[0] if verb_inx == 1 else ''
            objs = input[verb_inx + 1:]
        cmd = cls.CMD(sub=sub, verb=verb, objs=objs)
        return cmd

    @classmethod
    def scan_for_verb(cls, input_list):
        assert isinstance(input_list, list)
        counts = [ input_list.count(v) for v in cls.VERBS ]
        presence = [ cnt > 0 for cnt in counts ]
        if sum(presence) == 0:
            return None, -1
        elif sum(presence) > 1:
            raise ValueError("multiple verbs discovered in {}".format(input_list))

        if max(counts) > 1:
            raise ValueError("verbs repeated in cmd: {}".format(input_list))
        # by now, there is 1 verb that has occured exactly 1 time
        verb = cls.VERBS[presence.index(1)]
        inx = input_list.index(verb)
        return verb, inx


class NodeTracer():
    def __init__(self, src_node):
        """
        A src node can be either a list or dict
        """
        assert isinstance(src_node, (list, dict))

        # these are movable pointers
        self.child_token = "_"  # init token can be anything
        self.parent = {self.child_token: src_node}

        # these are permanent pointers at the root
        self.root_child_token = self.child_token
        self.root = self.parent

    @property
    def state(self):
        return self.root[self.root_child_token]

    @property
    def pointed(self):
        return self.parent[self.child_token]

    def advance_pointer(self, path):
        if len(path) == 0:
            return
        path_list = list(
            map(lambda x: int(x) if str.isdigit(x) else x, path.split('.'))
        )

        for i, token in enumerate(path_list):
            self.parent = self.pointed
            self.child_token = token
            try:
                self.pointed
            except (IndexError, KeyError):
                raise ValueError(
                    "During the tracing of {}, {}-th token '{}'"
                    " is not present in node {}".format(
                        path, i, self.child_token, self.state
                    )
                )

    def add(self, objs, arg):
        assert len(objs) <= 1, 'add deals with 1 obj everytime: {}'.format(objs)
        if len(objs) == 1:
            field = objs[0]
            if isinstance(self.pointed, list):
                assert str.isdigit(field)
                field = int(field)
                self.pointed.insert(field, arg)
            elif isinstance(self.pointed, dict):
                assert field not in self.pointed, 'field {} present in {}'\
                    .format(field, self.pointed)
                self.pointed[field] = arg
            else:
                raise ValueError("{} is not a container node".format(self.pointed))
        else:
            assert isinstance(self.pointed, dict),\
                '{} is not a dict, cannot use bare add'.format(self.pointed)
            assert isinstance(arg, dict), 'expect dict when adding into dict'
            for k in arg.keys():
                assert k not in self.pointed, \
                    '{} already present in {}'.format(k, self.pointed)
            self.pointed.update(arg)

    def replace(self, objs, arg):
        assert len(objs) <= 1, 'replace deals with 1 obj everytime: {}'.format(objs)
        if len(objs) == 1:
            field = objs[0]
            if isinstance(self.pointed, list):
                assert str.isdigit(field)
                field = int(field)
                self.pointed[field] = arg
            elif isinstance(self.pointed, dict):
                assert field in self.pointed, 'field {} not present in {}'\
                    .format(field, self.pointed)
                self.pointed[field] = arg
            else:
                raise ValueError("{} is not a container node".format(self.pointed))
        else:
            self.parent[self.child_token] = arg

    def delete(self, objs):
        if isinstance(self.pointed, list):
            objs = map(lambda x: int(x), objs)
        elif isinstance(self.pointed, dict):
            pass
        else:
            raise ValueError("{} is not a container node".format(self.pointed))
        for field in objs:
            del self.pointed[field]


def plant_files(launch_dir, exp_name, cfg_node, overwrite):
    '''plant the config and related files (run.py)
    Args:
        launch_dir: abspath! of launch directory from which run.py is copied
        exp_name: the bare name of experiment folder in which things are dumped
    '''
    cfg_fname = osp.join(exp_name, 'config.yml')
    if osp.isdir(exp_name):  # duplicate exists
        # 1. compare whether the options are identical
        with open(cfg_fname, 'r') as f:
            old_cfg = yaml.safe_load(f)
        if old_cfg == cfg_node:
            print("dup identical", end='; ')
        else:
            cprint("dup differs: {}".format(exp_name), color=_WARN_COLOR, end='; ')
        if overwrite:
            print("overwriting")
            pass
        else:
            print("skipping")
            return False
    else:
        os.mkdir(exp_name)

    with open(cfg_fname, 'w') as f:
        yaml.dump(cfg_node, f, default_flow_style=False)
    to_link_over = ('run.py',)
    for item in to_link_over:
        if osp.isfile(osp.join(launch_dir, item)):
            # shutil.copy(osp.join(launch_dir, item), exp_name)
            src = osp.join(launch_dir, item)
            target = osp.join(exp_name, item)
            if osp.islink(target):
                os.unlink(target)
            elif osp.isfile(target):
                os.remove(target)
            # note that we use relative path for maintainability.
            os.symlink(osp.relpath(src, osp.abspath(exp_name)), target)
    return True


if __name__ == '__main__':
    main()
