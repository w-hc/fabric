import os
import os.path as osp
import argparse
from copy import deepcopy
from collections import namedtuple
import yaml
from termcolor import cprint
from .. import yaml_read

# '_' prefix so that it is sorted to the top when dumping yaml
_INFO_COLOR = 'blue'
_WARN_COLOR = 'red'


_LAUNCH_FIELDS_SPEC = {
    'required': ['particular'],
    'optional': ['base_modify', 'desc', 'group'],
    'either': ['import_base', 'base']  # one of the field must be present
}

_PARTICULAR_FIELDS_SPEC = {
    'required': ['name'],  # why is it required?
    'optional': ['modify', 'expand'],
    'either': []
}

for _field_spec in (_LAUNCH_FIELDS_SPEC, _PARTICULAR_FIELDS_SPEC):
    _field_spec['all'] = _field_spec['required'] + _field_spec['optional'] \
        + _field_spec['either']


def join_parts_into_path(parts, nest_at):
    if nest_at is None:
        return '_'.join(parts)
    else:
        assert isinstance(nest_at, int) and nest_at >= 0
        nest_at += 1  # skip the initial lead name
        path = ""
        for i, e in enumerate(parts):
            if i == nest_at:
                path += f"{e}/"
            else:
                path += f"{e}_"
        path = path[:-1]  # remove the tail _ or /
        return path


def main():
    parser = argparse.ArgumentParser(description='deploy the experiments')
    parser.add_argument(
        '-f', '--file', type=str, required=True,
        help='a yaml based on our convention describing the experiments to run'
    )

    # parser.add_argument(
    #     '-d', '--dir', type=str, default='runs',
    #     help='the directory in which to plant configs'
    # )
    # parser.add_argument(
    #     '-l', '--log', type=str, default='touched_exps.yml',
    #     help='a yml containing a list of abspaths to touched exps'
    # )
    # HC: a strong, opinionated default to use runs_{k}/ and exps_{k}.yml
    # directory listing groups things together for easy visual tracking
    parser.add_argument(
        '-k', '--key', type=str, required=True,
        help='key used to name the runs directory and log file'
    )

    parser.add_argument(
        '--nest_at', type=int, required=False, default=None,
        help='the default dirname a_b_c can result in lots of subdirs. If nest_at 0, then a/b_c. If nest_at 1, then a_b/c'
    )

    parser.add_argument(
        '--repeat', type=int, default=0,
        help='repeat each exp k times'
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
    LAUNCH_DIR_ABSPATH = os.getcwd()
    RUN_DIR_NAME = f"runs_{args.key}"
    SOW_LOG_FNAME = osp.join(LAUNCH_DIR_ABSPATH, f"exps_{args.key}.yml")
    launch_config = yaml_read(LAUNCH_FNAME)

    # parse the config
    # chdir first. cfg import might assume relpath from template file
    # this statement must come after reading launch_cfg
    os.chdir(osp.dirname(osp.abspath(LAUNCH_FNAME)))
    cfg_name_2_maker = parse_launch_config(launch_config)
    cfg_name_2_maker = {
        join_parts_into_path(k, args.nest_at): v
        for k, v in cfg_name_2_maker.items()
    }

    os.chdir(LAUNCH_DIR_ABSPATH)

    # if mocking, print requested configs and quit
    if args.mock is not None:
        to_display = args.mock if len(args.mock) > 0 else cfg_name_2_maker.keys()
        for i, exp_name in enumerate(to_display):
            maker = cfg_name_2_maker[exp_name]
            cprint('{}: {}'.format(i, exp_name), color=_INFO_COLOR)
            print(_yaml_dump(maker.state))
        return

    # sow the cfgs

    # 1. create the runs folder, chdir, and plant configs inside
    if not osp.isdir(RUN_DIR_NAME):
        os.mkdir(RUN_DIR_NAME)
        print(f"making {RUN_DIR_NAME} inside launch")
    os.chdir(RUN_DIR_NAME)

    sow_acc = []
    for i, (exp_name, maker) in enumerate(cfg_name_2_maker.items()):
        cprint("sowing {}: {}".format(i, exp_name), color=_INFO_COLOR)
        _paths = plant_cfg(exp_name, maker.state, overwrite=args.overwrite, repeat=args.repeat)
        sow_acc.extend([osp.abspath(e) for e in _paths])

    # 2. save a log file for other utils to use
    with open(SOW_LOG_FNAME, 'w') as f:
        pl = _yaml_dump(sow_acc)
        f.write(pl)


def parse_launch_config(launch_config):
    validate_dict_fields(launch_config, _LAUNCH_FIELDS_SPEC)

    acc = {}
    # construct base config through import or from 'base'
    if 'import_base' in launch_config:
        assert 'base' not in launch_config, \
            'using imported base config; do not supply base config'
        import_path = launch_config['import_base']
        base_maker = ConfigMaker(yaml_read(import_path))
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
            assert isinstance(clauses, list)
            for _clau in clauses:
                curr_maker.execute_clause(_clau)
        # expand the field
        if 'expand' in part:
            assert isinstance(part['expand'], list)
            dfs_expand(
                level=0, namelist=[part_name],
                maker=curr_maker.clone(), deposit=acc, grids=part['expand']
            )
        else:
            acc[part_name] = curr_maker

    return acc


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


def dfs_expand(level, namelist, maker, deposit, grids):
    if level == len(grids):
        k = tuple(namelist)
        deposit[k] = maker.clone()
        return

    tier = grids[level]

    # tier can be either dict[list], or list[dict]!

    # tier: dict[list]
    if isinstance(tier, dict):
        _keys = list(tier.keys())
        size = max(len(tier[k]) for k in _keys)

        for k in _keys:
            # broadcast config if of length 1; alias is not broadcastable; must be full-size.
            if len(tier[k]) == 1 and k != 'alias':
                tier[k] = tier[k] * size
            assert len(tier[k]) == size

        if 'alias' in tier:
            alias = tier['alias']
            _keys.remove('alias')
        else:
            alias = range(size)

        accu = []
        for inx, nickname in enumerate(alias):
            _payload = {
                k: tier[k][inx] for k in _keys
            }
            _payload['alias'] = nickname
            accu.append(_payload)

        tier = accu

    tier: list[dict]
    keys = None
    for factor in tier:
        factor: dict
        if keys is None:
            keys = factor.keys()
        else:
            assert factor.keys() == keys

        nickname = factor['alias']
        new_namelist = [*namelist, nickname]
        curr_maker = maker.clone()
        for k, v in factor.items():
            if k == 'alias':
                # don't try to pop alias before here.
                # all kinds of subtle mem-ref issues.
                # 1) keys are modified in-place after pop().
                # 2) yaml parser makes ref-linked subtree share a dict storage. multiple subtrees affected after pop().
                #    yaml ref is hard to disable for auto-generated configs.
                continue
            curr_maker.execute_clause({k: v})
        dfs_expand(level + 1, new_namelist, curr_maker, deposit, grids)


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

    # def add(self, objs, arg):
    #     assert len(objs) <= 1, 'add deals with 1 obj everytime: {}'.format(objs)
    #     if len(objs) == 1:
    #         field = objs[0]
    #         if isinstance(self.pointed, list):
    #             assert str.isdigit(field)
    #             field = int(field)
    #             self.pointed.insert(field, arg)
    #         elif isinstance(self.pointed, dict):
    #             assert field not in self.pointed, 'field {} present in {}'\
    #                 .format(field, self.pointed)
    #             self.pointed[field] = arg
    #         else:
    #             raise ValueError("{} is not a container node".format(self.pointed))
    #     else:
    #         assert isinstance(self.pointed, dict),\
    #             '{} is not a dict, cannot use bare add'.format(self.pointed)
    #         assert isinstance(arg, dict), 'expect dict when adding into dict'
    #         for k in arg.keys():
    #             assert k not in self.pointed, \
    #                 '{} already present in {}'.format(k, self.pointed)
    #         self.pointed.update(arg)

    def replace(self, objs, arg):
        # assert len(objs) <= 1, 'replace deals with 1 obj everytime: {}'.format(objs)
        # if len(objs) == 1:
        #     # this is really obscure logic; this is for bulk replacement; no..
        #     field = objs[0]
        #     if isinstance(self.pointed, list):
        #         assert str.isdigit(field)
        #         field = int(field)
        #         self.pointed[field] = arg
        #     elif isinstance(self.pointed, dict):
        #         assert field in self.pointed, 'field {} not present in {}'\
        #             .format(field, self.pointed)
        #         self.pointed[field] = arg
        #     else:
        #         raise ValueError("{} is not a container node".format(self.pointed))

        assert len(objs) == 0
        val_type = type(self.parent[self.child_token])
        # this is such an unfortunate hack
        # turn everything to string, so that eval could work
        # some of the clauses come from cmdline, some from yaml files for sow.
        arg = str(arg)
        if val_type == str:
            pass
        else:
            arg = eval(arg)
            assert type(arg) == val_type, \
                f"require {val_type.__name__}, given {type(arg).__name__}"

        self.parent[self.child_token] = arg

    # def delete(self, objs):
    #     if isinstance(self.pointed, list):
    #         objs = map(lambda x: int(x), objs)
    #     elif isinstance(self.pointed, dict):
    #         pass
    #     else:
    #         raise ValueError("{} is not a container node".format(self.pointed))
    #     for field in objs:
    #         del self.pointed[field]


def plant_cfg(expname, cfg_node, overwrite, repeat):
    '''plant the config
    Args:
        launch_dir: abspath! of launch directory from which run.py is copied
        exp_name: the bare name of experiment folder in which things are dumped
    '''

    def _write_cfg(_path, payload):
        assert str(_path)[-3:] == 'yml'
        with open(_path, 'w') as f:
            pl = _yaml_dump(payload)
            f.write(pl)

    def _do_plant(exp_name):
        cfg_fname = osp.join(exp_name, 'config.yml')

        if osp.isdir(exp_name):  # duplicate exists
            # 1. compare whether the options are identical
            old_cfg = yaml_read(cfg_fname)

            if old_cfg == cfg_node:
                print("dup identical", end='; ')
            else:
                cprint("dup differs: {}".format(exp_name), color=_WARN_COLOR, end='; ')

            if overwrite:
                print("overwriting")
                _write_cfg(cfg_fname, cfg_node)
                return [exp_name]
            else:
                print("skipping")
                return []

        else:
            os.makedirs(exp_name, exist_ok=False)
            _write_cfg(cfg_fname, cfg_node)
            return [exp_name]

    if repeat == 0:
        return _do_plant(expname)
    else:
        assert repeat > 0
        os.makedirs(expname, exist_ok=True)
        _planted_paths = []
        for i in range(repeat):
            _planted_paths.extend(_do_plant(
                osp.join(expname, f"{i:0>2}")
            ))
        return _planted_paths


def _yaml_dump(state):
    return yaml.safe_dump(state, sort_keys=False, allow_unicode=True, default_flow_style=False)


if __name__ == '__main__':
    main()
