import inspect
from typing import get_origin, get_args
from types import FunctionType
from functools import partial
from pprint import pformat

ALLOWED_ATOMIC_TYPES = (bool, int, float, str)
ALLOWED_ITER_TYPES = (list, tuple)


def is_config_class(x):
    return isinstance(x, type) and issubclass(x, BaseConfig)


def optionally_break_composite_dtype(dtype):
    '''
    A[B] -> A, B
    A    -> A, None
    '''
    A = get_origin(dtype)
    if A is None:  # not composite type
        A, B = dtype, None
    else:
        B = get_args(dtype)[0]
    return A, B


def recursively_check_itr_dtype(itr, dtype_container, dtype_member):
    assert isinstance(itr, dtype_container)

    if len(itr) == 0:
        return

    if dtype_member is None:
        dtype_member = type(itr[0])

    A, B = optionally_break_composite_dtype(dtype_member)
    if not (A in ALLOWED_ATOMIC_TYPES or A in ALLOWED_ITER_TYPES):
        raise ValueError(f"iter member must be atomic or nested list/tuple; got {A.__name__}")

    for elem in itr:
        assert type(elem) is A, \
            f"{dtype_container.__name__} member dtype is {A.__name__}; got {elem} of type {type(elem).__name__}"
        if A in ALLOWED_ITER_TYPES:
            recursively_check_itr_dtype(elem, A, B)

    return


def do_type_check(name, dtype, val):
    # both args are assumed not None
    # dtype might be list[int]
    A, B = optionally_break_composite_dtype(dtype)
    if A in ALLOWED_ATOMIC_TYPES:
        assert type(val) is A, f"dtype mismatch on {name}: expect {A.__name__}, got {val}"
    elif A in ALLOWED_ITER_TYPES:
        recursively_check_itr_dtype(val, A, B)
    else:
        raise ValueError(f"{name}: {val} of declared type {dtype} not allowed in config")


def check_declared_dtype_and_val(name, dtype, val):
    # dtype and val might be None
    if dtype is None:
        if val is None:
            raise ValueError(f"{name} has neither type ann nor default value")
        else:
            dtype = type(val)

    # now guaranteed to have a dtype; verify it against the val

    if val is None:
        return dtype

    do_type_check(name, dtype, val)

    return dtype


"""
custom dict that logs the order of novel key insertion.
"""


class AnnotationDict(dict):
    def __init__(self, declared_order):
        '''record ordering to a external, provided list'''
        super().__init__()
        self._decl_order = declared_order

    def __setitem__(self, key, value):
        if key not in self._decl_order:
            self._decl_order.append(key)
        super().__setitem__(key, value)


class TrackerNamespace(dict):
    def __init__(self):
        super().__init__()
        self._decl_order = []
        # pre-insert the custom annotation dict so Python
        # uses it for storing `__annotations__`.
        super().__setitem__('__annotations__', AnnotationDict(self._decl_order))

    def __setitem__(self, key, value):
        # skip on __annotations__, __module__, __doc__, etc
        if not (key in self or key.startswith('_')):
            self._decl_order.append(key)
        super().__setitem__(key, value)


class TrackFieldsMeta(type):
    @classmethod
    def __prepare__(metacls, name, bases):
        return TrackerNamespace()

    def __new__(metacls, name, bases, namespace):
        """
        By the time we get here, namespace._decl_order has the order of:
          1) purely annotated fields,
          2) assigned fields,
          3) any other non-dunder symbols (methods, etc.)
        in the exact order they appeared in the source code.
        """
        decl_order = namespace._decl_order

        namespace = dict(namespace)  # switch to regular dict to freeze the _decl_order
        namespace['_decl_order'] = decl_order

        # build the class object
        return super().__new__(metacls, name, bases, namespace)


class BaseConfig(metaclass=TrackFieldsMeta):

    # used to dynamically make configs for functions and classes
    __directly_populated_fields__ = None

    @classmethod
    def key_name(cls):
        return cls.__name__

    @classmethod
    def find_fields(cls):
        mro_reversed = cls.__mro__[::-1]
        assert mro_reversed[1] is BaseConfig and mro_reversed[-1] is cls
        candidates = mro_reversed[2:]  # skip [object, BaseConfig]
        all_fields = {}
        for elem in candidates:
            all_fields.update(elem._find_curr_cls_fields())
        return all_fields

    @classmethod
    def _find_curr_cls_fields(cls):
        '''
        Fields excluding those of the parent classes.
        I could have used recursion to merge all fields.
        But I prefer directly traversing __mro__.
        - error msg is cleaner with fewer call stacks.
        - easier to handle multiple inheritance.
        '''
        # prevent mro lookup on parents. only check curr cls attribute!
        # critical when inheriting from a parent with directly populated fields.
        curr_cls_dfields = cls.__dict__.get('__directly_populated_fields__', None)
        if curr_cls_dfields is not None:
            return curr_cls_dfields

        decl_order = cls._decl_order
        cls_dict = cls.__dict__
        cls_anns = inspect.get_annotations(cls)

        fields = {}

        for k in decl_order:
            dtype = cls_anns.get(k, None)
            cls_val = cls_dict.get(k, None)

            if k.startswith('_') or isinstance(cls_val, (FunctionType, classmethod, staticmethod)):
                # WARN: not a sure-fire way to catch everything.
                # print(f"ignoring: {k}")
                continue

            if is_config_class(cls_val):
                fields[k] = (type, cls_val)
                continue

            if isinstance(cls_val, ConfigArray):
                fields[k] = (ConfigArray, cls_val)
                continue

            dtype = check_declared_dtype_and_val(k, dtype, cls_val)
            fields[k] = (dtype, cls_val)

        return fields

    @classmethod
    def schema(cls, expand_options=True):
        fields = cls.find_fields()
        res = {}
        for k, (dtype, v) in fields.items():
            if is_config_class(v):
                res[k] = v.schema()
            elif isinstance(v, ConfigArray):
                res[k] = "option"
                if expand_options:
                    for name in v.names:
                        res[name] = v[name].schema()
            else:
                res[k] = dtype.__name__
        return res

    def __init__(self, user_supplied_cfg: dict = {}):
        fields = self.find_fields()

        instantiated_cfg = {}
        for k, (dtype, v) in fields.items():
            if is_config_class(v):
                _kwargs = user_supplied_cfg.get(k, {})
                instantiated_cfg[k] = v(_kwargs)
                del _kwargs
                continue

            if isinstance(v, ConfigArray):
                user_choice = user_supplied_cfg.get(k, None)
                if user_choice is None:
                    user_choice = v.default_key
                else:
                    assert isinstance(user_choice, str), f"choice on {k} must be a str"
                    assert user_choice in v.options, f"{user_choice} not in options: {v.names}"

                instantiated_cfg[k] = user_choice
                '''
                model: GPT2
                GPT2:  # auto add this extra field
                    ...
                '''
                _kwargs = user_supplied_cfg.get(user_choice, {})
                instantiated_cfg[user_choice] = v[user_choice](_kwargs)
                del _kwargs
                continue

            if k in user_supplied_cfg:
                new_v = user_supplied_cfg[k]
                if new_v is not None:  # allow user providing None
                    do_type_check(k, dtype, new_v)
                v = new_v

            instantiated_cfg[k] = v

        for k, v in instantiated_cfg.items():
            setattr(self, k, v)

        self._cfg_dict = instantiated_cfg

    def as_dict(self):
        # shallow copy! fields which are containers (e.g. list) will be shared.
        # relying on user to not mutate them.
        res = self._cfg_dict.copy()
        for k, v in res.items():
            if isinstance(v, BaseConfig):
                res[k] = v.as_dict()
        return res

    def __str__(self):
        return pformat(self.as_dict(), sort_dicts=False)

    def make(self):
        raise NotImplementedError()


def extract_config_knobs_from_callable(obj):
    '''keyword-only parameters i.e. those coming after * in a function def are treated as configurable.
    '''
    if inspect.isfunction(obj):
        target = obj
    elif inspect.isclass(obj):
        target = getattr(obj, '__init__', None)
        if not target:
            raise ValueError(f"Class {obj.__name__} does not have an __init__ method.")
    else:
        raise ValueError("Input must be a function or a user-defined class.")

    # unwrap to go below decorators like @torch.no_grad()
    sig = inspect.signature(target, follow_wrapped=True)
    params = sig.parameters

    empty_marker = inspect.Parameter.empty

    def _read_if_not_emptymarker(val):
        return None if val is empty_marker else val

    config_knobs = []

    for name, param in params.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            raise ValueError(f"*args, **kwargs not allowed in a configurable callable; offending param: {name}")

        if param.kind == param.KEYWORD_ONLY:
            dtype, default_val = \
                _read_if_not_emptymarker(param.annotation), \
                _read_if_not_emptymarker(param.default)

            dtype = check_declared_dtype_and_val(name, dtype, default_val)
            config_knobs.append(
                (name, dtype, default_val)
            )

    return config_knobs


def mkcfg(target):
    # allow overriding some class attributes?
    class WrapperCfg(BaseConfig):
        @classmethod
        def key_name(cls):
            return target.__name__

        def make(self):
            return partial(target, **self._cfg_dict)

    knobs = extract_config_knobs_from_callable(target)

    fields = {}
    for (name, dtype, default_val) in knobs:
        assert dtype is not None
        fields[name] = (dtype, default_val)
    WrapperCfg.__directly_populated_fields__ = fields

    # WrapperCfg.__annotations__ = {}
    # # if default_val is not None:
    # setattr(WrapperCfg, name, default_val)
    # # if dtype is not None:
    # WrapperCfg.__annotations__[name] = dtype

    return WrapperCfg


class ConfigArray():
    def __init__(self, array):
        self.options = {
            cls.key_name(): cls for cls in array
        }
        self.names = list(self.options.keys())
        self.default_key = self.names[0]

    def get_default(self):
        return self.options[self.default_key]

    def __getitem__(self, k):
        return self.options[k]


def oneof(*args):
    for cls in args:
        assert is_config_class(cls), f"{cls} not a config class"

    return ConfigArray(args)
