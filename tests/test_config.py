from fabric.config import BaseConfig, mkcfg, oneof
from pprint import pp
import pytest
from contextlib import contextmanager


@contextmanager
def expect_error(exc_type, msg=None):
    with pytest.raises(exc_type) as exc_info:
        yield
    if msg is not None:
        assert msg in str(exc_info.value)


def assert_dict_eq(dictA, dictB):
    # key ordering must be equal too
    keysA = list(dictA.keys())
    keysB = list(dictB.keys())
    assert keysA == keysB
    assert dictA == dictB


def test_basic_use_cases():
    assert_dict_eq(BaseConfig.schema(), {})

    class Cfg(BaseConfig):
        pass
    assert_dict_eq(Cfg.schema(), {})
    x = Cfg()
    assert_dict_eq(x.as_dict(), {})

    # types are inferred from default vals
    class Cfg(BaseConfig):
        a = 16
        b = 3.5
    assert_dict_eq(Cfg.schema(), dict(a='int', b='float'))

    x = Cfg()
    assert_dict_eq(x.as_dict(), dict(a=16, b=3.5))

    x = Cfg(dict(a=4, b=-2.8))
    assert_dict_eq(x.as_dict(), dict(a=4, b=-2.8))

    with expect_error(AssertionError, 'dtype mismatch'):
        x = Cfg(dict(a=1.1))

    x = Cfg(dict(a=None, b=9.0))
    assert_dict_eq(x.as_dict(), dict(a=None, b=9.0))  # setting a val to None is ok.

    # if no default val, then must have type anns
    class Cfg(BaseConfig):
        a: int
        b: float = None  # None as default is ok, as long as there's type ann
    assert_dict_eq(Cfg.schema(), dict(a='int', b='float'))
    x = Cfg(dict(a=5))
    assert_dict_eq(x.as_dict(), dict(a=5, b=None))

    class Cfg(BaseConfig):
        a: int
        b = None

    with expect_error(ValueError, "neither type ann nor default value"):
        Cfg.schema()

    # mix of val and type anns are fine.
    # declaration ordering is repected.
    class Cfg(BaseConfig):
        a: int
        b = 2.0
        c: bool
    assert_dict_eq(Cfg.schema(), dict(a='int', b='float', c='bool'))
    x = Cfg()
    assert_dict_eq(x.as_dict(), dict(a=None, b=2.0, c=None))
    x = Cfg(dict(c=True, a=5))
    assert_dict_eq(x.as_dict(), dict(a=5, b=2.0, c=True))


def test_list():
    # we enforce that all members of list/tuple must be of same type
    class Cfg(BaseConfig):
        a: list[int] = None
        b = [3, 1]
        c: tuple[float]

    assert_dict_eq(Cfg.schema(), dict(a='list', b='list', c='tuple'))

    x = Cfg(dict(a=[8, 4], b=None))
    assert_dict_eq(x.as_dict(), dict(a=[8, 4], b=None, c=None))

    with expect_error(AssertionError, 'member dtype'):
        Cfg(dict(a=[8., 4], b=None))

    # nested list
    class Cfg(BaseConfig):
        a: tuple[list[int]] = (
            [1, 2],
            [3, 4]
        )

    assert_dict_eq(Cfg.schema(), dict(a='tuple'))

    x = Cfg(dict(a=([1], [2], [3])))
    assert_dict_eq(x.as_dict(), dict(a=([1], [2], [3])))


# Example function
# @torch.no_grad()
def some_routine(x, y, *, a: int, b: float, c: str, d: bool = True) -> int:
    pass


class MyCls():
    def __init__(
        self, input_1, input_2, *, a: int, b=2., c: bool
    ):
        pass


def test_using_external():
    class Cfg(BaseConfig):
        routine = mkcfg(some_routine)

    assert_dict_eq(
        Cfg.schema(),
        {'routine': dict(a='int', b='float', c='str', d='bool')}
    )

    x = Cfg({
        'routine': {
            'a': 3, 'b': 2.0, 'd': False
        }
    })
    assert_dict_eq(
        x.as_dict(),
        {'routine': dict(a=3, b=2.0, c=None, d=False)}
    )
    assert x.routine.d is False

    class Cfg(BaseConfig):
        model = mkcfg(MyCls)

    assert_dict_eq(
        Cfg.schema(),
        {'model': dict(a='int', b='float', c='bool')}
    )


def test_config_inheritance():
    class CfgA(BaseConfig):
        a: int = 3

    class CfgB(CfgA):
        b: float = 4.0

    assert_dict_eq(CfgB.schema(), dict(a='int', b='float'))

    class CfgC(CfgB):
        c: bool = False
        a: str

    assert_dict_eq(CfgC.schema(), dict(a='str', b='float', c='bool'))
    assert_dict_eq(CfgC().as_dict(), dict(a=None, b=4.0, c=False))

    CfgD = mkcfg(some_routine)

    class CfgE(CfgD):
        a: bool

    assert_dict_eq(
        CfgE.schema(),
        dict(a='bool', b='float', c='str', d='bool')
    )


class QWen:
    def __init__(self, x, y, *, a: float = 1.0, flag: bool = False):
        pass


class GPT2:
    def __init__(self, *, n_layers=5, apply_rope=False):
        pass


class LLAMA2:
    def __init__(self, *, use_moe=True):
        pass


class LLAMA2Config(BaseConfig):
    model: LLAMA2
    some_other: int
    pass


class ModelConfig(BaseConfig):
    input_dim: int
    hidden_dim: int
    inner_dim = 128
    process_fn = mkcfg(some_routine)
    # model: Annotated[str, mkcfg(GPT2), LLAMA2Config] = "GPT2"
    model = oneof(
        mkcfg(GPT2), LLAMA2Config, mkcfg(QWen)
    )
    dropout: float = 0.5
    lr = 0.1
    num_layers = [3, 4, 5]

    def test_func1(self):
        pass


def test_overall():
    pp(ModelConfig.__dict__)
    print("")
    pp(ModelConfig.schema(expand_options=True))
    print("")
    cfg = ModelConfig()
    print(cfg)
