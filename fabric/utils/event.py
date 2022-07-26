import logging
from pathlib import Path
import json
import os
from contextlib import contextmanager

from .heartbeat import IntervalTicker

# import pickle
# from ..io import save_object, load_object

logger = logging.getLogger(__name__)


_CURRENT_STORAGE_STACK = []


def get_event_storage():
    """
    Returns:
        The :class:`EventStorage` object that's currently being used.
        Throws an error if no :class:`EventStorage` is currently enabled.
    """
    assert len(
        _CURRENT_STORAGE_STACK
    ), "get_event_storage() has to be called inside a 'with EventStorage(...)' context!"
    return _CURRENT_STORAGE_STACK[-1]


def list_filter_items(inputs, func):
    accu = []
    for item in inputs:
        if func(item):
            accu.append(item)
    return accu


def read_lined_json(fname):
    with Path(fname).open('r') as f:
        lines = f.readlines()
    accu = []
    for li in lines:
        item = json.loads(li)
        accu.append(item)
    return accu


def read_stats(dirname, key):
    fname = dirname / "history.json"
    stats = read_lined_json(fname)
    stats = list_filter_items(stats, lambda x: key in x)
    xs = [e['iter'] for e in stats]
    ys = [e[key] for e in stats]
    return xs, ys


class EventStorage():
    def __init__(self, output_dir="./", start_iter=0, flush_period=60):
        self.iter = start_iter
        self.ticker = IntervalTicker(flush_period)
        self.history = []
        self._current_prefix = ""
        self._init_curr_buffer_()

        self.output_dir = output_dir
        self.writable = False

    def _open(self):
        if self.writable:
            output_dir = Path(self.output_dir)
            if not output_dir.is_dir():
                output_dir.mkdir(parents=True, exist_ok=True)
            json_fname = output_dir / 'history.json'

            self._file_handle = json_fname.open('a')
            self.output_dir = output_dir  # make sure it's a path object

    def _init_curr_buffer_(self):
        self.curr_buffer = {'iter': self.iter}

    def step(self, flush=False):
        self.history.append(self.curr_buffer)

        on_flush_period = self.ticker.tick()
        if flush or on_flush_period:
            self.flush_history()

        self.iter += 1
        self._init_curr_buffer_()

    def flush_history(self):
        if self.writable:
            for item in self.history:
                line = json.dumps(item, sort_keys=True) + "\n"
                self._file_handle.write(line)
            self._file_handle.flush()
        self.history = []

    def full_key(self, key):
        assert isinstance(key, str)
        name = self._current_prefix + key
        return name

    def put(self, key, val):
        key = self.full_key(key)
        assert isinstance(val, (int, float, str))
        if isinstance(val, float):
            val = round(val, 3)
        self.curr_buffer[key] = val

    def put_scalars(self, **kwargs):
        for k, v in kwargs.items():
            self.put(k, v)

    def put_artifact(self, key, ext, save_func):
        if not self.writable:
            return
        os.makedirs(self.output_dir / key, exist_ok=True)
        fname = (self.output_dir / key / f"step_{self.iter}").with_suffix(ext)
        fname = str(fname)

        # must be called inside so that
        # 1. the func is not executed if the metric is not writable
        # 2. the key is only inserted if the func succeeds
        save_func(fname)
        self.put(key, fname)
        return fname

    # def put_pickled(self, key, obj):
    #     raise NotImplementedError()
    #     self.put_artifact(
    #         lambda fn: save_object(obj, fn),
    #         key, f"{key}.pkl"
    #     )

    def close(self):
        self.flush_history()
        if self.writable:
            self._file_handle.close()

    def print_last(self):
        if len(self.history) > 0:
            last = self.history[-1]
            last = {
                k: round(v, 3)
                if isinstance(v, float) else v
                for k, v in last.items()
            }
            logger.info(last)

    @contextmanager
    def name_scope(self, name):
        """
        Yields:
            A context within which all the events added to this storage
            will be prefixed by the name scope.
        """
        old_prefix = self._current_prefix
        self._current_prefix = name.rstrip("/") + "/"
        yield
        self._current_prefix = old_prefix

    def __enter__(self):
        if len(_CURRENT_STORAGE_STACK) > 0:
            parent = _CURRENT_STORAGE_STACK[-1]
            root, dirname = parent.output_dir, self.output_dir
            if root is not None and dirname is not None:
                child_dir = parent.output_dir / f"{self.output_dir}_{parent.iter}"
                self.output_dir = child_dir
                parent.put(str(dirname), str(child_dir))

        if self.output_dir is not None:
            self.writable = True
            self._open()

        _CURRENT_STORAGE_STACK.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert _CURRENT_STORAGE_STACK[-1] == self
        _CURRENT_STORAGE_STACK.pop()
        self.close()


def test_storage():
    # import os
    # output_dir = Path(Path(os.getcwd()) / "out")

    # history = read_lined_json(output_dir / "history.json")
    # for item in history:
    #     if 'eval_test' in item:
    #         print(item['iter'])
    #         fname = item['eval_test']
    #         a = load_object(output_dir / fname)

    # # print(len(a))
    # return
    storage = EventStorage()
    for i in range(100):
        storage.put_scalars(
            loss=i, loss_R=i * 2, lr=0.01
        )
        # if (i + 1) % 50 == 0:
        #     arr = np.random.randn(100, 10)
        #     storage.put_artifact("pred", arr)
        storage.step()
    storage.close()


def main():
    event = EventStorage("./")
    fname = event.get_store_path("model.ckpt")
    torch.save(fname, "asdf")
    event.put("ckpt", fname.name)


if __name__ == '__main__':
    test_storage()
