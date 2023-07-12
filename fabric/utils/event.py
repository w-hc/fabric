from pathlib import Path
import json
import os
from contextlib import contextmanager
from .ticker import IntervalTicker


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


def read_lined_json(fname):
    with Path(fname).open('r') as f:
        for line in f:
            item = json.loads(line)
            yield item


def read_stats(dirname, key):
    if dirname is None or not (fname := Path(dirname) / "history.json").is_file():
        return [], []
    stats = read_lined_json(fname)
    stats = list(filter(lambda x: key in x, stats))
    xs = [e['iter'] for e in stats]
    ys = [e[key] for e in stats]
    return xs, ys


class EventStorage():
    def __init__(self, output_dir="./", start_iter=0, flush_period=60):
        self.iter = start_iter
        self.ticker = IntervalTicker(flush_period)
        self.history = []
        self._prefix = ""
        self._init_curr_buffer_()

        self.output_dir = output_dir
        self.writable = False

    def _open(self):
        if self.writable:
            output_dir = Path(self.output_dir)
            if not output_dir.is_dir():
                output_dir.mkdir(parents=True, exist_ok=True)
            json_fname = output_dir / 'history.json'

            self._file_handle = json_fname.open('a', encoding='utf8')
            self.output_dir = output_dir  # make sure it's a path object

    def _init_curr_buffer_(self):
        self.buffer = {'wrote_artifact': False}

    def step(self, flush=False):
        wrote_artifact = self.buffer.pop('wrote_artifact')

        if len(self.buffer) > 0:
            self.buffer['iter'] = self.iter
            self.history.append(self.buffer)

        on_flush_period = self.ticker.tick()
        if flush or on_flush_period or wrote_artifact:
            self.flush_history()

        self.iter += 1
        self._init_curr_buffer_()

    def flush_history(self):
        if self.writable:
            for item in self.history:
                line = json.dumps(item, sort_keys=True, ensure_ascii=False) + "\n"
                self._file_handle.write(line)
            self._file_handle.flush()
        self.history = []

    def full_key(self, key):
        assert isinstance(key, str)
        name = self._prefix + key
        return name

    def put(self, key, val):
        key = self.full_key(key)
        assert isinstance(val, (int, float, str))
        if isinstance(val, float):
            val = round(val, 3)
        self.buffer[key] = val

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
        self.buffer['wrote_artifact'] = True
        return fname

    def close(self):
        self.flush_history()
        if self.writable:
            self._file_handle.close()

    def get_last(self):
        if len(self.history) > 0:
            last = self.history[-1]
            return last

    @contextmanager
    def name_scope(self, name):
        """
        Yields:
            A context within which all the events added to this storage
            will be prefixed by the name scope.
        """
        old_prefix = self._prefix
        self._prefix = name.rstrip("/") + "/"
        yield
        self._prefix = old_prefix

    def __enter__(self):
        if len(_CURRENT_STORAGE_STACK) > 0:
            parent = _CURRENT_STORAGE_STACK[-1]
            dir_parent, dir_child = parent.output_dir, self.output_dir
            if dir_parent is not None and dir_child is not None:
                # child resides in parent at a particular iter
                self.output_dir = dir_parent / f"{dir_child}_{parent.iter}"
                parent.put(str(dir_child), str(self.output_dir))
            else:
                self.output_dir = None

        if self.output_dir is not None:
            self.writable = True
            self._open()

        _CURRENT_STORAGE_STACK.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert _CURRENT_STORAGE_STACK[-1] == self
        _CURRENT_STORAGE_STACK.pop()
        self.close()
