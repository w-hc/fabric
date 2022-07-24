from pathlib import Path
import json
from datetime import datetime, timedelta
from inspect import stack


_CURRENT_BEAT_STACK = []


def get_heartbeat():
    """
    Returns:
        The :class:`HeartBeat` object that's currently being used.
        Throws an error if no :class:`EventStorage` is currently enabled.
    """
    assert len(
        _CURRENT_BEAT_STACK
    ), "get_heartbeat() has to be called inside a 'with EventStorage(...)' context!"
    return _CURRENT_BEAT_STACK[-1]


def get_tqdm_meter(pbar):
    data = pbar.format_dict
    data['bar_format'] = "{r_bar}"
    meter_str = pbar.format_meter(**data)
    return meter_str


def caller_info(n_stack_up):
    info = stack()[1 + n_stack_up]  # 1 up as base so that it starts from caller
    msg = f"{info.filename}:{info.lineno} - {info.function}"
    return msg


class HeartBeat():
    def __init__(
        self, pbar, write_interval=60,
        output_dir="./", fname="heartbeat.json"
    ):
        self.pbar = pbar
        self.fname = Path(output_dir) / fname
        self.write_interval = timedelta(seconds=write_interval)
        self.last_write = datetime.now()
        self.completed = False

        # force one write at the beginning
        self.beat(force_write=True, n_stack_up=2)

    def beat(self, force_write=False, n_stack_up=1):
        now = datetime.now()

        if force_write or (now - self.last_write) > self.write_interval:
            stats = self.stats(now)
            stats['caller'] = caller_info(n_stack_up)

            with open(self.fname, "w") as f:
                json.dump(stats, f)

            self.last_write = now

    def done(self):
        self.completed = True
        self.beat(force_write=True, n_stack_up=2)

    def stats(self, now):
        # data = self.pbar.format_dict
        tick = now.isoformat(timespec='seconds')
        stats = {
            "beat": tick,
            "done": self.completed,
            "meter": get_tqdm_meter(self.pbar)
        }
        return  stats

    def __enter__(self):
        _CURRENT_BEAT_STACK.append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert _CURRENT_BEAT_STACK[-1] == self
        _CURRENT_BEAT_STACK.pop()



"""
# should write a heartbeat every few seconds; maybe every 30s;
# use Yuxin's global stack and context design to make sure you can always
# get hold of the current heartbeater, and tick it even if you are doing inner-looped eval task

maybe use this to report which file/line the caller is beating from; so that I might know it's in some loop
https://stackoverflow.com/questions/24438976/debugging-get-filename-and-line-number-from-which-a-function-is-called
benchmark the performance of this function


{
    done: bool,
    beat: timestamp,
    meter: str,
    caller: ..,
}
"""
