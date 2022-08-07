from pathlib import Path
import json
from datetime import datetime, timedelta
from inspect import stack


_CURRENT_BEAT_STACK = []


class IntervalTicker():
    def __init__(self, interval=60):
        self.interval = timedelta(seconds=interval)
        self.last_tick = datetime.now()
        self.now = self.last_tick

    def tick(self):
        self.now = datetime.now()
        if (self.now - self.last_tick) > self.interval:
            self.last_tick = self.now
            return True

    def tick_str(self):
        return self.now.isoformat(timespec='seconds')


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


def get_tqdm_meter(pbar, format_dict):
    format_dict['bar_format'] = "{r_bar}"
    meter_str = pbar.format_meter(**format_dict)
    meter_str = meter_str[2:]
    return meter_str


def caller_info(n_stack_up):
    info = stack()[1 + n_stack_up]  # 1 up as base so that it starts from caller
    msg = f"{info.filename}:{info.lineno} - {info.function}"
    return msg


class HeartBeat():
    def __init__(
        self, pbar, write_interval=10,
        output_dir="./", fname="heartbeat.json"
    ):
        self.pbar = pbar
        self.fname = Path(output_dir) / fname
        self.ticker = IntervalTicker(write_interval)
        self.completed = False

        # force one write at the beginning
        self.beat(force_write=True, n_stack_up=2)

    def beat(self, force_write=False, n_stack_up=1):
        on_write_period = self.ticker.tick()
        if force_write or on_write_period:
            stats = self.stats()
            stats['caller'] = caller_info(n_stack_up)

            with open(self.fname, "w") as f:
                json.dump(stats, f)

    def done(self):
        self.completed = True
        self.beat(force_write=True, n_stack_up=2)

    def stats(self):
        pbar = self.pbar
        fdict = pbar.format_dict
        stats = {
            "beat": self.ticker.tick_str(),
            "done": self.completed,
            "meter": get_tqdm_meter(pbar, fdict),
            "elapsed": int(fdict['elapsed'])
        }
        return stats

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
