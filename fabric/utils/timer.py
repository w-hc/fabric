from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import time
import datetime


class Timer(object):
    """A simple timer."""

    def __init__(self):
        self.reset()

    def tic(self):
        self.start_time = time.perf_counter()

    def toc(self, average=True):
        self.diff = time.perf_counter() - self.start_time
        self.total_time += self.diff
        self.calls += 1
        self.avg = self.total_time / self.calls
        if average:
            return self.avg
        else:
            return self.diff

    def reset(self):
        self.total_time = 0.
        self.calls = 0
        self.start_time = 0.
        self.diff = 0.
        self.avg = 0.

    def eta(self, curr_step, tot_step):
        remaining_steps = tot_step - curr_step
        eta_seconds = self.avg * remaining_steps
        return str(datetime.timedelta(seconds=int(eta_seconds)))
