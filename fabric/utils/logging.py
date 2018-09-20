"""Utilities for logging."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from collections import deque
import json
import logging
import numpy as np
import sys

# Print lower precision floating point values than default FLOAT_REPR
# Note! Has no use for json encode with C speedups
json.encoder.FLOAT_REPR = lambda o: format(o, '.6f')


def log_json_stats(stats, sort_keys=True):
    print('json_stats: {:s}'.format(json.dumps(stats, sort_keys=sort_keys)))


class SmoothedValue(object):
    """Track a series of values and provide access to smoothed values over a
    window or the global series average.
    """

    def __init__(self, window_size):
        self.deque = deque(maxlen=window_size)
        self.series = []
        self.total = 0.0
        self.count = 0

    def AddValue(self, value):
        self.deque.append(value)
        self.series.append(value)
        self.count += 1
        self.total += value

    def GetMedianValue(self):
        return np.median(self.deque)

    def GetAverageValue(self):
        return np.mean(self.deque)

    def GetGlobalAverageValue(self):
        return self.total / self.count


def setup_logging(name):
    FORMAT = '%(levelname)s %(filename)s:%(lineno)4d: %(message)s'
    # Manually clear root loggers to prevent any module that may have called
    # logging.basicConfig() from blocking our logging setup
    logging.root.handlers = []
    logging.basicConfig(level=logging.INFO, format=FORMAT, stream=sys.stdout)
    logger = logging.getLogger(name)
    return logger
