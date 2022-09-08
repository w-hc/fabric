from datetime import date, time, datetime, timedelta
from time import sleep


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


class PeriodSlot():
    # this is quite similar to interval ticker in the end; think abt merging them
    def __init__(self, start_time, period):
        start = datetime.combine(
            date.today(), time.fromisoformat(start_time)
        )
        while datetime.now() > start:
            start += period

        print(f"start time: {start}, period: {period}")
        self.next = start
        self.period = period

    def on_tick(self):
        now = datetime.now()
        return now > self.next

    def step(self):
        self.next += self.period


class ScheduledQueue():
    def __init__(self, queue):
        self.queue = queue

    def infinite_loop(self, sleep_interval=5):
        from tqdm import tqdm
        pbar = tqdm()

        while True:
            sleep(sleep_interval)
            pbar.update()
            for slot, action in self.queue:
                if slot.on_tick():
                    action()
                    slot.step()
