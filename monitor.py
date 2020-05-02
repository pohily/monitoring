from datetime import datetime, timedelta

from constants import TIME_DELTA

class Monitor():
    def __init__(self, start_time=None):
        NOW = datetime.now()
        if not start_time:
            self.start_time = NOW - timedelta(minutes=TIME_DELTA)
            self.last_time = NOW
        else:
            self.start_time = start_time
            if not self.last_time:
                self.last_time = NOW
            else:
                self.start_time = self.last_time
                self.last_time = self.last_time + timedelta(minutes=TIME_DELTA)

    def test(self):
        print('test')