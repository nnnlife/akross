from datetime import datetime

try:
    from time import time_ns
except ImportError:
    def time_ns():
        now = datetime.now()
        return int(now.timestamp() * 1e9)


def get_msec():
    return int(time_ns() / 1000000)


def msec_diff(msec):
    return datetime.fromtimestamp(msec / 1000) - datetime.now()