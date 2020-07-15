import time


def now_time_integer():
    return int(time.time() * 1000)


def now_time():
    return str(now_time_integer())
