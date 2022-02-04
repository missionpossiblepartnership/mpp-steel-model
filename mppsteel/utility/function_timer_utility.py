"""Script to time key functions at runtime"""

import time

def format_times(start_t: float, end_t: float) -> str:
    time_diff = end_t - start_t
    return f'{time_diff :0.2f} seconds | {time_diff / 60 :0.2f} minutes'

class TimeContainerClass:
    def __init__(self):
        self.time_container = {}

    def update_time(self, func_name: str, timings: str) -> None:
        self.time_container[func_name] = timings

    def return_time_container(self, return_object: bool = False) -> dict:
        time_container = self.time_container
        for entry in time_container:
            print(f'The {entry} function took {time_container[entry]}')
        if return_object:
            return time_container

TIME_CONTAINER = TimeContainerClass()

def timer_func(func):
    def wrap_func(*args, **kwargs):
        starttime = time.time()
        result = func(*args, **kwargs)
        endtime = time.time()
        TIME_CONTAINER.update_time(func.__name__, format_times(starttime, endtime))
        return result
    return wrap_func
