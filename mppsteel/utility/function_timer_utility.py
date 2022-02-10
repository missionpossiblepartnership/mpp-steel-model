"""Script to time key functions at runtime"""

import time
from typing import Union

def format_times(start_t: float, end_t: float) -> str:
    """Formats a time duration between a start time and end time as a string.

    Args:
        start_t (float): The start time.
        end_t (float): The end time.

    Returns:
        str: A formatted string of the time duration.
    """
    time_diff = end_t - start_t
    return f"{time_diff :0.2f} seconds | {time_diff / 60 :0.2f} minutes"


class TimeContainerClass:
    """A Timer class that times the difference between events.
    Insantiates with an empty dictionary where the function event timings will be stored.
    """
    def __init__(self):
        self.time_container = {}

    def update_time(self, func_name: str, timings: str) -> None:
        """Method that stores times in the instantiated timing dictionary store.

        Args:
            func_name (str): The name of the function you want to time.
            timings (str): The time it took to run the function.
        """
        self.time_container[func_name] = timings

    def return_time_container(self, return_object: bool = False) -> Union[None, dict]:
        """Returns the contents of the timer dictionary container as a printed statement or object.

        Args:
            return_object (bool, optional): Flag to return the dictionary object. Defaults to False.

        Returns:
            Union[None, dict]: Returns a printed statement to the console and optionally a dictionary object depending on the `return_object` flag.
        """
        time_container = self.time_container
        for entry in time_container:
            print(f"The {entry} function took {time_container[entry]}")
        if return_object:
            return time_container


TIME_CONTAINER = TimeContainerClass()


def timer_func(func):
    """Decorater function that times a function that is passed to it using a TimeContainerClass object.

    Args:
        func: A function that you want to time.
    """

    def wrap_func(*args, **kwargs):
        starttime = time.time()
        result = func(*args, **kwargs)
        endtime = time.time()
        TIME_CONTAINER.update_time(func.__name__, format_times(starttime, endtime))
        return result

    return wrap_func
