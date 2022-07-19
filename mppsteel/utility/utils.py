"""Utility library for functions used throughout the module"""
import itertools
import sys

import multiprocessing as mp

import numpy as np
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from typing import Union, Iterable as it

from currency_converter import CurrencyConverter

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def get_today_time(fmt: str = "%y%m%d_%H%M%S") -> str:
    """Returns a formatted string of todays date.

    Args:
        The format you would like the datetime object to take.

    Returns:
        str: A string with today's date.
    """
    return datetime.today().strftime(fmt)


def create_list_permutations(list1: list, list2: list) -> list:
    """Create a combined list of every permutation of objects in two lists.

    Args:
        list1 (list): The first list you want to use in the permuation.
        list2 (list): The second list you want to use in the permutation.

    Returns:
        list: The combined list of permutations between two lists.
    """
    comb = [
        list(zip(each_permutation, list2))
        for each_permutation in itertools.permutations(list1, len(list2))
    ]
    return list(itertools.chain(*comb))


def stdout_query(question: str, default: str, options: str) -> str:
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    if default not in options:
        raise ValueError(f"invalid default answer {default}. Not in options: {options}")

    while True:
        sys.stdout.write(f"{question} {default}")
        choice = input().lower()
        if choice == "":
            return default
        elif choice in options:
            return choice
        else:
            sys.stdout.write(f"Please respond with a choice from {options}.\n")


def get_currency_rate(base: str, target: str) -> None:
    """Gets a currency rate exchange between two currencies using the CurrencyConverter library.

    Args:
        base (str): [description]
        target (str): [description]

    Returns:
        None: A None object
    """
    logger.info(f"Getting currency exchange rate for {base}")

    if (len(base) == 3) & (len(target) == 3):
        try:
            curr = CurrencyConverter()
            return curr.convert(1, base.upper(), target.upper())
        except:
            raise ValueError(
                f"You entered an incorrect currency, either {base} or {target}"
            )
    return None


def enumerate_iterable(iterable: it) -> dict:
    """Enumerates an iterable as dictionary with the iterable value as the key and the order number as the value.

    Args:
        iterable (Iterable): The iterable you want to enumerate

    Returns:
        dict: A dictionary with the the iterable value as the key and the order number as the value.
    """
    return dict(zip(iterable, range(len(iterable))))


def cast_to_float(val: Union[float, int, Iterable[float]]) -> float:
    """Casts a numerical object to a float if not a float already.

    Args:
        val Union[float, int, Iterable[float]]): The numerical value you want to be a float. Can be an iterable containing a numberical value(s), that will be summated as a float.

    Returns:
        float: The float value.
    """
    if isinstance(val, float):
        return val
    elif isinstance(val, Iterable):
        return float(val.sum())


def create_bin_rank_dict(
    data: np.array, number_of_items: int, max_bin_size: int = 3, reverse: bool = False, rounding: int = 3
) -> dict:
    """Create a dictionary of bin value: bin rank key: value pairs.

    Args:
        data (np.array): The data that you want to create bins for.
        number_of_items: The number of items that determines the minium number of bins you want to create.
        max_bin_size (int, optional): The max number of bins you want to create. Defaults to 3.
        reverse (bool, optional): Reverse the enumeration of the bins (descending rather than ascending order). Defaults to False.
        rounding (int, optional): Optionally round the numbers for the bin groups. Defaults to 3.

    Returns:
        dict: A dictionary of bin value: bin rank.
    """

    bins = min(number_of_items, max_bin_size)
    bins = np.linspace(data.min(), data.max(), bins)
    digitized = np.digitize(data, bins)
    new_data_list = [data[digitized == i].mean() for i in range(1, len(bins))]
    new_data_list = [round(x, rounding) for x in new_data_list]
    if reverse:
        new_data_list.reverse()
    return enumerate_iterable(new_data_list)


def return_bin_rank(x: float, bin_dict: dict) -> float:
    """Return the matching bin rank from a bin_rank dictionary created in the `bin_rank_dict` function.

    Args:
        x (float): The raw value to check against the bin_dict
        bin_dict (dict): The bin_rank_dict object.

    Raises:
        ValueError: Raises error if the value entered if the value `x` is outside of the bin_dict range.

    Returns:
        float: Returns a float of the rank value.
    """
    bin_dict_vals = list(bin_dict.keys())
    if x < bin_dict_vals[0]:
        raise ValueError(
            f"Value provided {x} is smaller than the initial bin size {bin_dict_vals[0]}"
        )
    elif x > bin_dict_vals[-1]:
        raise ValueError(
            f"Value provided {x} is bigger than the last bin size {bin_dict_vals[-1]}"
        )
    else:
        for val in bin_dict_vals:
            if x <= val:
                return bin_dict[val]


def replace_dict_items(base_dict: dict, repl_dict: dict):
    base_dict_c = deepcopy(base_dict)
    for col_entry in repl_dict:
        if col_entry in base_dict_c:
            base_dict_c[col_entry] = repl_dict[col_entry]
    return base_dict_c


def get_dict_keys_by_value(base_dict: dict, value):
    item_list = base_dict.items()
    return [item[0] for item in item_list if item[1] == value]


def multiprocessing_scenarios(scenario_options: list, func):
    # Multiprocessing
    virtual_cores = len(scenario_options)
    n_cores = mp.cpu_count()
    logger.info(f"{n_cores} cores detected, creating {virtual_cores} virtual cores")
    pool = mp.Pool(processes=virtual_cores)

    # Model flow - Load reusable data
    for scenario in scenario_options:
        # run the multiprocessing pool over the cores
        pool.apply_async(func, args=(scenario, True))

    # close and join the pools
    pool.close()
    pool.join()


def join_list_as_string(list_object: list) -> str:
    return ", ".join(list_object)


def decades_between_dates(year_range: range, include_final_year: bool = False) -> set:
    decades_set = [year - (year%10) for year in list(year_range)]
    if include_final_year:
        decades_set.append(year_range[-1])
    return set(decades_set)


def get_closest_number_in_list(my_list: list, my_number: int):
    return min(my_list, key = lambda x: abs(x - my_number)) if my_list else None
