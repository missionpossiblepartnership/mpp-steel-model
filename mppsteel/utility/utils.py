"""Utility library for functions used throughout the module"""
import itertools
import sys

import numpy as np
import numpy.typing as npt
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from typing import Any, List, Sequence, Sized, Union

from currency_converter import CurrencyConverter
from mppsteel.config.model_config import NUMBER_OF_TECHNOLOGIES_PER_BIN_GROUP

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def get_today_time(fmt: str = "%y%m%d_%H%M%S") -> str:
    """Returns a formatted string of todays date.

    Args:
        The format you would like the datetime object to take.

    Returns:
        str: A string with today's date.
    """
    return datetime.now().strftime(fmt)


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


def get_currency_rate(base: str, target: str) -> float:
    """Gets a currency rate exchange between two currencies using the CurrencyConverter library.

    Args:
        base (str): [description]
        target (str): [description]

    Returns:
        None: A None object
    """
    logger.info(f"Getting currency exchange rate for {base}")
    conversion_rate = 1
    if (len(base) == 3) & (len(target) == 3):
        try:
            curr = CurrencyConverter()
            conversion_rate = curr.convert(1, base.upper(), target.upper())
        except:
            raise ValueError(
                f"You entered an incorrect currency, either {base} or {target}"
            )
    return conversion_rate


def enumerate_iterable(iterable: Sized) -> dict:
    """Enumerates an iterable as dictionary with the iterable value as the key and the order number as the value.

    Args:
        iterable (Sized): The iterable you want to enumerate

    Returns:
        dict: A dictionary with the the iterable value as the key and the order number as the value.
    """
    return dict(zip(iterable, range(len(iterable))))


def cast_to_float(val: Union[float, int, Iterable]) -> float:
    """Casts a numerical object to a float if not a float already.

    Args:
        val Union[float, int, Iterable[float]]): The numerical value you want to be a float. Can be an iterable containing a numberical value(s), that will be summated as a float.

    Returns:
        float: The float value.
    """
    return float(val.sum()) if isinstance(val, Iterable) else val


def create_bin_rank_dict(
    data: np.ndarray, number_of_items: int, reverse: bool = False, rounding: int = 3
) -> dict:
    """Create a dictionary of bin value: bin rank key: value pairs.

    Args:
        data (np.ndarray): The data that you want to create bins for.
        number_of_items: The number of items that determines the minium number of bins you want to create.
        reverse (bool, optional): Reverse the enumeration of the bins (descending rather than ascending order). Defaults to False.
        rounding (int, optional): Optionally round the numbers for the bin groups. Defaults to 3.

    Returns:
        dict: A dictionary of bin value: bin rank.
    """
    # max_bin_size = math.floor(number_of_items / NUMBER_OF_TECHNOLOGIES_PER_BIN_GROUP)
    max_bin_size = 3
    bins = min(number_of_items, max_bin_size)
    bins_linspaced: Sized = np.linspace(start=data.min(), stop=data.max(), num=bins)
    digitized = np.digitize(data, bins_linspaced)
    new_data_list = [data[digitized == i].mean() for i in range(1, len(bins_linspaced))]
    new_data_list = [round(x, rounding) for x in new_data_list]
    if reverse:
        new_data_list.reverse()
    return enumerate_iterable(new_data_list)


def return_bin_rank(x: float, bin_dict: dict) -> Union[float, None]:
    """Return the matching bin rank from a bin_rank dictionary created in the `bin_rank_dict` function.

    Args:
        x (float): The raw value to check against the bin_dict
        bin_dict (dict): The bin_rank_dict object.

    Raises:
        ValueError: Raises error if the value entered if the value `x` is outside of the bin_dict range.

    Returns:
        Union[float, None]: Returns a float of the rank value, else None.
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
    for val in bin_dict_vals:
            if x <= val:
                return bin_dict[val]
    return None


def replace_dict_items(base_dict: dict, replacement_dict: dict) -> dict:
    """Replaces certain key, value pairs in a dictionary with those from another dictionary, where the keys match.

    Args:
        base_dict (dict): The base dictionary.
        replacement_dict (dict): The replacement dictionary

    Returns:
        dict: The modified dictionary with the updated key, value pairs.
    """
    base_dict_c = deepcopy(base_dict)
    for col_entry in replacement_dict:
        if col_entry in base_dict_c:
            base_dict_c[col_entry] = replacement_dict[col_entry]
    return base_dict_c


def get_dict_keys_by_value(base_dict: dict, value) -> list:
    """Returns the dictionary key values that map to a specified value.

    Args:
        base_dict (dict): The base dictionary containing the values to check against.
        value (_type_): The value that will be checked against the key, value pairs in base_dict.

    Returns:
        list: A list containing the matching keys.
    """
    return [item[0] for item in base_dict.items() if item[1] == value]


def join_list_as_string(list_object: list, sep: str = ",") -> str:
    """Join elements of a list into a string object.

    Args:
        list_object (list): The list to join as a string.
        sep (str): The separator that will separate the list_object.

    Returns:
        str: A str with all elements in list_object joined separated by sep.
    """
    return f"{sep} ".join(list_object)


def reverse_dict_with_list_elements(dict_to_reverse: dict) -> dict:
    """Reverse a dictionary that has value as a list, where each element in the list becomes a key, and the old keys become values.

    Args:
        dict_to_reverse (dict): The dictionary with list values that you want to reverse.

    Returns:
        dict: A modified dictionary.
    """
    new_dict = {}
    for key in dict_to_reverse:
        for elem in dict_to_reverse[key]:
            new_dict[elem] = key
    return new_dict


def decades_between_dates(year_range: range, include_final_year: bool = False) -> set:
    """Returns a set of the decades between a range of dates.

    Args:
        year_range (range): A range of dates.
        include_final_year (bool, optional): Flag to determine whether to include the final year of year_range whether it is a decade or not. Defaults to False.

    Returns:
        set: The set of decade years.
    """
    decades_set = [year - (year % 10) for year in list(year_range)]
    if include_final_year:
        decades_set.append(year_range[-1])
    return set(decades_set)


def get_closest_number_in_list(my_list: list, my_number: int) -> Union[int, None]:
    """Returns the number closest to another number in list of numbers.

    Args:
        my_list (list): A list of values.
        my_number (int): The number you want to return the closest element in my_list.

    Returns:
        Union[int, None]: The closest number in my_list. If my_list is empty, will return None.
    """
    return min(my_list, key=lambda x: abs(x - my_number)) if my_list else None


def split_list_into_chunks(seq: Sequence[Any], n: int) -> list:
    """Splits a list into smaller lists of predetermined length.

    Args:
        seq (Sequence): A sequence with any elements.
        n (int): The predetermined size of each smaller list chunk.

    Returns:
        list: A list of length n, with each element as a smaller lists.
    """
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def get_intersection_of_ordered_list(
    ordered_list: Iterable, mapping_list: Iterable
) -> list:
    """Return values from a list in the order of a different list.

    Args:
        ordered_list (Iterable): The list in the order of that you want to subset the mapping_list.
        mapping_list (Iterable): The list that you want to subset with ordered_list.

    Returns:
        list: A list of ordered values at the intersection of ordered_list and mapping_list.
    """
    return [x for x in mapping_list if x in frozenset(ordered_list)]
