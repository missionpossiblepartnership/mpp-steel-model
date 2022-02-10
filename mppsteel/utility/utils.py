"""Utility library for functions used throughout the module"""
import itertools
import sys

from collections.abc import Iterable
from datetime import datetime

from currency_converter import CurrencyConverter

from mppsteel.utility.log_utility import get_logger

logger = get_logger("Utils")


def get_today_time() -> str:
    return datetime.today().strftime("%y%m%d_%H%M%S")


def create_list_permutations(list1: list, list2: list) -> list:
    comb = [
        list(zip(each_permutation, list2))
        for each_permutation in itertools.permutations(list1, len(list2))
    ]
    return list(itertools.chain(*comb))


def stdout_query(question: str, default: str, options: str) -> None:
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


def get_currency_rate(base: str) -> str:
    logger.info(f"Getting currency exchange rate for {base}")
    c = CurrencyConverter()
    if base.lower() == "usd":
        return c.convert(1, "USD", "EUR")
    if base.lower() == "eur":
        return c.convert(1, "EUR", "USD")


def enumerate_iterable(iterable: list) -> dict:
    return dict(zip(iterable, range(len(iterable))))


def cast_to_float(val) -> float:
    if isinstance(val, float):
        return val
    elif isinstance(val, Iterable):
        return float(val.sum())
