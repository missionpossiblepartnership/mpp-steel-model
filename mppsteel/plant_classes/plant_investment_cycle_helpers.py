"""Script with function to manipulate the PlantInvestmentCycle Class."""

import random
from typing import Dict, List, Sequence, Tuple, Union

import pandas as pd
from copy import deepcopy

from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    MODEL_YEAR_END,
    MODEL_YEAR_RANGE,
    NET_ZERO_TARGET_YEAR,
    NET_ZERO_VARIANCE_YEARS,
    INVESTMENT_CYCLE_VARIANCE_YEARS,
    INVESTMENT_OFFCYCLE_BUFFER_TOP,
    INVESTMENT_OFFCYCLE_BUFFER_TAIL,
)
from mppsteel.config.mypy_config_settings import MYPY_NUMERICAL_AND_RANGE

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.utils import combine_and_order_list_and_range


logger = get_logger(__name__)


def calculate_investment_years(
    op_start_year: int,
    cycle_length: int,
    cutoff_start_year: int = MODEL_YEAR_START,
    cutoff_end_year: int = MODEL_YEAR_END,
) -> list:
    """Creates a list of investment decision years for a plant based on inputted parameters that determine the decision years.

    Args:
        op_start_year (int): The operating start year of the plant.
        cycle_length (int, optional): The standard interval of the investment decision cycle for plants.
        cutoff_start_year (int, optional): The initial year of the model. Defaults to MODEL_YEAR_START.
        cutoff_end_year (int, optional): The last year of the model. Defaults to MODEL_YEAR_END.

    Returns:
        list: A list of investment decision years.
    """
    x = op_start_year
    decision_years = []
    while net_zero_year_bring_forward(x) <= cutoff_end_year:
        x = net_zero_year_bring_forward(x)
        if x >= cutoff_start_year:
            decision_years.append(x)
        x += cycle_length
    return decision_years


def return_cycle_length(
    inv_intervals, investment_cycle_randomness: bool = False
) -> int:
    """Returns a new cycle length based on a fixed value and a random value within a predefined inveral.

    Args:
        inv_intervals (int): A fixed interval value aroud which the final interval will fluctuate.
        investment_cycle_randomness (bool): Switch to turn on randomness in range.

    Returns:
        int: A final interval value.
    """
    cycle_length = inv_intervals
    if investment_cycle_randomness:
        cycle_length = inv_intervals + random.randrange(
            -INVESTMENT_CYCLE_VARIANCE_YEARS, INVESTMENT_CYCLE_VARIANCE_YEARS, 1
        )
    return cycle_length


def return_switch_type(investment_cycle: list, year: int) -> str:
    """Returns a string based on the relation of a specified `year` to the `investment_cycle`.
    There are three possible values `main cycle`, `trans switch` or `no switch`.

    Args:
        investment_cycle (list): The investment cycle for a given plant.
        year (int): The year to base the switch type evaluation on.

    Returns:
        str: A string of the switch type.
    """
    off_cycle_years = [
        inv_range for inv_range in investment_cycle if isinstance(inv_range, range)
    ]
    main_cycle_years = [
        inv_year for inv_year in investment_cycle if isinstance(inv_year, int)
    ]
    if year in main_cycle_years:
        return "main cycle"
    for range_object in off_cycle_years:
        if year in range_object:
            return "trans switch"
    return "no switch"


def net_zero_year_bring_forward(year: int) -> int:
    """Determines whether an investment year should be brought forward to be within the acceptable range to become net zero.

    Args:
        year (int): The year to be considered for a readjustment.

    Returns:
        int: The adjusted year that is within the net zero target range.
    """
    if year in range(
        NET_ZERO_TARGET_YEAR, NET_ZERO_TARGET_YEAR + NET_ZERO_VARIANCE_YEARS + 1
    ):
        return NET_ZERO_TARGET_YEAR - 1
    return year


def add_off_cycle_investment_years(
    main_investment_cycle: Sequence,
    start_buff: int = INVESTMENT_OFFCYCLE_BUFFER_TOP,
    end_buff: int = INVESTMENT_OFFCYCLE_BUFFER_TAIL,
) -> list:
    """Adds a set of off-cycle investment years to an investment decision list.

    Args:
        main_investment_cycle (Sequence): The list of main investment decision years.
        start_buff (int): Determines the minimum number of years after a main investment decision until an off-cycle investment can be made.
        end_buff (int): Determines the number of years prior to the next investment decision that signifies the cutoff point that off-cycle investment decisions can no longer be made.

    Returns:
        list: An enhanced investment decision cycle list including off-cycle range objects representing potential off-cycle switches.
    """
    inv_cycle_length = len(main_investment_cycle)
    range_list: List[Union[int, range]] = []

    # For inv_cycle_length = 0
    if inv_cycle_length == 0:
        return range_list

    # For inv_cycle_length >= 1
    first_year = main_investment_cycle[0]

    # Add initial transitional switch window
    if first_year - end_buff > MODEL_YEAR_START:
        initial_range = range(MODEL_YEAR_START, first_year - end_buff)
        range_list.append(initial_range)

    range_list.append(first_year)

    if inv_cycle_length > 1:
        for index in range(1, inv_cycle_length):
            inv_year = main_investment_cycle[index]
            start_year = main_investment_cycle[index - 1] + start_buff
            end_year = inv_year - end_buff
            if start_year < NET_ZERO_TARGET_YEAR < end_year:
                end_year = NET_ZERO_TARGET_YEAR
            range_object = range(start_year, end_year)
            range_list.append(range_object)
            range_list.append(inv_year)
    return range_list


def create_investment_cycle_reference(plant_investment_year_dict: dict) -> pd.DataFrame:
    """Creates an Investment cycle DataFrame from a plant DataFrame, and a list of main cycle and off-cycle investment years.

    Args:
        plant_investment_year_dict (dict): A list of investment years - main cycle years as integers, transitional switch year ranges as range objects.

    Returns:
        pd.DataFrame: Creates a Dataframe based on a plant investment dictionary.
    """
    df_list = []
    for plant_name, investment_cycle in plant_investment_year_dict.items():
        off_cycle_years = [
            inv_range for inv_range in investment_cycle if isinstance(inv_range, range)
        ]
        main_cycle_years = [
            inv_year for inv_year in investment_cycle if isinstance(inv_year, int)
        ]
        for year in MODEL_YEAR_RANGE:
            if year in main_cycle_years:
                entry = {
                    "plant_name": plant_name,
                    "year": year,
                    "switch_type": "main cycle",
                }
                df_list.append(entry)
            elif any(
                [
                    True if year in range_object else False
                    for range_object in off_cycle_years
                ]
            ):
                entry = {
                    "plant_name": plant_name,
                    "year": year,
                    "switch_type": "trans switch",
                }
                df_list.append(entry)
            else:
                entry = {
                    "plant_name": plant_name,
                    "year": year,
                    "switch_type": "no switch",
                }
                df_list.append(entry)
    if len(df_list) == 0:
        return pd.DataFrame(columns=["year", "plant_name"]).set_index(
            ["year", "plant_name"]
        )
    return pd.DataFrame(df_list).set_index(["year", "plant_name"])


def extract_tech_plant_switchers(
    plant_investment_year_dict: pd.DataFrame, active_plants: list, year: int
) -> Union[list, Tuple[list, list, list, list]]:
    """Extracts the list of plants that are due for a main cycle switch or a transitional switch in a given year according to an investment cycle DataFrame.

    Args:
        plant_investment_year_dict (pd.DataFrame): DataFrame containing the investment cycle reference for each plant.
        active_plants (list): The list of active plants to create references for.
        year (int): The year to extract the plant switchers for.

    Returns:
        Union[list, Tuple[list, list, list, list]]: Returns multiple lists based on the outputs of
        main_cycle_switchers: all plants with main cycle switches in the specified year
        trans_cycle_switchers: all plants with transitional switches in the specified year
        non_switchers: all plants that aren't switching at all in the specified year
        combined_switchers: all the plants in main_cycle_switchers + trans_cycle_switchers
    """
    main_cycle_switchers = []
    trans_cycle_switchers = []
    adjusted_plant_investment_dict = {
        plant_name: plant_investment_year_dict[plant_name]
        for plant_name in active_plants
    }
    for plant_name, investment_cycle in adjusted_plant_investment_dict.items():
        off_cycle_years = [
            inv_range for inv_range in investment_cycle if isinstance(inv_range, range)
        ]
        main_cycle_years = [
            inv_year for inv_year in investment_cycle if isinstance(inv_year, int)
        ]
        if year in main_cycle_years:
            main_cycle_switchers.append(plant_name)
        for range_object in off_cycle_years:
            if year in range_object:
                trans_cycle_switchers.append(plant_name)
    combined_switchers = main_cycle_switchers + trans_cycle_switchers
    non_switchers = []
    for plant_name in adjusted_plant_investment_dict:
        if plant_name not in combined_switchers:
            non_switchers.append(plant_name)
    return (
        main_cycle_switchers,
        trans_cycle_switchers,
        non_switchers,
        combined_switchers,
    )


def adjust_transitional_switch_in_investment_cycle(
    cycle_years: MYPY_NUMERICAL_AND_RANGE, rebase_year: int
) -> MYPY_NUMERICAL_AND_RANGE:
    """Adjusts the investment cycle when a plan decides to undertake a transitional switch away from its base technology.
    The adjustment removes the possibility of an additional transitional switch before its next main investment cycle.

    Args:
        cycle_years (list): The initial investment cycle years
        rebase_year (int): The year to rebase the investment cycle to.

    Returns:
        list: The rebased investment cycle.
    """
    inv_ranges_list: List[range] = [
        year_obj for year_obj in cycle_years if isinstance(year_obj, range)
    ]
    if not inv_ranges_list:
        return cycle_years
    inv_range_match: List[range] = [
        matching_range
        for matching_range in inv_ranges_list
        if rebase_year in matching_range
    ]
    if not inv_range_match:
        return cycle_years
    matching_range: range = inv_range_match[0]
    index_position = cycle_years.index(matching_range)
    new_list = deepcopy(cycle_years)
    new_list[index_position] = range(list(matching_range)[0], rebase_year)
    return new_list


def adjust_cycles_for_first_year(plant_cycles: dict) -> dict:
    """Adjusts the investment cycles to ensure that the initial model year will not be an investment cycle switch (main or transitional)

    Args:
        plant_cycles (dict): A dictionary of the investment cycles: plants as keys, cycles as values.

    Returns:
        dict: A dictionary containing the updated plant cycles.
    """
    new_plant_cycles: Dict[str, MYPY_NUMERICAL_AND_RANGE] = {}
    for plant_name, plant_cycle in plant_cycles.items():
        new_cycle: MYPY_NUMERICAL_AND_RANGE = []
        for obj in plant_cycle:
            if isinstance(obj, int):
                if obj == MODEL_YEAR_START:
                    new_cycle.append(MODEL_YEAR_START + 1)
                else:
                    new_cycle.append(obj)
            elif isinstance(obj, range):
                if MODEL_YEAR_START in obj:
                    new_cycle.append(
                        range(
                            MODEL_YEAR_START + 1, min(obj[-1] + 1, MODEL_YEAR_END - 1)
                        )
                    )
                else:
                    new_cycle.append(obj)
        new_plant_cycles[plant_name] = new_cycle
    return new_plant_cycles


def increment_investment_cycle_year(
    cycle_years: list, rebase_year: int, increment_amount: int = 1
) -> list:
    """Adjusts the investment cycle when a plant has to postpone its investment year. Every year in the cycle is incremented by `increment_amount`.

    Args:
        cycle_years (list): The initial investment cycle years
        rebase_year (int): The year to rebase the investment cycle to.
        increment_amount (int): The amount to increment the investment_years by

    Returns:
        list: The rebased investment cycle.
    """
    years = [year_obj for year_obj in cycle_years if isinstance(year_obj, int)]
    if years:
        years = [
            net_zero_year_bring_forward(year + increment_amount)
            if year >= rebase_year
            else year
            for year in years
        ]
    ranges = [year_obj for year_obj in cycle_years if isinstance(year_obj, range)]
    if not ranges:
        return years
    new_range_list = []
    for range_obj in ranges:
        first_year = range_obj[0]
        last_year = range_obj[-1]
        first_year_incremented = net_zero_year_bring_forward(
            first_year + increment_amount
        )
        last_year_incremented = net_zero_year_bring_forward(
            last_year + increment_amount + 1
        )
        new_range_obj = range_obj
        if last_year < rebase_year:
            new_range_list.append(range_obj)
        elif first_year >= rebase_year:
            new_range_obj = range(first_year_incremented, last_year_incremented)
        elif first_year <= rebase_year <= last_year:
            new_range_obj = range(first_year, last_year_incremented)
        new_range_list.append(new_range_obj)
    return combine_and_order_list_and_range(years, new_range_list)
