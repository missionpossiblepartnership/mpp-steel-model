"""Script to determine when investments will take place."""

import random
from typing import Tuple, Union

import pandas as pd
from copy import deepcopy

from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    MODEL_YEAR_END,
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED,
    NET_ZERO_TARGET,
    NET_ZERO_VARIANCE_YEARS,
    INVESTMENT_CYCLE_DURATION_YEARS,
    INVESTMENT_CYCLE_VARIANCE_YEARS,
    INVESTMENT_OFFCYCLE_BUFFER_TOP,
    INVESTMENT_OFFCYCLE_BUFFER_TAIL,
)

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger

# Create logger
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
    while x < cutoff_end_year:
        if x >= cutoff_start_year:
            decision_years.append(x)
        x += cycle_length
    return decision_years


def return_cycle_length(inv_intervals) -> int:
    """Returns a new cycle length based on a fixed value and a random value within a predefined inveral.

    Args:
        inv_intervals (int): A fixed interval value aroud which the final interval will fluctuate.

    Returns:
        int: A final interval value.
    """
    return inv_intervals + random.randrange(
        -INVESTMENT_CYCLE_VARIANCE_YEARS, INVESTMENT_CYCLE_VARIANCE_YEARS, 1
    )


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


def add_off_cycle_investment_years(
    main_investment_cycle: list,
    start_buff: int = INVESTMENT_OFFCYCLE_BUFFER_TOP,
    end_buff: int = INVESTMENT_OFFCYCLE_BUFFER_TAIL,
) -> list:
    """Adds a set of off-cycle investment years to an investment decision list.

    Args:
        main_investment_cycle (list): The list of main investment decision years.
        start_buff (int): Determines the minimum number of years after a main investment decision until an off-cycle investment can be made.
        end_buff (int): Determines the number of years prior to the next investment decision that signifies the cutoff point that off-cycle investment decisions can no longer be made.

    Returns:
        list: An enhanced investment decision cycle list including off-cycle range objects representing potential off-cycle switches.
    """
    inv_cycle_length = len(main_investment_cycle)
    range_list = []

    def net_zero_year_bring_forward(year: int) -> int:
        """Determines whether an investment year should be brought forward to be within the acceptable range to become net zero.

        Args:
            year (int): The year to be considered for a readjustment.

        Returns:
            int: The adjusted year that is within the net zero target range.
        """
        if year in range(
            NET_ZERO_TARGET + 1, NET_ZERO_TARGET + NET_ZERO_VARIANCE_YEARS + 1
        ):
            bring_forward_date = NET_ZERO_TARGET - 1
            logger.info(f"Investment Cycle Brought Forward to {bring_forward_date}")
            return bring_forward_date
        return year

    # For inv_cycle_length = 0
    if inv_cycle_length == 0:
        return range_list

    # For inv_cycle_length >= 1
    first_year = net_zero_year_bring_forward(main_investment_cycle[0])

    # Add initial transitional switch window
    if first_year - end_buff > MODEL_YEAR_START:
        initial_range = range(MODEL_YEAR_START, first_year - end_buff)
        range_list.append(initial_range)

    range_list.append(first_year)

    if inv_cycle_length > 1:
        for index in range(1, inv_cycle_length):
            inv_year = net_zero_year_bring_forward(main_investment_cycle[index])
            range_object = range(
                main_investment_cycle[index - 1] + start_buff, inv_year - end_buff
            )
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


def adjust_investment_cycle_dict(cycle_years: list, rebase_year: int) -> list:
    """Adjusts the investment cycle when a plan decides to undertake a transitional switch away from its base technology.
    The adjustment removes the possibility of an additional transitional switch before its next main investment cycle.

    Args:
        cycle_years (list): The initial investment cycle years
        rebase_year (int): The year to rebase the investment cycle to.

    Returns:
        list: The rebased investment cycle.
    """
    inv_ranges_list = [
        year_obj for year_obj in cycle_years if isinstance(year_obj, range)
    ]
    if not inv_ranges_list:
        return cycle_years
    inv_range_match = [
        matching_range
        for matching_range in inv_ranges_list
        if rebase_year in matching_range
    ]
    if not inv_range_match:
        return cycle_years
    matching_range = inv_range_match[0]
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
    new_plant_cycles = {}
    for plant_name, plant_cycle in plant_cycles.items():
        new_cycle = []
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


class PlantInvestmentCycle:
    """Class for managing the the investment cycles for plants."""

    def __init__(self):
        self.plant_names = []
        self.plant_start_years = {}
        self.plant_investment_cycle_length = {}
        self.plant_cycles = {}
        self.plant_cycles_with_off_cycle = {}

    def instantiate_plants(self, plant_names: list, plant_start_years: list):
        self.plant_names = plant_names
        start_year_dict = dict(zip(plant_names, plant_start_years))
        for plant_name in self.plant_names:
            self.plant_start_years[plant_name] = start_year_dict[plant_name]
            self.plant_investment_cycle_length[plant_name] = return_cycle_length(
                INVESTMENT_CYCLE_DURATION_YEARS
            )
            self.plant_cycles[plant_name] = calculate_investment_years(
                self.plant_start_years[plant_name],
                self.plant_investment_cycle_length[plant_name],
            )
            self.plant_cycles_with_off_cycle[
                plant_name
            ] = add_off_cycle_investment_years(self.plant_cycles[plant_name])
        self.plant_cycles_with_off_cycle = adjust_cycles_for_first_year(
            self.plant_cycles_with_off_cycle
        )

    def add_new_plants(self, plant_names: list, plant_start_years: list):
        new_dict = dict(zip(plant_names, plant_start_years))
        for plant_name in plant_names:
            self.plant_names.append(plant_name)
            self.plant_start_years[plant_name] = new_dict[plant_name]
            self.plant_investment_cycle_length[plant_name] = return_cycle_length(
                INVESTMENT_CYCLE_DURATION_YEARS
            )
            self.plant_cycles[plant_name] = calculate_investment_years(
                self.plant_start_years[plant_name],
                self.plant_investment_cycle_length[plant_name],
            )
            self.plant_cycles_with_off_cycle[
                plant_name
            ] = add_off_cycle_investment_years(self.plant_cycles[plant_name])

    def adjust_cycle_for_transitional_switch(self, plant_name: str, rebase_year: int):
        new_cycle = adjust_investment_cycle_dict(
            self.plant_cycles_with_off_cycle[plant_name], rebase_year
        )
        self.plant_cycles_with_off_cycle[plant_name] = new_cycle

    def create_investment_df(self):
        return create_investment_cycle_reference(self.plant_cycles_with_off_cycle)

    def return_plant_switch_type(self, plant_name: str, year: int):
        return return_switch_type(self.plant_cycles_with_off_cycle[plant_name], year)

    def return_investment_dict(self):
        return self.plant_cycles_with_off_cycle

    def return_cycle_lengths(self, plant_name: str = None):
        return (
            self.plant_investment_cycle_length[plant_name]
            if plant_name
            else self.plant_investment_cycle_length
        )

    def return_plant_switchers(self, active_plants: list, year: int, value_type: str):
        (
            main_cycle_switchers,
            trans_cycle_switchers,
            non_switchers,
            combined_switchers,
        ) = extract_tech_plant_switchers(
            self.plant_cycles_with_off_cycle, active_plants, year
        )
        if value_type == "main cycle":
            return main_cycle_switchers
        elif value_type == "trans switch":
            return trans_cycle_switchers
        elif value_type == "no switch":
            return non_switchers
        elif value_type == "combined":
            return combined_switchers


@timer_func
def investment_cycle_flow(serialize: bool = False) -> pd.DataFrame:
    """Inintiates the complete investment cycle flow and serializes the resulting DataFrame.

    Args:
        serialize (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the Complete Investment Decision Cycle Reference.
    """
    steel_plant_df = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )

    PlantInvestmentCycles = PlantInvestmentCycle()
    steel_plant_names = steel_plant_df["plant_name"].to_list()
    start_plant_years = steel_plant_df["start_of_operation"].to_list()
    PlantInvestmentCycles.instantiate_plants(steel_plant_names, start_plant_years)

    if serialize:
        logger.info("-- Serializing Investment Cycle Reference")
        serialize_file(
            PlantInvestmentCycles,
            PKL_DATA_FORMATTED,
            "plant_investment_cycle_container",
        )
    return PlantInvestmentCycles
