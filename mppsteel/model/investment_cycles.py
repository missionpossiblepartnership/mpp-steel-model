"""Script to determine when investments will take place."""

import random
import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    MODEL_YEAR_END,
    PKL_DATA_INTERMEDIATE,
    PKL_DATA_IMPORTS,
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
logger = get_logger("Investment Cycles")


def calculate_investment_years(
    op_start_year: int,
    cutoff_start_year: int = MODEL_YEAR_START,
    cutoff_end_year: int = MODEL_YEAR_END,
    inv_intervals: int = INVESTMENT_CYCLE_DURATION_YEARS,
) -> list:
    """Creates a list of investment decision years for a plant based on inputted parameters that determine the decision years.

    Args:
        op_start_year (int): The operating start year of the plant.
        cutoff_start_year (int, optional): The initial year of the model. Defaults to MODEL_YEAR_START.
        cutoff_end_year (int, optional): The last year of the model. Defaults to MODEL_YEAR_END.
        inv_intervals (int, optional): The standard interval of the investment decision cycle for plants. Defaults to INVESTMENT_CYCLE_DURATION_YEARS.

    Returns:
        list: A list of investment decision years.
    """
    x = op_start_year
    decision_years = []
    unique_investment_interval = inv_intervals + random.randrange(
        -INVESTMENT_CYCLE_VARIANCE_YEARS, INVESTMENT_CYCLE_VARIANCE_YEARS, 1
    )
    while x < cutoff_end_year:
        if x >= cutoff_start_year:
            decision_years.append(x)
        x += unique_investment_interval
    return decision_years


def add_off_cycle_investment_years(
    main_investment_cycle: list,
    start_buff: int,
    end_buff: int,
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
        if year in range(NET_ZERO_TARGET + 1, NET_ZERO_TARGET + NET_ZERO_VARIANCE_YEARS + 1):
            bring_forward_date = NET_ZERO_TARGET - 1
            logger.info(f"Investment Cycle Brought Forward to {bring_forward_date}")
            return bring_forward_date
        return year

    # For inv_cycle_length = 1
    first_year = net_zero_year_bring_forward(main_investment_cycle[0])
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


def apply_investment_years(year_value: int) -> list:
    """Formats the operating start date column values from and applies the calculate_investment_years function.

    Args:
        year_value (int): The raw operating start date column from plant data.

    Returns:
        list: The list of investment decision years.
    """
    if pd.isna(year_value):
        return calculate_investment_years(MODEL_YEAR_START)
    elif "(anticipated)" in str(year_value):
        year_value = year_value[:4]
        return calculate_investment_years(int(year_value))
    else:
        try:
            return calculate_investment_years(int(float(year_value)))
        except:
            return calculate_investment_years(int(year_value[:4]))


def create_investment_cycle_reference(
    plant_names: list, investment_years: list, year_end: int
) -> pd.DataFrame:
    """Creates an Investment cycle DataFrame from a plant DataFrame, and a list of main cycle and off-cycle investment years.

    Args:
        plant_names (list): A list of plant names.
        investment_years (list): A list of investment years - main cycle years as integers, transitional switch year ranges as range objects.
        year_end (int): The last year of the model.

    Returns:
        pd.DataFrame: _description_
    """
    logger.info("Creating the investment cycle reference table")
    zipped_plant_investments = zip(plant_names, investment_years)
    year_range = range(MODEL_YEAR_START, year_end + 1)
    df = pd.DataFrame(columns=["plant_name", "year", "switch_type"])

    for plant_name, investments in tqdm(
        zipped_plant_investments, total=len(plant_names), desc="Investment Cycles"
    ):
        for year in year_range:
            row_dict = {
                "plant_name": plant_name,
                "year": year,
                "switch_type": "no switch",
            }
            if year in [
                inv_year for inv_year in investments if isinstance(inv_year, int)
            ]:
                row_dict["switch_type"] = "main cycle"
            for range_object in [
                inv_range for inv_range in investments if isinstance(inv_range, range)
            ]:
                if year in range_object:
                    row_dict["switch_type"] = "trans switch"
            df = df.append(row_dict, ignore_index=True)
    return df.set_index(["year", "switch_type"])


def create_investment_cycle(steel_plant_df: pd.DataFrame) -> pd.DataFrame:
    """Full flow to create the investment decision cycle reference DataFrame.

    Args:
        steel_plant_df (pd.DataFrame): Steel Plant DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the Complete Investment Decision Cycle Reference.
    """
    logger.info("Creating investment cycle")
    investment_years = steel_plant_df["start_of_operation"].apply(
        lambda year: apply_investment_years(year)
    )
    investment_years_inc_off_cycle = [
        add_off_cycle_investment_years(
            inv_year, INVESTMENT_OFFCYCLE_BUFFER_TOP, INVESTMENT_OFFCYCLE_BUFFER_TAIL
        )
        for inv_year in investment_years
    ]
    return create_investment_cycle_reference(
        steel_plant_df["plant_name"].values,
        investment_years_inc_off_cycle,
        MODEL_YEAR_END,
    )


@timer_func
def investment_cycle_flow(serialize: bool = False) -> pd.DataFrame:
    """Inintiates the complete investment cycle flow and serializes the resulting DataFrame.

    Args:
        serialize (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the Complete Investment Decision Cycle Reference.
    """
    steel_plants_aug = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
    )
    plant_investment_cycles = create_investment_cycle(steel_plants_aug)

    if serialize:
        logger.info("-- Serializing Investment Cycle Reference")
        serialize_file(
            plant_investment_cycles, PKL_DATA_INTERMEDIATE, "plant_investment_cycles"
        )
    return plant_investment_cycles
