"""Module that generates a timeseries for various purposes"""
# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from mppsteel.utility.file_handling_utility import serialize_file, get_scenario_pkl_path
from mppsteel.utility.function_timer_utility import timer_func

# Get model parameters
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
)

from mppsteel.config.model_scenarios import CARBON_TAX_SCENARIOS, GREEN_PREMIUM_SCENARIOS
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Timeseries generator")

# Main timeseries function
def timeseries_generator(
    timeseries_type: str,
    start_year: int,
    end_year: int,
    end_value: float,
    start_value: float = 0,
    units: str = "",
) -> pd.DataFrame:
    """Function that generates a timeseries based on particular logic

    Args:
        timeseries_type (str): Defines the timeseries to produce. Options: Biomass, Carbon Tax.
        start_year (int): Defines the start date of the timeseries.
        end_year (int): Defines the end date of the timeseries.
        end_value (float): Defines the terminal value of the timeseries.
        start_value (float, optional): Defines the starting value of the timeseries. Defaults to 0.
        units (str, optional): [description]. Define units of the timeseries values. Defaults to ''.

    Returns:
        DataFrame: A DataFrame of the timeseries.
    """
    # Define schema for the DataFrame
    df_schema = {"year": int, "value": float, "units": str}
    # Define the year range for the df
    year_range = range(int(start_year), int(end_year + 1))
    # Create the DataFrame
    df = pd.DataFrame(
        index=pd.RangeIndex(0, len(year_range)),
        columns=[key.lower() for key in df_schema],
    )
    # Define the year columns
    df["year"] = year_range
    df["units"] = units
    df["units"] = df["units"].apply(lambda x: x.lower())

    def levy_logic(df: pd.DataFrame) -> pd.DataFrame:
        """Applies logic to generate carbon tax timeseries

        Args:
            df (pd.DataFrame): A dataframe with empty values column.

        Returns:
            pd.DataFrame: A dataframe with the value logic applied.
        """
        df_c = df.copy()
        for row in df_c.itertuples():
            if row.Index == 0:  # skip first year
                df_c.loc[row.Index, "value"] = start_value
            elif row.Index < len(year_range) - 1:
                # logic for remaining years except last year
                df_c.loc[row.Index, "value"] = (end_value / len(year_range)) * (
                    row.year - start_year
                )
            else:
                df_c.loc[row.Index, "value"] = end_value  # logic for last year
        return df_c

    # Setting values: BUSINESS LOGIC
    logger.info(f"Running {timeseries_type} timeseries generator")
    if timeseries_type == "carbon_tax":
        df = levy_logic(df)
        df["units"] = "EUR / t CO2 eq"
    if timeseries_type == "green_premium":
        df = levy_logic(df)
        df["units"] = "EUR / t steel"
    # change the column types
    for key in df_schema.keys():
        df[key].astype(df_schema[key])
    logger.info(f"{timeseries_type} timeseries complete")
    return df


@timer_func
def generate_timeseries(scenario_dict: dict = None, serialize: bool = False) -> dict:
    """Generates timeseries for biomass, carbon taxes and electricity.

    Args:
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dict containing dataframes with the following keys: 'biomass', 'carbon_tax', 'electricity'.
    """
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    carbon_tax_scenario_values = CARBON_TAX_SCENARIOS[
        scenario_dict["carbon_tax_scenario"]
    ]
    green_premium_scenario_values = GREEN_PREMIUM_SCENARIOS[
        scenario_dict["green_premium_scenario"]
    ]
    # Create Carbon Tax timeseries
    carbon_tax_timeseries = timeseries_generator(
        "carbon_tax",
        MODEL_YEAR_START,
        MODEL_YEAR_END,
        carbon_tax_scenario_values[1],
        carbon_tax_scenario_values[0],
    )
    green_premium_timeseries = timeseries_generator(
        "green_premium",
        MODEL_YEAR_START,
        MODEL_YEAR_END,
        green_premium_scenario_values[1],
        green_premium_scenario_values[0],
    )

    if serialize:
        # Serialize timeseries
        serialize_file(
            carbon_tax_timeseries, intermediate_path, "carbon_tax_timeseries"
        )
        serialize_file(
            green_premium_timeseries, intermediate_path, "green_premium_timeseries"
        )

    return {
        "carbon_tax_timeseries": carbon_tax_timeseries,
        "green_premium_timeseries": green_premium_timeseries,
    }
