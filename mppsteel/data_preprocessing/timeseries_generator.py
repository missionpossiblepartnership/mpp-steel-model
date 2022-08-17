"""Module that generates a timeseries for various purposes"""
# For Data Manipulation
from typing import Union
import pandas as pd
from mppsteel.utility.dataframe_utility import convert_currency_col, extend_df_years

# For logger and units dict
from mppsteel.utility.file_handling_utility import return_pkl_paths, serialize_file
from mppsteel.utility.function_timer_utility import timer_func

# Get model parameters
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    CARBON_TAX_START_YEAR,
    GREEN_PREMIUM_START_YEAR,
    CARBON_TAX_END_YEAR,
    GREEN_PREMIUM_END_YEAR,
)

from mppsteel.config.model_scenarios import (
    CARBON_TAX_SCENARIOS,
    GREEN_PREMIUM_SCENARIOS,
)
from mppsteel.utility.log_utility import get_logger


logger = get_logger(__name__)

# Main timeseries function
def timeseries_generator(
    timeseries_type: str,
    start_year: int,
    end_year: int,
    series_start_year: int,
    end_value: float,
    start_value: float = 0,
    units: str = "",
    extension_year: int = None,
) -> pd.DataFrame:
    """Function that generates a timeseries based on particular logic

    Args:
        timeseries_type (str): Defines the timeseries to produce. Options: Biomass, Carbon Tax.
        start_year (int): Defines the start date of the timeseries.
        end_year (int): Defines the end date of the timeseries.
        series_start_year (int): The year that the timeseries starts.
        end_value (float): Defines the terminal value of the timeseries.
        start_value (float, optional): Defines the starting value of the timeseries. Defaults to 0.
        units (str, optional): [description]. Define units of the timeseries values. Defaults to ''.

    Returns:
        DataFrame: A DataFrame of the timeseries.
    """
    # Define schema for the DataFrame
    df_schema = {"value": float, "units": str}
    # Define the year range for the df
    year_range = pd.RangeIndex(start_year, end_year + 1)
    zero_range = range(int(start_year), int(series_start_year + 1))
    value_range = range(int(series_start_year + 1), int(end_year + 1))
    # Create the DataFrame
    df = pd.DataFrame(
        index=pd.RangeIndex(0, len(year_range)),
        columns=[key.lower() for key in df_schema],
    )
    # Define the year columns
    df["year"] = year_range
    df["units"] = units
    df["units"] = df["units"].apply(lambda x: x.lower())
    df.set_index(["year"], inplace=True)

    def levy_logic(df: pd.DataFrame) -> pd.DataFrame:
        """Applies logic to generate carbon tax timeseries

        Args:
            df (pd.DataFrame): A dataframe with empty values column.

        Returns:
            pd.DataFrame: A dataframe with the value logic applied.
        """
        df_c = df.copy()

        for row in df_c.itertuples():
            if row.Index in list(zero_range):  # skip first year
                df_c.loc[row.Index, "value"] = start_value
            elif row.Index < end_year:
                # logic for remaining years except last year
                df_c.loc[row.Index, "value"] = (end_value / len(value_range)) * (
                    (row.Index) - series_start_year
                )
            else:
                df_c.loc[row.Index, "value"] = end_value  # logic for last year
        return df_c

    # Setting values: BUSINESS LOGIC
    logger.info(f"Running {timeseries_type} timeseries generator")
    if timeseries_type == "carbon_tax":
        df = levy_logic(df)
        df["units"] = "USD / t CO2 eq"
    if timeseries_type == "green_premium":
        df = levy_logic(df)
        df["units"] = "USD / t steel"
    # change the column types
    for key in df_schema.keys():
        df[key].astype(df_schema[key])
    df.reset_index(inplace=True)
    if extension_year:
        df = extend_df_years(df, "year", extension_year)
    logger.info(f"{timeseries_type} timeseries complete")
    return df


@timer_func
def generate_timeseries(
    scenario_dict: dict = None,
    pkl_paths: Union[dict, None] = None,
    serialize: bool = False,
) -> dict:
    """Generates timeseries for biomass, carbon taxes and electricity.

    Args:
        scenario_dict (dict): The scenario_dict containing the full scenario setting for the current model run.
        pkl_paths (Union[dict, None], optional): A dictionary containing custom pickle paths. Defaults to {}.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dict containing dataframes with the following keys: 'biomass', 'carbon_tax', 'electricity'.
    """
    _, intermediate_path, _ = return_pkl_paths(
        scenario_name=scenario_dict["scenario_name"], paths=pkl_paths
    )
    carbon_tax_scenario_values = CARBON_TAX_SCENARIOS[
        scenario_dict["carbon_tax_scenario"]
    ]
    green_premium_scenario_values = GREEN_PREMIUM_SCENARIOS[
        scenario_dict["green_premium_scenario"]
    ]
    # Create Carbon Tax timeseries
    carbon_tax_timeseries = timeseries_generator(
        timeseries_type="carbon_tax",
        start_year=MODEL_YEAR_START,
        end_year=CARBON_TAX_END_YEAR,
        series_start_year=CARBON_TAX_START_YEAR,
        end_value=carbon_tax_scenario_values[1],
        start_value=carbon_tax_scenario_values[0],
        extension_year=MODEL_YEAR_END,
    )
    carbon_tax_timeseries = convert_currency_col(
        carbon_tax_timeseries, "value", scenario_dict["eur_to_usd"]
    )

    green_premium_timeseries = timeseries_generator(
        timeseries_type="green_premium",
        start_year=MODEL_YEAR_START,
        end_year=GREEN_PREMIUM_END_YEAR,
        series_start_year=GREEN_PREMIUM_START_YEAR,
        end_value=green_premium_scenario_values[1],
        start_value=green_premium_scenario_values[0],
        extension_year=MODEL_YEAR_END,
    )
    green_premium_timeseries = convert_currency_col(
        green_premium_timeseries, "value", scenario_dict["eur_to_usd"]
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
