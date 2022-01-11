"""Module that generates a timeseries for various purposes"""
# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from mppsteel.utility.utils import get_logger, serialize_file, timer_func

# Get model parameters
from mppsteel.model_config import (
    PKL_DATA_INTERMEDIATE,
    BIOMASS_AV_TS_END_VALUE,
    BIOMASS_AV_TS_END_YEAR,
    BIOMASS_AV_TS_START_YEAR,
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    ELECTRICITY_PRICE_MID_YEAR,
    CARBON_TAX_SCENARIOS,
    GREEN_PREMIUM_SCENARIOS
)

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
        timeseries_type (str): Defines the timeseries to produce. Options: Biomass, Carbon Tax
        start_year (int): Defines the start date of the timeseries
        end_year (int): Defines the end date of the timeseries
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
        columns=[key.lower() for key in df_schema.keys()],
    )
    # Define the year columns
    df["year"] = year_range
    df["units"] = units
    df["units"] = df["units"].apply(lambda x: x.lower())

    def biomass_logic(df: pd.DataFrame) -> pd.DataFrame:
        """Applies logic to generate biomass timeseries

        Args:
            df (pd.DataFrame): A dataframe with empty values column.

        Returns:
            pd.DataFrame: A dataframe with the value logic applied.
        """
        df_c = df.copy()
        for row in df_c.itertuples():
            if row.Index < 2:  # skip first 2 years
                df_c.loc[row.Index, "value"] = 0
            elif (
                row.Index < len(year_range) - 1
            ):  # logic for remaining years except last year
                df_c.loc[row.Index, "value"] = end_value / (
                    1 + (np.exp(-0.45 * (row.year - ELECTRICITY_PRICE_MID_YEAR)))
                )
            else:
                df_c.loc[row.Index, "value"] = end_value  # logic for last year
        return df_c

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
    if timeseries_type == "biomass":
        df = biomass_logic(df)
        df["units"] = "PJ / y"
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
def generate_timeseries(serialize_only: bool = False, scenario_dict: dict = None) -> dict:
    """Generates timeseries for biomass, carbon taxes and electricity.

    Args:
        serialize_only (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dict containing dataframes with the following keys: 'biomass', 'carbon_tax', 'electricity'
    """

    carbon_tax_scenario_values = CARBON_TAX_SCENARIOS[scenario_dict['carbon_tax_scenario']]
    green_premium_scenario_values = GREEN_PREMIUM_SCENARIOS[scenario_dict['green_premium_scenario']]
    # Create Biomass timeseries
    biomass_availability = timeseries_generator(
        "biomass",
        BIOMASS_AV_TS_START_YEAR,
        BIOMASS_AV_TS_END_YEAR,
        BIOMASS_AV_TS_END_VALUE,
    )

    # Create Carbon Tax timeseries
    carbon_tax_timeseries = timeseries_generator(
        "carbon_tax",
        MODEL_YEAR_START,
        MODEL_YEAR_END,
        carbon_tax_scenario_values[1],
        carbon_tax_scenario_values[0],
    )
    print(carbon_tax_scenario_values)
    print(green_premium_scenario_values)
    green_premium_timeseries = timeseries_generator(
        'green_premium',
        MODEL_YEAR_START,
        MODEL_YEAR_END,
        green_premium_scenario_values[1],
        green_premium_scenario_values[0],
    )

    if serialize_only:
        # Serialize timeseries
        serialize_file(biomass_availability, PKL_DATA_INTERMEDIATE, "biomass_availability")
        serialize_file(carbon_tax_timeseries, PKL_DATA_INTERMEDIATE, "carbon_tax_timeseries")
        serialize_file(green_premium_timeseries, PKL_DATA_INTERMEDIATE, "green_premium_timeseries")

    return {
        "biomass": biomass_availability,
        "carbon_tax_timeseries": carbon_tax_timeseries,
        "green_premium_timeseries": green_premium_timeseries
    }
