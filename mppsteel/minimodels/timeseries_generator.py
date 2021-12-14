"""Module that generates a timeseries for various purposes"""
# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from mppsteel.utility.utils import get_logger, read_pickle_folder, serialize_df

# Get model parameters
from mppsteel.model_config import (
    PKL_FOLDER,
    BIOMASS_AV_TS_END_VALUE,
    BIOMASS_AV_TS_END_YEAR,
    BIOMASS_AV_TS_START_YEAR,
    CARBON_TAX_END_VALUE,
    CARBON_TAX_END_YEAR,
    CARBON_TAX_START_VALUE,
    CARBON_TAX_START_YEAR,
    ELECTRICITY_PRICE_START_YEAR,
    ELECTRICITY_PRICE_MID_YEAR,
    ELECTRICITY_PRICE_END_YEAR,
    EUR_USD_CONVERSION,
)

from .electricity_assumptions import (
    GRID_ELECTRICITY_PRICE_FAVORABLE_MID,
    GRID_ELECTRICITY_PRICE_AVG_MID,
    DEEPLY_DECARBONISED_POWER_SYSTEM_PRICE_AVG,
    DEEPLY_DECARBONISED_POWER_SYSTEM_PRICE_INCREASE,
)

# Create logger
logger = get_logger("Timeseries generator")


def get_grid_refs(df: pd.DataFrame, geography: str, metrics: list) -> pd.DataFrame:
    return df[(df["Geography (NRG_PRC)"] == geography) & (df["Metric"].isin(metrics))][
        "Value"
    ].tolist()


power_grid_assumptions = read_pickle_folder(PKL_FOLDER, "power_grid_assumptions")

grid_electricity_price_sweden = (
    sum(
        get_grid_refs(
            power_grid_assumptions, "Sweden", ["Energy and supply", "Network costs"]
        )
    )
    * 1000
)

grid_electricity_price_eu = (
    sum(
        get_grid_refs(
            power_grid_assumptions,
            "European Union A",
            ["Energy and supply", "Network costs"],
        )
    )
    * 1000
)

t_and_d_premium = sum(
    get_grid_refs(power_grid_assumptions, "European Union A", ["Network costs"])
) / sum(
    get_grid_refs(
        power_grid_assumptions,
        "European Union A",
        ["Energy and supply", "Network costs"],
    )
)

diff_in_price_between_mid_and_large_business = 1 - (
    sum(
        get_grid_refs(power_grid_assumptions, "European Union A", ["Energy and supply"])
    )
    / sum(
        get_grid_refs(power_grid_assumptions, "European Union B", ["Energy and supply"])
    )
)


def grid_price_selector(year: int, scenario: str):
    if (scenario == "favorable") & (year == ELECTRICITY_PRICE_START_YEAR):
        return grid_electricity_price_sweden
    elif (scenario == "average") & (year == ELECTRICITY_PRICE_START_YEAR):
        return grid_electricity_price_eu
    elif (scenario == "favorable") & (year == ELECTRICITY_PRICE_MID_YEAR):
        return GRID_ELECTRICITY_PRICE_FAVORABLE_MID.value
    elif (scenario == "average") & (year == ELECTRICITY_PRICE_MID_YEAR):
        return GRID_ELECTRICITY_PRICE_AVG_MID.value


def grid_price_mid(scenario: str):
    return (
        grid_price_selector(ELECTRICITY_PRICE_MID_YEAR, scenario)
        * EUR_USD_CONVERSION
        * (1 + t_and_d_premium)
        * (1 - diff_in_price_between_mid_and_large_business)
    )


def grid_price_last_year(scenario: str):
    if scenario == "favorable":
        return grid_price_mid(scenario) + (
            DEEPLY_DECARBONISED_POWER_SYSTEM_PRICE_INCREASE.value * EUR_USD_CONVERSION
        )
    elif scenario == "average":
        return grid_price_selector(ELECTRICITY_PRICE_START_YEAR, scenario) * (
            1 - DEEPLY_DECARBONISED_POWER_SYSTEM_PRICE_AVG.value
        )


# Main timeseries function
def timeseries_generator(
    timeseries_type: str,
    start_year: int,
    end_year: int,
    end_value: float,
    start_value: float = 0,
    units: str = "",
    **kwargs,
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
    year_range = range(start_year, end_year + 1)
    # Create the DataFrame
    df = pd.DataFrame(
        index=pd.RangeIndex(0, len(year_range)),
        columns=[key.lower() for key in df_schema.keys()],
    )
    # Define the year columns
    df["year"] = year_range
    df["units"] = units
    df["units"] = df["units"].apply(lambda x: x.lower())

    power_scenario = ""
    if kwargs:
        power_scenario = kwargs["scenario"]

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

    def carbon_tax_logic(df: pd.DataFrame) -> pd.DataFrame:
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

    def power_grid_logic(
        df: pd.DataFrame,
        scenario: str = power_scenario,
        mid_price_year: int = ELECTRICITY_PRICE_MID_YEAR,
    ) -> pd.DataFrame:
        """Applies logic to generate electricity timeseries

        Args:
            df (pd.DataFrame): A dataframe with empty values column.
            scenario (str, optional): A scenario either 'favorable' or 'average'. Defaults to power_scenario.
            mid_price_year (int, optional): Defines the year that the model uses to calculate the middle price. Defaults to ELECTRICITY_PRICE_MID_YEAR.

        Returns:
            pd.DataFrame: [description]
        """
        df_c = df.copy()
        for row in df_c.itertuples():
            # skip first x years
            if row.Index == 0:
                df_c.loc[row.Index, "value"] = grid_price_selector(
                    ELECTRICITY_PRICE_START_YEAR, scenario
                )
            # first half years
            elif row.Index < mid_price_year - start_year:
                df_c.loc[row.Index, "value"] = (
                    (
                        grid_price_mid(scenario)
                        / grid_price_selector(ELECTRICITY_PRICE_START_YEAR, scenario)
                    )
                    ** (1 / (mid_price_year - start_year))
                ) * df_c.loc[row.Index - 1, "value"]
            # middle year
            elif row.Index == mid_price_year - start_year:
                df_c.loc[row.Index, "value"] = grid_price_mid(scenario)
            # second half years
            elif row.Index > mid_price_year - start_year < len(year_range) - 1:
                df_c.loc[row.Index, "value"] = (
                    (grid_price_last_year(scenario) / grid_price_mid(scenario))
                    ** (1 / (end_year - mid_price_year))
                ) * df_c.loc[row.Index - 1, "value"]
            # final years
            else:
                df_c.loc[row.Index, "value"] = grid_price_last_year(scenario)
        # create a column
        df_c["category"] = "grid electricity price"
        df_c["scenario"] = f"{scenario}"
        return df_c

    # Setting values: BUSINESS LOGIC
    logger.info(f"Running {timeseries_type} timeseries generator")
    if timeseries_type == "biomass":
        df = biomass_logic(df)
        df["units"] = "PJ / y"
    if timeseries_type == "carbon_tax":
        df = carbon_tax_logic(df)
        df["units"] = "EUR / t CO2 eq"
    if timeseries_type == "power":
        df = power_grid_logic(df)
        df["units"] = "USD / MWh"
    # change the column types
    for key in df_schema.keys():
        df[key].astype(df_schema[key])
    logger.info(f"{timeseries_type} timeseries complete")
    return df


def generate_timeseries(serialize_only: bool = False) -> dict:
    """Generates timeseries for biomass, carbon taxes and electricity.

    Args:
        serialize_only (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dict containing dataframes with the following keys: 'biomass', 'carbon_tax', 'electricity'
    """
    # Create Biomass timeseries
    biomass_availability = timeseries_generator(
        "biomass",
        BIOMASS_AV_TS_START_YEAR,
        BIOMASS_AV_TS_END_YEAR,
        BIOMASS_AV_TS_END_VALUE,
    )

    # Create Carbon Tax timeseries
    carbon_tax = timeseries_generator(
        "carbon_tax",
        CARBON_TAX_START_YEAR,
        CARBON_TAX_END_YEAR,
        CARBON_TAX_END_VALUE,
        CARBON_TAX_START_VALUE,
    )

    # Create Electricity timeseries
    favorable_ts = timeseries_generator(
        "power",
        ELECTRICITY_PRICE_START_YEAR,
        ELECTRICITY_PRICE_END_YEAR,
        0,
        units="USD / MWh",
        scenario="favorable",
    )

    average_ts = timeseries_generator(
        "power",
        ELECTRICITY_PRICE_START_YEAR,
        ELECTRICITY_PRICE_END_YEAR,
        0,
        units="USD / MWh",
        scenario="average",
    )

    electricity_minimodel_timeseries = pd.concat([favorable_ts, average_ts])

    if serialize_only:
        # Serialize timeseries
        serialize_df(biomass_availability, PKL_FOLDER, "biomass_availability")
        serialize_df(carbon_tax, PKL_FOLDER, "carbon_tax")
        serialize_df(
            electricity_minimodel_timeseries,
            PKL_FOLDER,
            "electricity_minimodel_timeseries",
        )
        return

    return {
        "biomass": biomass_availability,
        "carbon_tax": carbon_tax,
        "electricity": electricity_minimodel_timeseries,
    }
