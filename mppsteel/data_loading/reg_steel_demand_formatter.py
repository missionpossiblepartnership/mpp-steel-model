"""Formats Regional Steel Demand and defines getter function"""

import logging
import pandas as pd
import pandera as pa

from mppsteel.config.model_config import (
    PKL_DATA_IMPORTS,
    PKL_DATA_FORMATTED,
    MODEL_YEAR_END,
)
from mppsteel.utility.utils import create_list_permutations
from mppsteel.utility.timeseries_extender import full_model_flow
from mppsteel.config.model_scenarios import STEEL_DEMAND_SCENARIO_MAPPER
from mppsteel.utility.dataframe_utility import melt_and_index
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.location_utility import get_unique_countries, get_countries_from_group
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.validation.data_import_tests import REGIONAL_STEEL_DEMAND_SCHEMA
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Regional Steel Demand Formatter")

RMI_MATCHER = {
    "Japan, South Korea, and Taiwan": ["JPN", "KOR", "TWN"],
    "World": ["RoW"],
}


def steel_demand_region_assignor(region: str, country_ref: pd.DataFrame, rmi_matcher: dict) -> list:
    """Returns the country codes in a region_dictionary based on keys.

    Args:
        region (str): The region you want the country codes for.
        country_ref (pd.DataFrame): The DataFrame containing the mapping of country codes to regions.
        rmi_matcher (dict): The dictionary containing the exceptions in the data.

    Returns:
        list: A list of country codes based on the `region` key.
    """
    if region in rmi_matcher:
        return rmi_matcher[region]
    return get_countries_from_group(country_ref, 'RMI Model Region', region)


@pa.check_input(REGIONAL_STEEL_DEMAND_SCHEMA)
def steel_demand_creator(df: pd.DataFrame, rmi_matcher: dict) -> pd.DataFrame:
    """Formats the Steel Demand data. Assigns country codes to the regions, and melt and indexes them.

    Args:
        df (pd.DataFrame): A DataFrame containing the initial Steel Demand Data.
        rmi_matcher (dict): The exception dictionary for regions that do not map precisely.

    Returns:
        pd.DataFrame: The formatted DataFrame.
    """
    logger.info("Formatting the Regional Steel Demand Data")
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref", "df")
    df_c = df.copy()
    df_c["country_code"] = df_c["Region"].apply(
        lambda x: steel_demand_region_assignor(x, country_ref, rmi_matcher)
    )

    df_c.columns = [col.lower() for col in df_c.columns]

    return melt_and_index(df_c, 
        id_vars=["metric", "region", "scenario", "country_code"], 
        var_name=["year"], 
        index=["year", "scenario", "metric"])


@timer_func
def get_steel_demand(serialize: bool = False) -> pd.DataFrame:
    """Complete preprocessing flow for the regional steel demand data.

    Args:
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: The formatted DataFrame of regional Steel Demand data.
    """
    steel_demand = read_pickle_folder(PKL_DATA_IMPORTS, "regional_steel_demand", "df")
    steel_demand_f = steel_demand_creator(steel_demand, RMI_MATCHER)
    if serialize:
        serialize_file(
            steel_demand_f, PKL_DATA_FORMATTED, "regional_steel_demand_formatted"
        )
    return steel_demand_f


def steel_demand_getter(
    df: pd.DataFrame,
    year: int,
    scenario: str,
    metric: str,
    region: str = None,
    country_code: str = None,
    force_default: bool = True,
    default_region: str = "RoW",
    default_country: str = "GBL",
) -> float:
    """A getter function for the regional steel demand data. 

    Args:
        df (pd.DataFrame): The DataFrame containing the preprocessed Regional Steel Demand data.
        year (int): The year of the demand data you want.
        scenario (str): The scenario you want to access (BAU or High Circ or average).
        metric (str): The metric you want to access (crude or scrap).
        region (str): The region you want to get the data for. Either region OR country_code should be entered, not both. Defaults to None.
        country_code (str): The country code you want to get the data for. Either region OR country_code should be entered, not both. Defaults to None.
        force_default (bool): If True, will defer to defaults if incorrect region or country codes are entered. If False, invalid entries will raise an error. Defaults to True.
        default_region (str, optional): The default region you want to get the data for. Defaults to "World" for global.
        default_country (str, optional): The default country you want to get the data for. Defaults to "GBL" for global.

    Returns:
        float: The demand value for the inputted data.
    """
    df_c = df.copy()
    # define country list based on the data_type
    region_list = list(df_c["region"].unique())
    country_list = get_unique_countries(df_c["country_code"].values)

    if not region and not country_code:
        raise AttributeError('Neither `region` or `country_code` attributes were entered. Enter a valid option for one.')
    # Apply country check and use default

    if region and country_code:
        raise AttributeError('You entered both region and country_code attributes were entered. Enter a valid option for one.')

    if region:
        if region in region_list:
            df_c = df_c[df_c["region"].str.contains(region, regex=False)]
        elif force_default:
            print(f'Invalid region string entered: {region}. Reverting to default region: {default_region}. Valid entries here: {region_list}.')
            df_c = df_c[df_c["region"].str.contains(default_region, regex=False)]
        else:
            AttributeError(f'You entered an incorrect region. Valid entries here: {region_list}.')

    if country_code:
        if country_code in country_list:
            df_c = df_c[df_c["country_code"].str.contains(country_code, regex=False)]
        elif force_default:
            print(f'Invalid country string entered: {country_code}. Reverting to default country_code: {default_country}.')
            df_c = df_c[df_c["country_code"].str.contains(default_country, regex=False)]
        else:
            AttributeError(f'You entered an incorrect country_code. Valid entries here: {country_list}.')

    # Cap year at 2050
    MODEL_YEAR_END = 2050
    year = min(MODEL_YEAR_END, year)
    # Apply subsets
    # Scenario: BAU, High Circ, average
    # Metric: crude, scrap
    STEEL_DEMAND_SCENARIO_MAPPER = {"bau": "BAU", "high": "High Circ", "average": "average"}
    scenario_entry = STEEL_DEMAND_SCENARIO_MAPPER[scenario]

    metric_mapper = {
        "crude": "Crude steel demand",
        "scrap": "Scrap availability",
    }

    if scenario_entry == "average":
        df1_val = df_c.xs(
            (str(year), "BAU", metric_mapper[metric]),
            level=["year", "scenario", "metric"],
        ).value.values[0]
        df2_val = df_c.xs(
            (str(year), "High Circ", metric_mapper[metric]),
            level=["year", "scenario", "metric"],
        ).value.values[0]
        return (df1_val + df2_val) / 2
    else:
        df_c = df_c.xs(
            (str(year), scenario_entry, metric_mapper[metric]),
            level=["year", "scenario", "metric"],
        )
        df_c.reset_index(drop=True, inplace=True)
        # Return the value figure
        return df_c.value.values[0]


def extend_steel_demand(year_end: int) -> pd.DataFrame:
    """Extends the Steel Demand data beyond its range to a specified point in the future.
    Args:
        year_end (int): The year you intend to extend the steel demand data towards.

    Returns:
        pd.DataFrame: A DataFrame of Steel Demand with an extended year range.
    """
    # Need to amend to regional steel demand data.
    logger.info(f"-- Extedning the Steel Demand DataFrame to {year_end}")
    scenarios = ["Circular", "BAU"]
    steel_types = ["Crude", "Scrap"]
    steel_demand_perms = create_list_permutations(steel_types, scenarios)
    global_demand = read_pickle_folder(PKL_DATA_IMPORTS, "steel_demand", "df")
    df_list = []
    for permutation in steel_demand_perms:
        steel_type = permutation[0]
        scenario = permutation[1]
        if steel_type == "Crude" and scenario == "BAU":
            series_type = "geometric"
            growth_type = "fixed"
            value_change = 2850
        if steel_type == "Crude" and scenario == "Circular":
            series_type = "linear"
            growth_type = "fixed"
            value_change = 1500
        if steel_type == "Scrap" and scenario == "BAU":
            series_type = "geometric"
            growth_type = "pct"
            value_change = 15
        if steel_type == "Scrap" and scenario == "Circular":
            series_type = "geometric"
            growth_type = "pct"
            value_change = 20
        df = full_model_flow(
            df=global_demand[
                (global_demand["Steel Type"] == steel_type)
                & (global_demand["Scenario"] == scenario)
            ],
            year_value_col_dict={"year": "year", "value": "value"},
            static_value_override_dict={
                "Source": "RMI + Model Extension beyond 2050",
                "Excel Tab": "Extended from Excel",
            },
            new_end_year=year_end,
            series_type=series_type,
            growth_type=growth_type,
            value_change=value_change,
            year_only=True,
        )
        df_list.append(df)
    return pd.concat(df_list).reset_index(drop=True)
