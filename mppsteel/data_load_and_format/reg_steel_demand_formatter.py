"""Formats Regional Steel Demand and defines getter function"""

import itertools
import pandas as pd
import pandera as pa

from mppsteel.config.model_config import IMPORT_DATA_PATH, MODEL_YEAR_END, PKL_DATA_IMPORTS, PROJECT_PATH
from mppsteel.config.model_scenarios import STEEL_DEMAND_SCENARIO_MAPPER
from mppsteel.utility.dataframe_utility import extend_df_years
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.location_utility import (
    get_unique_countries,
    get_countries_from_group,
)
from mppsteel.utility.file_handling_utility import (
    extract_data,
    read_pickle_folder,
    serialize_file,
    get_scenario_pkl_path,
)
from mppsteel.data_validation.data_import_tests import REGIONAL_STEEL_DEMAND_SCHEMA
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)

RMI_MATCHER = {
    "Japan, South Korea, and Taiwan": ["JPN", "KOR", "TWN"],
    "World": ["RoW"],
}


def steel_demand_region_assignor(
    region: str, country_ref: pd.DataFrame, rmi_matcher: dict
) -> list:
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
    return get_countries_from_group(country_ref, "RMI Model Region", region)


@pa.check_input(REGIONAL_STEEL_DEMAND_SCHEMA)
def steel_demand_creator(df: pd.DataFrame, rmi_matcher: dict, index_cols: list) -> pd.DataFrame:
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
    df_c = pd.melt(
        frame=df_c,
        id_vars=["metric", "region", "scenario", "country_code"],
        var_name=["year"]
    )
    df_c["year"] = df_c["year"].astype(int)
    return df_c.set_index(index_cols)


def add_average_values(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a new set of values for the data which is the average of the `BAU` and `High Circ` scenarios.

    Args:
        df (pd.DataFrame): The Steel Demand DataFrame.

    Returns:
        pd.DataFrame: The modified DataFrame with the 'Average' data at the same level as the other scenarios.
    """
    df_c = df.copy()
    average_values = (
        df_c.loc[:, "BAU", :]["value"].values
        + df_c.loc[:, "High Circ", :]["value"].values
    ) / 2
    average_base = df_c.loc[:, "BAU", :].rename({"BAU": "Average"}).copy()
    average_base["value"] = average_values
    return pd.concat([df_c, average_base])

def replace_2020_steel_demand_values_with_wsa_production(
    demand_df: pd.DataFrame, 
    index_cols: list, 
    year_to_replace: int = 2020, 
    project_dir=PROJECT_PATH
) -> pd.DataFrame:

    wsa_production = read_pickle_folder(project_dir / PKL_DATA_IMPORTS, "wsa_production", "df")
    wsa_production.set_index("Region", inplace=True)
    demand_df_c = demand_df.copy()
    demand_df_c.reset_index(inplace=True)
    scenarios = demand_df_c["scenario"].unique()
    demand_df_c.set_index(["year", "metric", "scenario", "region"], inplace=True)
    for region, scenario in list(itertools.product(wsa_production.index, scenarios)):
        demand_df_c.loc[(year_to_replace, "Crude steel demand", scenario, region), "value"] = wsa_production.loc[region, "Value"]
    for scenario in scenarios:
        demand_df_c.loc[(year_to_replace, "Crude steel demand", scenario, "World"), "value"] = wsa_production["Value"].sum()
    demand_df_c = demand_df_c.reset_index().set_index(index_cols)
    assert not demand_df.equals(demand_df_c)
    return demand_df_c

@timer_func
def get_steel_demand(scenario_dict: dict, serialize: bool = False, from_csv: bool = False) -> pd.DataFrame:
    """Complete preprocessing flow for the regional steel demand data.

    Args:
        scenario_dict (dict): The scenario_dict containing the full scenario setting for the current model run.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: The formatted DataFrame of regional Steel Demand data.
    """
    intermediate_path = get_scenario_pkl_path(
        scenario_dict["scenario_name"], "intermediate"
    )
    if from_csv:
        steel_demand = extract_data(
            IMPORT_DATA_PATH, "Regional Steel Demand", "csv"
        )
    else:
        steel_demand = read_pickle_folder(PKL_DATA_IMPORTS, "regional_steel_demand", "df")
    index_cols = ["year", "scenario", "metric"]
    steel_demand_f = steel_demand_creator(steel_demand, RMI_MATCHER, index_cols)
    steel_demand_f = replace_2020_steel_demand_values_with_wsa_production(steel_demand_f, index_cols)
    steel_demand_f = add_average_values(steel_demand_f)
    scenario_entry = STEEL_DEMAND_SCENARIO_MAPPER[
        scenario_dict["steel_demand_scenario"]
    ]
    steel_demand_f = steel_demand_f.loc[:, scenario_entry, :].copy()
    index_cols = ["year", "metric"]
    steel_demand_f.reset_index().set_index(index_cols)
    steel_demand_f = extend_df_years(steel_demand_f, "year", MODEL_YEAR_END, index_cols)
    if serialize:
        serialize_file(
            steel_demand_f, intermediate_path, "regional_steel_demand_formatted"
        )
    return steel_demand_f


def steel_demand_getter(
    df: pd.DataFrame,
    year: int,
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
        raise AttributeError(
            "Neither `region` or `country_code` attributes were entered. Enter a valid option for one."
        )
    # Apply country check and use default

    if region and country_code:
        raise AttributeError(
            "You entered both region and country_code attributes were entered. Enter a valid option for one."
        )

    if region:
        if region in region_list:
            df_c = df_c[df_c["region"].str.contains(region, regex=False)]
        elif force_default:
            print(
                f"Invalid region string entered: {region}. Reverting to default region: {default_region}. Valid entries here: {region_list}."
            )
            df_c = df_c[df_c["region"].str.contains(default_region, regex=False)]
        else:
            AttributeError(
                f"You entered an incorrect region. Valid entries here: {region_list}."
            )

    if country_code:
        if country_code in country_list:
            df_c = df_c[df_c["country_code"].str.contains(country_code, regex=False)]
        elif force_default:
            print(
                f"Invalid country string entered: {country_code}. Reverting to default country_code: {default_country}."
            )
            df_c = df_c[df_c["country_code"].str.contains(default_country, regex=False)]
        else:
            AttributeError(
                f"You entered an incorrect country_code. Valid entries here: {country_list}."
            )
    # Apply subsets
    # Scenario: BAU, High Circ, Average
    # Metric: crude, scrap

    if df_c.empty:
        df_c = pd.DataFrame(
            [["2020", "Africa", 0, "DEU", "Crude steel demand"]],
            columns=["year", "region", "value", "country_code", "metric"],
        ).set_index(["year", "metric"])

    metric_mapper = {
        "crude": "Crude steel demand",
        "scrap": "Scrap availability",
    }
    df_c = df_c.xs(
        (year, metric_mapper[metric]),
        level=["year", "metric"],
    )
    df_c.reset_index(drop=True, inplace=True)
    # Return the value figure
    return df_c.value.values[0]
