"""Calculates Global Meta Results of the model"""

import pandas as pd

from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_FORMATTED
)
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def global_metaresults_calculator(
    steel_market_df: pd.DataFrame,
    capacity_dict: dict,
    production_results_df: pd.DataFrame,
    steel_demand_scenario: str,
    year_end: int,
) -> pd.DataFrame:
    """_summary_

    Args:
        steel_market_df (pd.DataFrame): The DataFrame of Steel Demand values.
        tech_capacity_df (pd.DataFrame): The DataFrame of Technology Capacity values.
        production_results_df (pd.DataFrame): The Production Stats Results DataFrame.
        steel_demand_scenario (str): Specifies the steel demand scenario.
        year_end (int): Specifies the model end year.

    Returns:
        pd.DataFrame: A DataFrame containing all of the Metaresults for the model run.
    """
    # Initial Steel capacity values
    logger.info("- Generating Global Metaresults")

    # Base DataFrame
    year_range = list(range(MODEL_YEAR_START, year_end + 1))
    df = pd.DataFrame(
        {
            "year": year_range,
            "steel_demand": 0,
            "steel_capacity": 0,
            "potential_extra_capacity": 0,
        }
    )
    # Assign initial values
    df["steel_demand"] = df["year"].apply(
        lambda year: steel_demand_getter(
            steel_market_df, year, steel_demand_scenario, "crude", region="World"
        )
    )
    df["steel_capacity"] = df["year"].apply(lambda year: sum(list(capacity_dict[str(year)].values())))
    df["extra_capacity"] = df["steel_capacity"] - df["steel_demand"]
    df['capacity_utilization_factor'] = (df['steel_demand'] / df['steel_capacity']).round(3)
    df["scrap_availability"] = df["year"].apply(
        lambda year: steel_demand_getter(
            steel_market_df, year, steel_demand_scenario, "scrap", region="World"
        )) # Mt
    df["scrap_consumption"] = df["year"].apply(
        lambda year: production_results_df.loc[year]["scrap"].sum())
    df["scrap_consumption"] *= 1000
    df["scrap_avail_above_cons"] = df["scrap_availability"] - df["scrap_consumption"]
    return df


@timer_func
def metaresults_flow(scenario_dict: dict, serialize: bool = False) -> pd.DataFrame:
    """Complete Metaresults flow to generate the Investment Results references DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the Metaresults.
    """
    logger.info("- Starting Production Results Model Flow")
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    final_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'final')
    steel_demand_df = read_pickle_folder(
        PKL_DATA_FORMATTED, "regional_steel_demand_formatted", "df"
    )
    production_resource_usage = read_pickle_folder(
        final_path, "production_resource_usage", "df"
    )
    capacity_results = read_pickle_folder(
        intermediate_path, "capacity_results", "dict"
    )
    steel_demand_scenario = scenario_dict["steel_demand_scenario"]
    global_metaresults = global_metaresults_calculator(
        steel_demand_df,
        capacity_results,
        production_resource_usage,
        steel_demand_scenario,
        MODEL_YEAR_END,
    )
    global_metaresults = add_results_metadata(
        global_metaresults, scenario_dict, include_regions=False, single_line=True
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(global_metaresults, final_path, "global_metaresults")
    return global_metaresults
