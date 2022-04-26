"""Calculates Global Meta Results of the model"""

import pandas as pd

from mppsteel.config.model_config import (
    MODEL_YEAR_RANGE
)
from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter
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
    capacity_results: dict,
    utilization_results: dict,
    production_results_df: pd.DataFrame,
    rounding: int = 1
) -> pd.DataFrame:
    """_summary_

    Args:
        steel_market_df (pd.DataFrame): The DataFrame of Steel Demand values.
        tech_capacity_df (pd.DataFrame): The DataFrame of Technology Capacity values.
        production_results_df (pd.DataFrame): The Production Stats Results DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing all of the Metaresults for the model run.
    """
    # Initial Steel capacity values
    logger.info("- Generating Global Metaresults")
    production_results_df_c = production_results_df.copy()
    production_results_df_c.set_index('year', inplace=True)
    # Base DataFrame
    year_range = list(MODEL_YEAR_RANGE)
    df = pd.DataFrame({"year": year_range})
    # Assign initial values
    df["steel_demand"] = df["year"].apply(
        lambda year: steel_demand_getter(
            steel_market_df, year, "crude", region="World"
        ) # Mt
    ).round(rounding)
    df["steel_capacity"] = df["year"].apply(lambda year: sum(capacity_results[year].values())).round(rounding) # Mt
    df["capacity_balance"] = (df["steel_capacity"] - df["steel_demand"]).round(rounding) # Mt
    df['capacity_utilization_factor'] = df["year"].apply(lambda year: utilization_results[year]['World'])
    df['steel_production'] = (df["steel_capacity"] * df["capacity_utilization_factor"]).round(rounding) # Mt
    df['market_balance'] = (df["steel_production"] - df["steel_demand"]).round(rounding) # Mt
    df["scrap_availability"] = df["year"].apply(
        lambda year: steel_demand_getter(
            steel_market_df, year, "scrap", region="World"
        )).round(rounding) # Mt
    df["scrap_consumption"] = df["year"].apply(
        lambda year: production_results_df_c.loc[year]["scrap_mt"].sum()).round(rounding) # Mt
    df["scrap_avail_above_cons"] = (df["scrap_availability"] - df["scrap_consumption"]).round(rounding)
    return df[[
        'year', 'steel_capacity', 'capacity_utilization_factor', 
        'steel_production', 'steel_demand', 'capacity_balance', 
        'market_balance', 'scrap_availability', 'scrap_consumption', 
        'scrap_avail_above_cons'
    ]]


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
        intermediate_path, "regional_steel_demand_formatted", "df"
    )
    production_resource_usage = read_pickle_folder(
        final_path, "production_resource_usage", "df"
    )
    regional_capacity_results = read_pickle_folder(
        intermediate_path, "regional_capacity_results", "dict"
    )
    utilization_results = read_pickle_folder(
        intermediate_path, "utilization_results", "dict"
    )
    global_metaresults = global_metaresults_calculator(
        steel_demand_df,
        regional_capacity_results,
        utilization_results,
        production_resource_usage,
    )
    global_metaresults = add_results_metadata(
        global_metaresults, scenario_dict, include_regions=False, 
        single_line=True, scenario_name=True
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(global_metaresults, final_path, "global_metaresults")
    return global_metaresults
