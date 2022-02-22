"""Calculates Global Meta Results of the model"""

import pandas as pd
from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.config.model_config import (
    AVERAGE_LEVEL_OF_CAPACITY,
    MODEL_YEAR_START,
    PKL_DATA_INTERMEDIATE,
    PKL_DATA_FINAL,
)
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.results.production import tech_capacity_splits
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Global Metaresults")


def global_metaresults_calculator(
    steel_market_df: pd.DataFrame,
    tech_capacity_df: pd.DataFrame,
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
    logger.info(f"- Generating Global Metaresults")

    steel_capacity_years = {"2020": 2362.5}
    steel_capacity_years["2022"] = steel_capacity_years["2020"] * 1.03
    steel_capacity_years["2021"] = (
        steel_capacity_years["2020"] + steel_capacity_years["2022"]
    ) / 2

    def initial_capacity_assignor(year: int):
        return steel_capacity_years[str(year)] if 2020 <= year <= 2022 else 0

    def potential_extra_capacity(capacity_value: float, steel_demand_value: float):
        excess_demand_check = steel_demand_value - (
            AVERAGE_LEVEL_OF_CAPACITY * capacity_value
        )
        if excess_demand_check < 0:
            return 0
        return round(excess_demand_check, 3)

    def tech_capacity_summary(tech_capacity_df: pd.DataFrame, year: int):
        return (
            tech_capacity_df[tech_capacity_df["year"] == year][["capacity"]]
            .sum()
            .values[0]
        )

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
            steel_market_df, year, steel_demand_scenario, "crude", "World"
        )
    )
    df["steel_capacity"] = df["year"].apply(lambda x: initial_capacity_assignor(x))

    # Assign iterative values
    for row in tqdm(
        df.itertuples(), total=df.shape[0], desc="Steel Capacity Calculator"
    ):
        if row.year < 2023:
            steel_capacity = df.loc[(df["year"] == row.year), "steel_capacity"].values[
                0
            ]
            df.loc[row.Index, "potential_extra_capacity"] = potential_extra_capacity(
                steel_capacity, row.steel_demand
            )
        else:
            prior_capacity_value = df.loc[row.Index - 1, "steel_capacity"]
            prior_extra_capacity_value = df.loc[
                row.Index - 1, "potential_extra_capacity"
            ]
            current_year_tech_capacity = tech_capacity_summary(
                tech_capacity_df, row.year
            )
            prior_year_tech_capacity = tech_capacity_summary(
                tech_capacity_df, row.year - 1
            )
            steel_capacity = (
                prior_capacity_value
                + ((current_year_tech_capacity - prior_year_tech_capacity) * 1000)
                + prior_extra_capacity_value
            )
            df.loc[row.Index, "steel_capacity"] = steel_capacity
            df.loc[row.Index, "potential_extra_capacity"] = potential_extra_capacity(
                steel_capacity, row.steel_demand
            )

    df["capacity_utilization_factor"] = (
        df["steel_demand"] / df["steel_capacity"]
    ).round(3)
    df["scrap_availability"] = df["year"].apply(
        lambda year: steel_demand_getter(
            steel_market_df, year, steel_demand_scenario, "crude", "World"
        )
    )
    df["scrap_consumption"] = [
        production_results_df.loc[year]["scrap"].sum() for year in year_range
    ]
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
    steel_demand_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted", "df"
    )
    production_resource_usage = read_pickle_folder(
        PKL_DATA_FINAL, "production_resource_usage", "df"
    )
    tech_capacity_df, max_solver_year = tech_capacity_splits()
    steel_demand_scenario = scenario_dict["steel_demand_scenario"]
    global_metaresults = global_metaresults_calculator(
        steel_demand_df,
        tech_capacity_df,
        production_resource_usage,
        steel_demand_scenario,
        max_solver_year,
    )
    global_metaresults = add_results_metadata(
        global_metaresults, scenario_dict, include_regions=False, single_line=True
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(global_metaresults, PKL_DATA_FINAL, "global_metaresults")
    return global_metaresults
