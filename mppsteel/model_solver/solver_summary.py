"""Script to manage post-solver summary dataframes"""

from mppsteel.data_load_and_format.steel_plant_formatter import map_plant_id_to_df
import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    MODEL_YEAR_RANGE
)

from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def tech_capacity_splits(
    steel_plants: pd.DataFrame,
    tech_choices: dict,
    capacity_dict: dict,
    active_check_results_dict: dict,
) -> pd.DataFrame:
    """Create a DataFrame containing the technologies and capacities for every plant in every year.

    Args:
        steel_plants (pd.DataFrame): The steel plant DataFrame.
        tech_choices (dict): A dictionary containing the technology choices for each plant.
        capacity_dict (dict): A dictionary containing the capacity values for each plant
        active_check_results_dict (dict): A dictionary containing a reference to whether a plant in a specified year was active or not.

    Returns:
        pd.DataFrame: A DataFrame containing the technologies and capacities for every plant in every year.
    """
    logger.info("- Generating Capacity split DataFrame")

    df_list = []

    plant_country_code_mapper = dict(
        zip(steel_plants["plant_name"].values, steel_plants["country_code"].values)
    )

    for year in tqdm(
        MODEL_YEAR_RANGE, total=len(MODEL_YEAR_RANGE), desc="Tech Capacity Splits"
    ):
        steel_plant_names = capacity_dict[year].keys()
        df = pd.DataFrame({"year": year, "plant_name": steel_plant_names})
        df["technology"] = df["plant_name"].apply(
            lambda plant: get_tech_choice(
                tech_choices, active_check_results_dict, year, plant
            )
        )
        df["capacity"] = df["plant_name"].apply(
            lambda plant_name: get_capacity(
                capacity_dict, active_check_results_dict, year, plant_name
            )
        )
        df["country_code"] = df["plant_name"].apply(
            lambda plant_name: plant_country_code_mapper[plant_name]
        )
        df = df[df["technology"] != ""]
        df_list.append(df)

    df_combined = pd.concat(df_list)
    df_combined = map_plant_id_to_df(df_combined, steel_plants, "plant_name")

    return df_combined


def get_tech_choice(
    tc_dict: dict, active_plant_checker_dict: dict, year: int, plant_name: str
) -> str:
    """Return a technology choice for a given plant in a given year.

    Args:
        tc_dict (dict): Dictionary containing all technology choices for every plant across every year.
        active_plant_checker_dict (dict): A dictionary containing boolean values that reveal whether a plant in a given year was active or not.
        year (int): The year you want the technology choice for.
        plant_name (str): The name of the plant.

    Returns:
        str: The technology choice requested via the function arguments.
    """
    return (
        tc_dict[year][plant_name] if active_plant_checker_dict[plant_name][year] else ""
    )


def get_capacity(
    capacity_dict: dict, active_plant_checker_dict: dict, year: int, plant_name: str
) -> str:
    """Return a technology choice for a given plant in a given year.

    Args:
        capacity_dict (dict): Dictionary containing all capacity values for every plant across every year.
        active_plant_checker_dict (dict): A dictionary containing boolean values that reveal whether a plant in a given year was active or not.
        year (int): The year you want the technology choice for.
        plant_name (str): The name of the plant

    Returns:
        str: The technology choice requested via the function arguments.
    """
    return (
        capacity_dict[year][plant_name]
        if active_plant_checker_dict[plant_name][year]
        else 0
    )

def utilization_mapper(row, utilization_results: dict):
    return (
        0
        if row.technology == "Close plant"
        else utilization_results[row.year][row.region]
    )