"""Classes to manage regional utilization"""

import pandas as pd
import numpy as np

from mppsteel.config.model_config import (
    IMPORT_DATA_PATH,
    PKL_DATA_IMPORTS,
    PKL_DATA_FORMATTED,
    PROJECT_PATH,
    MODEL_YEAR_START,
)
from mppsteel.utility.file_handling_utility import extract_data, read_pickle_folder
from mppsteel.plant_classes.capacity_container_class import (
    create_regional_capacity_dict,
)

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


class UtilizationContainerClass:
    """Description
    Class for managing each region's utilization rates.

    Main Class Attirbutes
        It maintains a dictionary called utilization_container structured as [year][region] -> value
        World Utilization rates are treated as a weighted average of all the region's utilizatoin rates
    """

    def __init__(self):
        self.utilization_container = {}

    def initiate_container(self, year_range: range, region_list: list):
        for year in year_range:
            self.utilization_container[year] = {region: 0 for region in region_list}

    def assign_year_utilization(self, year: int, entry: dict):
        self.utilization_container[year] = entry

    def update_region(self, year: int, region: str, value: float):
        self.utilization_container[year][region] = value

    def get_average_utilization(self, year: int):
        return np.mean(self.utilization_container[year].values())

    def calculate_world_utilization(
        self, year: int, capacity_dict: dict, demand_value: float
    ):
        self.utilization_container[year]["World"] = demand_value / sum(
            capacity_dict.values()
        )

    def get_utilization_values(self, year: int = None, region: str = None):
        year = year + 1 if year == MODEL_YEAR_START - 1 else year
        if region and not year:
            # return a year valye time series for a region
            return {
                year_val: self.utilization_container[year_val][region]
                for year_val in self.utilization_container
            }

        if year and not region:
            # return all regions for single year
            return self.utilization_container[year]

        if year and region:
            # return single value
            return self.utilization_container[year][region]

        # return all years and regions
        return self.utilization_container


def format_wsa_production_data(df: pd.DataFrame, as_dict: bool = False) -> pd.DataFrame:
    """Formats the inital WSA DataFrame in preparation to extract utilization figures.

    Args:
        df (pd.DataFrame): The initial WSA Data.
        as_dict (bool, optional): Boolean flag that determines whether the DataFrame should be returned as a dictionary. Defaults to False.

    Returns:
        pd.DataFrame: Formatted WSA Data.
    """
    logger.info("Formatting WSA production data for 2020")
    df_c = df.copy()
    df_c.columns = [col.lower() for col in df_c.columns]
    if as_dict:
        return dict(zip(df_c["region"], df_c["value"]))
    return df_c


def return_utilization(
    prod_dict: dict, cap_dict: dict, utilization_cap: float = None
) -> dict:
    """Creates a utilization dictionary based on production reference dictionary and capacity reference dictionary.
    Takes the minimum of the calculated capacity and the utilization `value cap`.

    Args:
        prod_dict (dict): The dictionary containing the actual production figures.
        cap_dict (dict): The dictionary containing the capacity values.
        value_cap (float, optional): The maximum utilization value that a plant is allowed to take. Defaults to None.

    Returns:
        dict: A dictionary with regions as keys and utilization numbers as values.
    """
    util_dict = {}
    for region in prod_dict:
        val = round(prod_dict[region] / cap_dict[region], 2)
        if utilization_cap:
            val = min(val, utilization_cap)
        util_dict[region] = val
    return util_dict


def create_wsa_2020_utilization_dict(
    project_dir=PROJECT_PATH, from_csv: bool = False, utilization_cap: int = 1
) -> dict:
    """Creates the initial utilization dictionary for 2020 based on data from the World Steel Association (WSA).

    Returns:
        dict: A dictionary with regions as keys and utilization numbers as values.
    """
    logger.info("Creating the utilization dictionary for 2020.")
    if from_csv:
        wsa_production = extract_data(IMPORT_DATA_PATH, "WSA Production 2020", "csv")
    else:
        wsa_production = read_pickle_folder(
            project_dir / PKL_DATA_IMPORTS, "wsa_production", "df"
        )
    steel_plants_processed = read_pickle_folder(
        project_dir / PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
    wsa_2020_production_dict = format_wsa_production_data(wsa_production, as_dict=True)
    capacity_dict = create_regional_capacity_dict(steel_plants_processed, as_mt=True)
    return return_utilization(
        wsa_2020_production_dict, capacity_dict, utilization_cap=utilization_cap
    )
