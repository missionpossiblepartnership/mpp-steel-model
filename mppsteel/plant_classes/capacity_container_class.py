"""Classes to manage plant capacity"""

from typing import Tuple
import pandas as pd

from mppsteel.config.model_config import MEGATON_TO_KILOTON_FACTOR, MAIN_REGIONAL_SCHEMA
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


class CapacityContainerClass:
    """Description
    Class for maintaining the state of each plant's capacity.

    Main Class Attributes
        At the plant level: `plant_capacities` in the form [year][plant_name] -> total capacity value
        At the region level:
            `regional_capacities_agg` in the form [year][region] -> total regional capacity value
            `regional_capacities_avg` in the form [year][region] -> average regional capacity value
    """

    def __init__(self):
        self.plant_capacities = {}
        self.regional_capacities_agg = {}

    def instantiate_container(self, year_range: range):
        self.plant_capacities = {year: 0 for year in year_range}
        self.regional_capacities_agg = {year: 0 for year in year_range}
        self.regional_capacities_avg = {year: 0 for year in year_range}

    def map_capacities(self, plant_df: pd.DataFrame, year: int):
        # Map capacities of plants that are still active for aggregates, else use averages
        plant_capacity_dict, regional_capacity_dict = create_annual_capacity_dict(
            plant_df, as_mt=True
        )
        self.plant_capacities[year] = plant_capacity_dict
        self.regional_capacities_agg[year] = regional_capacity_dict

    def set_average_plant_capacity(self, original_plant_df: pd.DataFrame):
        self.average_plant_capacity = create_average_plant_capacity(
            original_plant_df, as_mt=True
        )

    def return_regional_capacity(self, year: int = None, region: str = None):
        capacity_dict = self.regional_capacities_agg
        if region and not year:
            # return a year value time series for a region
            return {
                year_val: capacity_dict[year_val][region] for year_val in capacity_dict
            }

        if year and not region:
            # return all regions for single year
            return capacity_dict[year]

        if year and region:
            # return single value
            return capacity_dict[year][region]

        # return all years and regions
        return capacity_dict

    def return_avg_capacity_value(self):
        return self.average_plant_capacity

    def get_world_capacity_sum(self, year: int):
        return sum(list(self.regional_capacities_agg[year].values()))

    def return_plant_capacity(self, year: int = None, plant: str = None):
        if plant and not year:
            # return a year valye time series for a region
            return {
                year_val: (
                    self.plant_capacities[year_val][plant]
                    if plant in self.plant_capacities[year_val]
                    else 0
                )
                for year_val in self.plant_capacities
            }

        if year and not plant:
            # return all plants for single year
            return self.plant_capacities[year]

        if year and plant:
            # return single value
            return (
                self.plant_capacities[year][plant]
                if plant in self.plant_capacities[year]
                else 0
            )

        # return all years and regions
        return self.plant_capacities


def create_regional_capacity_dict(
    plant_df: pd.DataFrame, rounding: int = 3, as_avg: bool = False, as_mt: bool = False
) -> dict:
    """Creates a regional capacity dictionary.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        rounding (int, optional): The rounding factor for the capacity values. Defaults to 3.
        as_avg (bool, optional): Optionally returns the average capacity value instead of the aggregate value. Defaults to False.
        as_mt (bool, optional): Boolean flag that optionall converts the capacity value from Kilotons to Megatons. Defaults to False.

    Returns:
        dict: A dictionary containing regions as keys and capacity values as values.
    """
    logger.info("Deriving average plant capacity statistics")
    plant_df_c = plant_df.set_index(["active_check"]).loc[True].copy()
    df = plant_df_c[[MAIN_REGIONAL_SCHEMA, "plant_capacity"]].groupby(
        [MAIN_REGIONAL_SCHEMA]
    )
    if as_avg:
        df = df.mean().round(rounding).reset_index()
    else:
        df = df.sum().round(rounding).reset_index()
    dict_obj = dict(zip(df[MAIN_REGIONAL_SCHEMA], df["plant_capacity"]))
    if as_mt:
        return {
            region: value / MEGATON_TO_KILOTON_FACTOR
            for region, value in dict_obj.items()
        }
    return dict_obj


def create_average_plant_capacity(
    plant_df: pd.DataFrame, rounding: int = 3, as_mt: bool = False
) -> float:
    """Generates an average plant capacity value across all plants and regions to use as a reference.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        rounding (int, optional): Rounding factor to use for the final values. Defaults to 3.
        as_mt (bool, optional): Boolean flag to determine whether to convert to units from Kilotons to Megatons. Defaults to False.

    Returns:
        float: The average plant capacity
    """
    plant_df_c = plant_df.set_index(["active_check"]).loc[True].copy()
    capacity_sum = plant_df_c["plant_capacity"].sum()
    if as_mt:
        capacity_sum = capacity_sum / MEGATON_TO_KILOTON_FACTOR
    return round(capacity_sum / len(plant_df_c), rounding)


def create_annual_capacity_dict(
    plant_df: pd.DataFrame, as_mt: bool = False
) -> Tuple[dict, dict]:
    """Creates two dictionaries. One with the plants as keys and capacities as values. The second with regions as keys and capacities as values.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        as_mt (bool, optional): Boolean flag to determine whether to convert to units from Kilotons to Megatons. Defaults to False.

    Returns:
        Tuple[dict, dict]: A tuple of the two capacity dictionary references.
    """
    regions = plant_df[MAIN_REGIONAL_SCHEMA].unique()
    plant_capacity_dict = dict(zip(plant_df["plant_name"], plant_df["plant_capacity"]))
    regional_capacity_dict_list = {
        region: [
            plant_capacity_dict[plant_name]
            for plant_name in set(plant_capacity_dict.keys()).intersection(
                set(
                    plant_df[plant_df[MAIN_REGIONAL_SCHEMA] == region][
                        "plant_name"
                    ].unique()
                )
            )
        ]
        for region in regions
    }
    regional_capacity_dict = {
        region: sum(value_list)
        for region, value_list in regional_capacity_dict_list.items()
    }
    if as_mt:
        plant_capacity_dict = {
            plant_name: value / MEGATON_TO_KILOTON_FACTOR
            for plant_name, value in plant_capacity_dict.items()
        }
        regional_capacity_dict = {
            region: value / MEGATON_TO_KILOTON_FACTOR
            for region, value in regional_capacity_dict.items()
        }
    return plant_capacity_dict, regional_capacity_dict
