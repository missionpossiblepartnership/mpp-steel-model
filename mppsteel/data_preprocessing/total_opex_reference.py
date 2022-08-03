"""Script to create Total Opex Cost Reference"""

import itertools
from typing import Union
import pandas as pd

from tqdm import tqdm

from mppsteel.config.model_config import MODEL_YEAR_RANGE
from mppsteel.config.reference_lists import TECHNOLOGIES_TO_DROP
from mppsteel.model_tests.df_tests import test_negative_df_values

from mppsteel.config.model_config import (
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED
)

from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    return_pkl_paths,
    serialize_file,
    get_scenario_pkl_path,
)
from mppsteel.utility.function_timer_utility import timer_func

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def get_opex_costs(
    country_code: str,
    year: int,
    variable_costs_df: pd.DataFrame,
    opex_df: pd.DataFrame,
    carbon_tax_full_ref: pd.DataFrame
) -> pd.DataFrame:
    """Returns the combined Opex costs for each technology in each region.

    Args:
        country_code (str): The country code of the plant you want to get opex costs for,
        year (int): The year you want to request opex values for.
        variable_costs_df (pd.DataFrame): DataFrame containing the variable costs data split by technology and region.
        opex_df (pd.DataFrame): The Fixed Opex DataFrame containing opex costs split by technology.
        s1_emissions_ref (pd.DataFrame): The DataFrame for scope 1 emissions.
        s2_emissions_value (pd.DataFrame): The DataFrame for scope 2 emissions.
        carbon_tax_timeseries (dict): The carbon tax timeseries with the carbon tax amounts on a yearly basis.

    Returns:
        pd.DataFrame: A DataFrame containing the opex costs for each technology for a given year.
    """
    opex_costs = opex_df.loc[year]
    variable_costs = variable_costs_df.loc[country_code, year]
    carbon_tax_result = carbon_tax_full_ref.loc[year, country_code]
    total_opex = variable_costs + opex_costs + carbon_tax_result
    return total_opex.rename(mapper={"value": "opex"}, axis=1)

def total_opex_cost_ref_loop(
    product_range_year_country: list,
    variable_cost_summary: pd.DataFrame,
    other_opex_df: pd.DataFrame,
    carbon_tax_full_ref: pd.DataFrame
) -> dict:

    opex_cost_ref = {}
    for year, country_code in tqdm(
        product_range_year_country,
        total=len(product_range_year_country),
        desc="Opex Cost Loop",
    ):
        technology_opex_values = get_opex_costs(
            country_code,
            year,
            variable_cost_summary,
            other_opex_df,
            carbon_tax_full_ref
        )
        assert technology_opex_values.isnull().values.any() == False, f"DF entry has nans: {technology_opex_values}"
        opex_cost_ref[(year, country_code)] = technology_opex_values
    return opex_cost_ref

@timer_func
def generate_total_opex_cost_reference(
    scenario_dict: dict, pkl_paths: Union[dict, None] = None, serialize: bool = False
) -> pd.DataFrame:
    logger.info("Total Opex Reference Preprocessing")
    _, intermediate_path, final_path = return_pkl_paths(scenario_dict["scenario_name"], pkl_paths)
    carbon_tax_reference = read_pickle_folder(
        intermediate_path, "carbon_tax_reference", "df"
    )
    # Variable Cost Preprocessing
    variable_costs_regional = read_pickle_folder(
        intermediate_path, "variable_costs_regional", "df"
    )
    variable_cost_summary = variable_costs_regional.rename(
        mapper={"cost": "value"}, axis=1
    )
    variable_cost_summary.drop(TECHNOLOGIES_TO_DROP, level="technology", inplace=True)
    test_negative_df_values(variable_cost_summary)

    # Other opex processing
    opex_values_dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    other_opex_df = opex_values_dict["other_opex"].swaplevel().copy()
    other_opex_df.drop(TECHNOLOGIES_TO_DROP, level="Technology", inplace=True)

    # Get iteration loop
    steel_plants = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
    product_range_year_country = list(itertools.product(
        MODEL_YEAR_RANGE, steel_plants["country_code"].unique()))

    logger.info("Creating Total Opex Reference Table")

    total_opex_reference = total_opex_cost_ref_loop(
        product_range_year_country,
        variable_cost_summary,
        other_opex_df,
        carbon_tax_reference
    )
    if serialize:
        logger.info("-- Serializing dataframe")
        serialize_file(
            total_opex_reference,
            intermediate_path,
            "total_opex_reference",
        )
    return total_opex_reference
