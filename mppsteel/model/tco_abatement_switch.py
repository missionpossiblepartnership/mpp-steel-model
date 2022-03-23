"""Script to run a full reference dataframe for tco switches and abatement switches"""
import itertools
from functools import lru_cache

import pandas as pd
import numpy as np
import numpy_financial as npf

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.data_loading.data_interface import load_business_cases
from mppsteel.model.tco_calculation_functions import tco_calc, calculate_green_premium
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.config.reference_lists import LOW_CARBON_TECHS, SWITCH_DICT
from mppsteel.config.model_config import (
    DISCOUNT_RATE,
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_INTERMEDIATE,
    PKL_DATA_IMPORTS,
    INVESTMENT_CYCLE_DURATION_YEARS,
)

from mppsteel.utility.location_utility import (
    country_mapping_fixer,
    country_matcher,
    match_country,
    get_region_from_country_code,
)

from mppsteel.utility.log_utility import get_logger

logger = get_logger("TCO & Abatement switches")


def tco_regions_ref_generator(
    electricity_cost_scenario: str, grid_scenario: str, hydrogen_cost_scenario: str
) -> pd.DataFrame:
    """Creates a summary of TCO values for each technology and region.

    Args:
        electricity_cost_scenario (str): The scenario that determines the electricity cost from the shared model.
        grid_scenario (str): The scenario that determines the grid decarbonisation cost from the shared model.
        hydrogen_cost_scenario (str): The scenario that determines the hydrogen cost from the shared model.

    Returns:
        pd.DataFrame: A DataFrame containing the components necessary to calculate TCO (not including green premium).
    """

    carbon_tax_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "carbon_tax_timeseries", "df"
    )
    variable_costs_regional = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "variable_costs_regional", "df"
    )
    power_model = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "power_model_formatted", "df"
    )
    hydrogen_model = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "hydrogen_model_formatted", "df"
    )
    opex_values_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_dict", "df")
    business_cases = load_business_cases()
    calculated_s1_emissivity = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "calculated_s1_emissivity", "df"
    )
    capex_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_switching_df", "df")
    country_ref_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    steel_plants = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
    )
    steel_plant_country_codes = list(steel_plants["country_code"].unique())
    technologies = SWITCH_DICT.keys()
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in tqdm(
        year_range, total=len(year_range), desc="All Regions TCO: Year Loop"
    ):
        for country_code in tqdm(
            steel_plant_country_codes,
            total=len(steel_plant_country_codes),
            desc="All Regions TCO: Region Loop",
        ):
            for tech in technologies:
                tco_df = tco_calc(
                    country_code,
                    year,
                    tech,
                    carbon_tax_df,
                    business_cases,
                    variable_costs_regional,
                    power_model,
                    hydrogen_model,
                    opex_values_dict["other_opex"],
                    calculated_s1_emissivity,
                    country_ref_dict,
                    capex_df,
                    INVESTMENT_CYCLE_DURATION_YEARS,
                    electricity_cost_scenario,
                    grid_scenario,
                    hydrogen_cost_scenario,
                )
                df_list.append(tco_df)
    return pd.concat(df_list).reset_index(drop=True)


def get_abatement_difference(
    df: pd.DataFrame,
    year: int,
    country_code: str,
    base_tech: str,
    switch_tech: str,
    emission_type: str,
    emissivity_mapper: dict,
    date_span: int,
) -> float:
    @lru_cache(maxsize=200000)
    def return_abatement_value(base_tech_sum, switch_tech_sum):
        return float(base_tech_sum - switch_tech_sum)

    year_range = range(year, year + date_span)
    emission_val = emissivity_mapper[emission_type]
    base_tech_list = []
    switch_tech_list = []
    for eval_year in year_range:
        year_loop_val = min(MODEL_YEAR_END, eval_year)
        base_tech_val = df.loc[year_loop_val, country_code, base_tech][emission_val]
        switch_tech_val = df.loc[year_loop_val, country_code, switch_tech][emission_val]
        base_tech_list.append(base_tech_val)
        switch_tech_list.append(switch_tech_val)
    return return_abatement_value(sum(base_tech_list), sum(switch_tech_list))


def emissivity_abatement(combined_emissivity: pd.DataFrame, scope: str) -> pd.DataFrame:
    """Creates a emissivity abatement reference DataFrame based on an emissivity input DataFrame. 

    Args:
        combined_emissivity (pd.DataFrame): A combined emissivity DataFrame containing data on scopes 1, 2, 3 and combined emissivity per technology and region.
        scope (str): The scope you want to create emission abatement for.

    Returns:
        pd.DataFrame: A DataFrame containing emissivity abatement potential for each possible technology switch.
    """
    logger.info(
        "Getting all Emissivity Abatement combinations for all technology switches"
    )
    emissivity_mapper = {
        "s1": "s1_emissivity",
        "s2": "s2_emissivity",
        "s3": "s3_emissivity",
        "combined": "combined_emissivity",
    }
    combined_emissivity = (
        combined_emissivity.reset_index(drop=True)
        .set_index(["year", "country_code", "technology"])
        .copy()
    )
    country_codes = combined_emissivity.index.get_level_values(1).unique()
    technologies = combined_emissivity.index.get_level_values(2).unique()
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in tqdm(
        year_range, total=len(year_range), desc="Emission Abatement: Year Loop"
    ):
        for country_code in country_codes:
            for base_tech in technologies:
                for switch_tech in SWITCH_DICT[base_tech]:
                    value_difference = get_abatement_difference(
                        combined_emissivity[["combined_emissivity"]],
                        year,
                        country_code,
                        base_tech,
                        switch_tech,
                        "combined",
                        emissivity_mapper,
                        INVESTMENT_CYCLE_DURATION_YEARS,
                    )
                    entry = {
                        "year": year,
                        "country_code": country_code,
                        "base_tech": base_tech,
                        "switch_tech": switch_tech,
                        f"abated_{scope}_emissivity": value_difference,
                    }
                    df_list.append(entry)
    return pd.DataFrame(df_list)


@timer_func
def tco_presolver_reference(scenario_dict, serialize: bool = False) -> pd.DataFrame:
    """Complete flow to create two reference TCO DataFrames.
    The first DataFrame `tco_summary` create contains only TCO summary data on a regional level (not plant level).
    The second DataFrame `tco_reference_data` contains the full TCO reference data on a plant level, including green premium calculations. 

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the complete TCO reference DataFrame (including green premium values).
    """
    country_ref_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    electricity_cost_scenario = scenario_dict["electricity_cost_scenario"]
    grid_scenario = scenario_dict["grid_scenario"]
    hydrogen_cost_scenario = scenario_dict["hydrogen_cost_scenario"]
    opex_capex_reference_data = tco_regions_ref_generator(
        electricity_cost_scenario, grid_scenario, hydrogen_cost_scenario
    )
    tco_summary = opex_capex_reference_data.copy()
    tco_summary["tco"] = tco_summary["discounted_opex"] + tco_summary["capex_value"]
    tco_summary["tco"] = tco_summary["tco"] / INVESTMENT_CYCLE_DURATION_YEARS
    tco_summary['region']= tco_summary['country_code'].apply(lambda x: get_region_from_country_code(
        x, "rmi_region", country_ref_dict
    ))
    if serialize:
        logger.info("-- Serializing dataframe")
        serialize_file(
            tco_summary, PKL_DATA_INTERMEDIATE, "tco_summary_data"
        )
    return tco_summary


@timer_func
def abatement_presolver_reference(
    scenario_dict, serialize: bool = False
) -> pd.DataFrame:
    """Complete flow required to create the emissivity abatement presolver reference table.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the emissivity abatement values.
    """
    logger.info("Running Abatement Reference Sheet")
    calculated_emissivity_combined = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "calculated_emissivity_combined", "df"
    )
    emissivity_abatement_switches = emissivity_abatement(
        calculated_emissivity_combined, scope="combined"
    )
    emissivity_abatement_switches = add_results_metadata(
        emissivity_abatement_switches, scenario_dict, single_line=True
    )
    if serialize:
        logger.info("-- Serializing dataframe")
        serialize_file(
            emissivity_abatement_switches,
            PKL_DATA_INTERMEDIATE,
            "emissivity_abatement_switches",
        )
    return emissivity_abatement_switches
