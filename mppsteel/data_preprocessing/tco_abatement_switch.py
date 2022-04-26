"""Script to run a full reference dataframe for tco switches and abatement switches"""

import itertools
from functools import lru_cache

import pandas as pd

from tqdm import tqdm

from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.data_preprocessing.tco_calculation_functions import (
    get_discounted_opex_values, calculate_capex, get_opex_costs
)
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.model_tests.df_tests import test_negative_df_values, test_negative_list_values
from mppsteel.config.reference_lists import SWITCH_DICT, TECH_REFERENCE_LIST
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED,
    INVESTMENT_CYCLE_DURATION_YEARS,
    DISCOUNT_RATE,
)

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def tco_regions_ref_generator(scenario_dict: dict) -> pd.DataFrame:
    """Creates a summary of TCO values for each technology and region.

    Args:
        electricity_cost_scenario (str): The scenario that determines the electricity cost from the shared model.
        grid_scenario (str): The scenario that determines the grid decarbonisation cost from the shared model.
        hydrogen_cost_scenario (str): The scenario that determines the hydrogen cost from the shared model.

    Returns:
        pd.DataFrame: A DataFrame containing the components necessary to calculate TCO (not including green premium).
    """
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    carbon_tax_df = read_pickle_folder(
        intermediate_path, "carbon_tax_timeseries", "df"
    )
    variable_costs_regional = read_pickle_folder(
        intermediate_path, "variable_costs_regional", "df"
    )
    opex_values_dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    calculated_s1_emissivity = read_pickle_folder(
        intermediate_path, "calculated_s1_emissivity", "df"
    )
    calculated_s2_emissivity = read_pickle_folder(
        intermediate_path, "calculated_s2_emissivity", "df"
    )
    capex_df = read_pickle_folder(PKL_DATA_FORMATTED, "capex_switching_df", "df")
    capex_df.reset_index(inplace=True)
    capex_df.rename({
        'Start Technology': 'start_technology', 
        'New Technology': 'end_technology', 
        'Year': 'year', 
        'value': 'capex_value'}, axis=1, inplace=True
    )
    capex_df = capex_df.set_index(['start_technology', 'year']).sort_index(ascending=True)
    steel_plants = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )

    # Preprocessing
    techs_to_drop = ["Charcoal mini furnace", "Close plant"]
    other_opex_df = opex_values_dict["other_opex"].swaplevel().copy()
    other_opex_df.drop(techs_to_drop, level='Technology', inplace=True)
    variable_cost_summary = variable_costs_regional.rename(mapper={"cost": "value"}, axis=1)
    variable_cost_summary.drop(techs_to_drop, level='technology', inplace=True)
    test_negative_df_values(variable_cost_summary)
    calculated_s2_emissivity.set_index(['year', 'country_code', 'technology'], inplace=True)
    calculated_s2_emissivity.rename(mapper={'s2_emissivity': 'emissions'}, axis=1, inplace=True)
    calculated_s2_emissivity = calculated_s2_emissivity.sort_index(ascending=True)
    calculated_s1_emissivity.drop(techs_to_drop, level='technology', inplace=True)
    carbon_tax_df = carbon_tax_df.set_index("year")
    # Prepare looping references
    steel_plant_country_codes = steel_plants["country_code"].unique()
    product_range_year_country = list(itertools.product(MODEL_YEAR_RANGE, steel_plant_country_codes))
    product_range_year_tech = list(itertools.product(MODEL_YEAR_RANGE, TECH_REFERENCE_LIST))
    product_year_country_code_tech = list(itertools.product(MODEL_YEAR_RANGE, steel_plant_country_codes, TECH_REFERENCE_LIST))
    column_order = [
        "country_code",
        "year",
        "start_technology",
        "end_technology",
        "capex_value",
        "discounted_opex",
    ]

    opex_cost_ref = {}

    for year, country_code in tqdm(product_range_year_country, total=len(product_range_year_country), desc='Opex Cost Loop'):
        technology_opex_values = get_opex_costs(
            country_code,
            year,
            variable_cost_summary,
            other_opex_df,
            calculated_s1_emissivity,
            calculated_s2_emissivity,
            carbon_tax_df
        )
        opex_cost_ref[(year, country_code)] = technology_opex_values

    discounted_capex_ref = {}
    for year, country_code in tqdm(product_range_year_country, total=len(product_range_year_country), desc='(Discounted) Opex Loop'):
        discounted_opex_values = get_discounted_opex_values(
            country_code,
            year,
            opex_cost_ref,
            int_rate=DISCOUNT_RATE,
            year_interval=INVESTMENT_CYCLE_DURATION_YEARS
        )
        discounted_capex_ref[(year, country_code)] = discounted_opex_values

    switch_capex_cost_ref = {}
    for year, tech in tqdm(product_range_year_tech, total=len(product_range_year_tech), desc='Capex Cost Loop'):
        capex_values = calculate_capex(capex_df, year, tech)
        switch_capex_cost_ref[(year, tech)] = capex_values

    technology_df_ref = {tech: pd.DataFrame({'start_technology': tech, 'end_technology': SWITCH_DICT[tech]}) for tech in TECH_REFERENCE_LIST}

    df_list = []
    for year, country_code, start_technology in tqdm(product_year_country_code_tech, total=len(product_year_country_code_tech), desc='Full Opex Reference'):
        new_df = technology_df_ref[start_technology]
        new_df["year"] = year
        new_df["country_code"] = country_code
        new_df['discounted_opex'] = new_df['end_technology'].apply(lambda end_technology: discounted_capex_ref[(year, country_code)][end_technology])
        switch_capex_values = switch_capex_cost_ref[(year, start_technology)]
        capex_opex_values = new_df.set_index(['end_technology']).join(switch_capex_values, on="end_technology")
        df_list.append(capex_opex_values)
    return pd.concat(df_list).reset_index()[column_order]


def get_abatement_difference(
    emissions_ref: dict,
    year: int,
    country_code: str,
    base_tech: str,
    switch_tech: str,
    date_span: int,
) -> float:
    
    @lru_cache(maxsize=200000)
    def return_abatement_value(base_tech_sum, switch_tech_sum):
        return float(base_tech_sum - switch_tech_sum)
    
    year_range = range(year, year + date_span)
    year_range = [year if (year <= MODEL_YEAR_END) else min(MODEL_YEAR_END, year) for year in year_range]
    base_tech_list = [emissions_ref[(year, country_code, base_tech)] for year in year_range]
    switch_tech_list = [emissions_ref[(year, country_code, switch_tech)] for year in year_range]
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
    combined_emissivity = (
        combined_emissivity.reset_index(drop=True)
        .set_index(["year", "country_code", "technology"])
        .copy()
    )
    
    country_codes = combined_emissivity.index.get_level_values(1).unique()
    product_range_full = list(itertools.product(MODEL_YEAR_RANGE, country_codes, TECH_REFERENCE_LIST))

    technology_emissions_ref = combined_emissivity.to_dict()['combined_emissivity']

    df_list = []
    for year, country_code, base_tech in tqdm(
        product_range_full, total=len(product_range_full), desc='Emissions DataFrame'):
        for switch_tech in SWITCH_DICT[base_tech]:
            value_difference = get_abatement_difference(
                technology_emissions_ref,
                year,
                country_code,
                base_tech,
                switch_tech,
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


def tco_calculator(tco_ref_df: pd.DataFrame):
    rmi_mapper = create_country_mapper('rmi')
    df = tco_ref_df.copy()
    df["tco"] = (df["discounted_opex"] + df["capex_value"]) / INVESTMENT_CYCLE_DURATION_YEARS
    df['region'] = df['country_code'].apply(lambda x: rmi_mapper[x])
    return df


@timer_func
def tco_presolver_reference(scenario_dict: dict, serialize: bool = False) -> pd.DataFrame:
    """Complete flow to create two reference TCO DataFrames.
    The first DataFrame `tco_summary` create contains only TCO summary data on a regional level (not plant level).
    The second DataFrame `tco_reference_data` contains the full TCO reference data on a plant level, including green premium calculations. 

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the complete TCO reference DataFrame (including green premium values).
    """
    logger.info("Running TCO Reference Sheet")
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    opex_capex_reference_data = tco_regions_ref_generator(scenario_dict)
    tco_summary = tco_calculator(opex_capex_reference_data)
    if serialize:
        logger.info("-- Serializing dataframe")
        serialize_file(
            tco_summary, intermediate_path, "tco_summary_data"
        )
    return tco_summary


@timer_func
def abatement_presolver_reference(
    scenario_dict: dict, serialize: bool = False
) -> pd.DataFrame:
    """Complete flow required to create the emissivity abatement presolver reference table.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the emissivity abatement values.
    """
    logger.info("Running Abatement Reference Sheet")
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    calculated_emissivity_combined = read_pickle_folder(
        intermediate_path, "calculated_emissivity_combined", "df"
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
            intermediate_path,
            "emissivity_abatement_switches",
        )
    return emissivity_abatement_switches
