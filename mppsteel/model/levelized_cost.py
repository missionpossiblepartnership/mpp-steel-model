"""Calculation Functions used to derive various forms of Cost of Steelmaking."""

import itertools
import pandas as pd
import numpy as np
import numpy_financial as npf

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_FORMATTED,
    DISCOUNT_RATE,
    INVESTMENT_CYCLE_DURATION_YEARS,
    STEEL_PLANT_LIFETIME_YEARS,
)
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST, SWITCH_DICT

logger = get_logger(__name__)


def calculate_cc(
    capex_ref: dict,
    year: int,
    year_span: range,
    technology: str,
    discount_rate: float,
) -> float:
    """Calculates the capital charges from a capex DataFrame reference and inputted function arguments.

    Args:
        capex_dict (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        year (int): The year you want to calculate the capital charge for.
        year_span (range): The year span for the capital charge values (used in the PV calculation).
        technology (str): The technology you want to calculate the capital charge for.
        discount_rate (float): The discount rate to apply to the capital charge amounts.
        cost_type (str): The cost you want to calculate `brownfield` or `greenfield`.

    Returns:
        float: The capital charge value.
    """
    year_range = range(year, year + year_span)
    year_range = [year if (year <= MODEL_YEAR_END) else min(MODEL_YEAR_END, year) for year in year_range]
    value_arr = [capex_ref[(year, technology)] for year in year_range]
    return npf.npv(discount_rate, value_arr)


def apply_lcost(
    row, variable_cost_ref: pd.DataFrame, combined_capex_ref: dict, include_greenfield: bool = True
):
    """Applies the Levelized Cost function to a given row in a DataFrame.

    Args:
        row (_type_): A vectorized DataFrame row from .apply function.
        v_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_costs (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        include_greenfield (bool, optional): A boolean flag to toggle the greenfield specific calculations. Defaults to True.

    Returns:
        _type_: An amended vectorized DataFrame row from .apply function.
    """
    variable_cost = variable_cost_ref[(row.year, row.country_code, row.technology)]
    other_opex_cost = combined_capex_ref['other_opex'][(row.year, row.technology)]
    discount_rate = DISCOUNT_RATE
    relining_year_span = INVESTMENT_CYCLE_DURATION_YEARS
    life_of_plant = STEEL_PLANT_LIFETIME_YEARS
    greenfield_cost = 0

    renovation_cost = calculate_cc(
        combined_capex_ref['brownfield'],
        row.year,
        relining_year_span,
        row.technology,
        discount_rate
    )
    if include_greenfield:
        greenfield_cost = calculate_cc(
            combined_capex_ref['greenfield'],
            row.year,
            life_of_plant,
            row.technology,
            discount_rate
        )
    row["levelised_cost"] = (
        other_opex_cost + variable_cost + renovation_cost + greenfield_cost
    )
    return row


def create_df_reference(plant_df: pd.DataFrame, cols_to_create: list) -> pd.DataFrame:
    """Creates a DataFrame reference for the Levelized Cost values to be inserted.

    Args:
        plant_df: Plant DataFrame containing Plant Metadata.
        cols_to_create (list): A list of columns to create and set initial values for.

    Returns:
        pd.DataFrame: A DataFrame reference.
    """
    country_codes = plant_df["country_code"].unique()
    init_cols = ["year", "country_code", "technology"]
    df_list = []
    technologies = SWITCH_DICT.keys()
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    product_range_full = list(itertools.product(year_range, country_codes, technologies))
    for year, country_code, tech in tqdm(product_range_full, total=len(product_range_full), desc='DataFrame Reference'):
        entry = dict(zip(init_cols, [year, country_code, tech]))
        df_list.append(entry)
    combined_df = pd.DataFrame(df_list)
    for column in cols_to_create:
        combined_df[column] = ""
    return combined_df


def create_levelised_cost(
    plant_df: pd.DataFrame, variable_costs: pd.DataFrame, capex_ref: dict, include_greenfield=True
) -> pd.DataFrame:
    """Generate a DataFrame with Levelised Cost values.
    Args:
        plant_df: Plant DataFrame containing Plant Metadata.
        variable_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_ref (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        include_greenfield (bool, optional): A boolean flag to toggle the greenfield specific calculations. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with Levelised Cost of Steelmaking values.
    """
    lev_cost = create_df_reference(plant_df, ["levelised_cost"])

    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    technologies = SWITCH_DICT.keys()
    steel_plant_country_codes = list(plant_df["country_code"].unique())
    product_range_year_tech = list(itertools.product(year_range, technologies))
    product_range_full = list(itertools.product(year_range, steel_plant_country_codes, technologies))
    brownfield_capex_ref = {}
    greenfield_capex_ref = {}
    variable_cost_ref = {}
    other_opex_ref = {}
    for year, country_code, tech in tqdm(product_range_full, total=len(product_range_full), desc='Variable Costs Reference'):
        variable_cost_ref[(year, country_code, tech)] =  variable_costs.loc[country_code, year, tech]["cost"]
    
    for year, tech in tqdm(product_range_year_tech, total=len(product_range_year_tech), desc='Capex Ref Loop'):
        brownfield_capex_ref[(year, tech)] = capex_ref['brownfield'].loc[tech, year]["value"]
        greenfield_capex_ref[(year, tech)] = capex_ref['greenfield'].loc[tech, year]["value"]
        other_opex_ref[(year, tech)] = capex_ref["other_opex"].loc[tech, year]["value"]
    
    combined_capex_ref = {
        'brownfield': brownfield_capex_ref,
        'greenfield': greenfield_capex_ref,
        'other_opex': other_opex_ref
    }

    tqdma.pandas(desc="Applying Levelized Cost")
    lev_cost = lev_cost.progress_apply(
        apply_lcost,
        variable_cost_ref=variable_cost_ref,
        combined_capex_ref=combined_capex_ref,
        include_greenfield=include_greenfield,
        axis=1,
    )
    return lev_cost


@timer_func
def generate_levelized_cost_results(scenario_dict: dict, serialize: bool = False, steel_plant_df = None) -> dict:
    """Full flow to create the Levelized Cost DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary with the Levelized Cost DataFrame.
    """
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    variable_costs_regional = read_pickle_folder(
        intermediate_path, "variable_costs_regional", "df"
    )
    capex_dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    if not isinstance(steel_plant_df, pd.DataFrame):
        steel_plant_df = read_pickle_folder(
            PKL_DATA_FORMATTED, "steel_plants_processed", "df"
        )
    lcos_data = create_levelised_cost(
        steel_plant_df, variable_costs_regional, capex_dict, include_greenfield=True
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(lcos_data, intermediate_path, "levelized_cost")
    return lcos_data
