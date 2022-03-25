"""Calculation Functions used to derive various forms of Cost of Steelmaking."""

import pandas as pd
import numpy as np
import numpy_financial as npf

from tqdm.auto import tqdm as tqdma

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_INTERMEDIATE,
    DISCOUNT_RATE,
    INVESTMENT_CYCLE_DURATION_YEARS,
    STEEL_PLANT_LIFETIME_YEARS,
)
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST

logger = get_logger("Levelized Cost")


def calculate_cc(
    capex_dict: dict,
    year: int,
    year_span: range,
    technology: str,
    discount_rate: float,
    cost_type: str,
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
    value_arr = np.array([])
    for eval_year in year_range:
        year_loop_val = min(MODEL_YEAR_END, eval_year)
        value = capex_dict[cost_type].loc[technology, year_loop_val]["value"]
        value_arr = np.append(value_arr, value)
    return npf.npv(discount_rate, value_arr)


def apply_lcost(
    row, v_costs: pd.DataFrame, capex_costs: dict, include_greenfield: bool = True
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
    variable_cost = v_costs.loc[row.country_code, row.year, row.technology]["cost"]
    other_opex_cost = capex_costs["other_opex"].loc[row.technology, row.year]["value"]
    discount_rate = DISCOUNT_RATE
    relining_year_span = INVESTMENT_CYCLE_DURATION_YEARS
    life_of_plant = STEEL_PLANT_LIFETIME_YEARS
    greenfield_cost = 0

    renovation_cost = calculate_cc(
        capex_costs,
        row.year,
        relining_year_span,
        row.technology,
        discount_rate,
        "brownfield",
    )
    if include_greenfield:
        greenfield_cost = calculate_cc(
            capex_costs,
            row.year,
            life_of_plant,
            row.technology,
            discount_rate,
            "greenfield",
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
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in year_range:
        for country_code in country_codes:
            for technology in TECH_REFERENCE_LIST:
                entry = dict(zip(init_cols, [year, country_code, technology]))
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
        pd.DataFrame: A DataFrane with Levelised Cost of Steelmaking values.
    """
    lev_cost = create_df_reference(plant_df, ["levelised_cost"])
    tqdma.pandas(desc="Applying Levelized Cost")
    lev_cost = lev_cost.progress_apply(
        apply_lcost,
        v_costs=variable_costs,
        capex_costs=capex_ref,
        include_greenfield=include_greenfield,
        axis=1,
    )
    return lev_cost


@timer_func
def generate_levelized_cost_results(steel_plant_df = None, serialize: bool = False) -> dict:
    """Full flow to create the Levelized Cost DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary with the Levelized Cost DataFrame.
    """
    variable_costs_regional = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "variable_costs_regional", "df"
    )
    capex_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_dict", "df")
    if not isinstance(steel_plant_df, pd.DataFrame):
        steel_plant_df = read_pickle_folder(
            PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
        )
    lcos_data = create_levelised_cost(
        steel_plant_df, variable_costs_regional, capex_dict, include_greenfield=True
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(lcos_data, PKL_DATA_INTERMEDIATE, "levelized_cost")
    return lcos_data
