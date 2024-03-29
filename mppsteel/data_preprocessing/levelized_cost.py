"""Calculation Functions used to derive various forms of Cost of Steelmaking."""

import itertools
from typing import Union
import pandas as pd

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    return_pkl_paths,
    serialize_file,
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    AVERAGE_CAPACITY_MT,
    AVERAGE_CUF,
    INVESTMENT_CYCLE_DURATION_YEARS,
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED,
    DISCOUNT_RATE,
    PKL_DATA_IMPORTS
)
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST

logger = get_logger(__name__)


def acc_calculator(discount_rate: float, plant_lifetime: int) -> float:
    exp_discount_factor = pow(1 + discount_rate, plant_lifetime)
    return (discount_rate * exp_discount_factor) / (exp_discount_factor - 1)


def create_lcox_cost_reference(
    row: pd.DataFrame,
    capex_ref: dict,
    total_opex_reference: dict,
) -> float:
    """Calculates the levelised cost component from a capex ref and variable costs ref and inputted function arguments.

    Args:
        row (pd.DataFrame): A row of a DataFrame reference.
        capex_ref (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        total_opex_reference (dict): A dictionary containing total opex DataFrame with the (year, country_code) as keys
    Returns:
        float: The levelised cost capital charge.
    """
    year = row.year
    country_code = row.country_code
    technology = row.technology
    greenfield_value = capex_ref["greenfield"][(year, technology)]
    total_opex_value = (
        total_opex_reference[(year, country_code)].loc[technology].values[0]
    )
    row.greenfield_capex = greenfield_value
    row.total_opex = total_opex_value
    return row


def create_df_reference(country_codes: list, cols_to_create: list) -> pd.DataFrame:
    """Creates a DataFrame reference for the Levelized Cost values to be inserted.

    Args:
        country_codes (list): list containing all the unique plant country codes
        cols_to_create (list): A list of columns to create and set initial values for.

    Returns:
        pd.DataFrame: A DataFrame reference.
    """
    init_cols = ["year", "country_code", "technology"]
    df_list = []
    product_range_full = list(
        itertools.product(MODEL_YEAR_RANGE, country_codes, TECH_REFERENCE_LIST)
    )
    for year, country_code, tech in tqdm(
        product_range_full, total=len(product_range_full), desc="DataFrame Reference"
    ):
        entry = dict(zip(init_cols, [year, country_code, tech]))
        df_list.append(entry)
    combined_df = pd.DataFrame(df_list)
    for column in cols_to_create:
        combined_df[column] = ""
    return combined_df


def summarise_levelized_cost(plant_lev_cost_df: pd.DataFrame) -> pd.DataFrame:
    """Final formatting for the full reference levelized cost DataFrame.

    Args:
        plant_lev_cost_df (pd.DataFrame): The initial Plant Levelized Cost DataFrame.

    Returns:
        pd.DataFrame: The formatted levelized cost DataFrame.
    """
    df_c = plant_lev_cost_df[
        ["year", "country_code", "technology", "levelized_cost"]
    ].copy()
    df_c = df_c.groupby(["year", "country_code", "technology"]).agg("mean")
    return df_c.reset_index()


def create_levelized_cost(
    total_opex_reference: dict,
    capex_ref: dict,
    plant_df: pd.DataFrame,
    standard_plant_ref: bool = True,
) -> pd.DataFrame:
    """Generate a DataFrame with Levelized Cost values.
    Args:
        plant_df: Plant DataFrame containing Plant Metadata.
        variable_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_ref (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        standard_plant_ref (bool): Decide whether to use a standard plant reference capacity and utilization.

    Returns:
        pd.DataFrame: A DataFrame with Levelized Cost of Steelmaking values.
    """

    brownfield_capex_ref = (
        capex_ref["brownfield"]
        .reset_index()
        .set_index(["Year", "Technology"])
        .to_dict()["value"]
    )
    greenfield_capex_ref = (
        capex_ref["greenfield"]
        .reset_index()
        .set_index(["Year", "Technology"])
        .to_dict()["value"]
    )
    combined_capex_ref = {
        "brownfield": brownfield_capex_ref,
        "greenfield": greenfield_capex_ref,
    }

    country_codes = list(plant_df["country_code"].unique())
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref", "df")
    rmi_mapper = create_country_mapper(country_ref)
    df_reference = create_df_reference(
        country_codes, ["greenfield_capex", "total_opex"]
    )
    acc = acc_calculator(DISCOUNT_RATE, INVESTMENT_CYCLE_DURATION_YEARS)

    tqdma.pandas(desc="LCOS Cost Columns population")
    lev_cost_reference = df_reference.progress_apply(
        create_lcox_cost_reference,
        total_opex_reference=total_opex_reference,
        capex_ref=combined_capex_ref,
        axis=1,
    )
    lev_cost_reference["region"] = lev_cost_reference["country_code"].apply(
        lambda x: rmi_mapper[x]
    )
    lev_cost_reference = lev_cost_reference.set_index(
        ["year", "country_code", "technology"]
    ).sort_index()

    def levelized_cost_calculation(row: pd.DataFrame, acc: float):
        return (
            (row.greenfield_capex * row.capacity * acc)
            + (row.total_opex * row.capacity * row.cuf)
        ) / (row.capacity * row.cuf)

    tqdma.pandas(desc="Creating Levelized cost values")
    if standard_plant_ref:
        lev_cost_reference["capacity"] = AVERAGE_CAPACITY_MT
        lev_cost_reference["cuf"] = AVERAGE_CUF
        lev_cost_reference["levelized_cost"] = lev_cost_reference.progress_apply(
            levelized_cost_calculation, acc=acc, axis=1
        )

    else:
        index_cols = ["year", "region", "country_code", "technology"]
        plant_df_c = plant_df.set_index(index_cols).copy()
        lev_cost_reference = lev_cost_reference.reset_index().set_index(index_cols)
        lev_cost_reference = plant_df_c.join(lev_cost_reference)
        lev_cost_reference["levelized_cost"] = lev_cost_reference.progress_apply(
            levelized_cost_calculation, acc=acc, axis=1
        )

    return lev_cost_reference.reset_index()


@timer_func
def generate_levelized_cost_results(
    scenario_dict: dict,
    pkl_paths: Union[dict, None] = None,
    serialize: bool = False,
    standard_plant_ref: bool = False,
    steel_plant_df=None,
) -> dict:
    """Full flow to create the Levelized Cost DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        pkl_paths (Union[dict, None], optional): A dictionary containing custom pickle paths. Defaults to {}.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.
        standard_plant_ref (bool): Determines whether to create a netural levelized cost reference with the same average capacity and cuf values or custom ones.

    Returns:
        dict: A dictionary with the Levelized Cost DataFrame.
    """
    _, intermediate_path, _ = return_pkl_paths(
        scenario_name=scenario_dict["scenario_name"], paths=pkl_paths
    )
    total_opex_reference = read_pickle_folder(
        intermediate_path, "total_opex_reference", "df"
    )
    capex_dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    if not isinstance(steel_plant_df, pd.DataFrame):
        steel_plant_df = read_pickle_folder(
            PKL_DATA_FORMATTED, "steel_plants_processed", "df"
        )
    lcos_data = create_levelized_cost(
        total_opex_reference,
        capex_dict,
        steel_plant_df,
        standard_plant_ref=standard_plant_ref,
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(lcos_data, intermediate_path, "levelized_cost_standardized")
    return lcos_data
