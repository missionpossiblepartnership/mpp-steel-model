"""Calculation Functions used to derive various forms of Cost of Steelmaking."""

from functools import lru_cache
import itertools
import pandas as pd

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file,
    get_scenario_pkl_path,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    KILOTON_TO_TON_FACTOR,
    MODEL_YEAR_END,
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED,
    DISCOUNT_RATE,
    STEEL_PLANT_LIFETIME_YEARS,
)
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST

logger = get_logger(__name__)


def create_contracted_year_range(year_start: int, year_span: int) -> list:
    """Limits a year range based on a max model year.

    Args:
        year_start (int): The start of the model
        year_span (int): The span of the range.

    Returns:
        list: _description_
    """
    year_range = range(year_start, year_start + year_span)
    return [
        year if (year <= MODEL_YEAR_END) else min(MODEL_YEAR_END, year)
        for year in year_range
    ]


@lru_cache(maxsize=1000)
def calculate_cycle_discount_factor(
    start_year: int, cycle_year: int, discount_rate: int
) -> float:
    """Calculates the discount factor to apply for a specific year.

    Args:
        start_year (int): The start year of the discount factor
        cycle_year (int): The specific year that the cycle is in, starting from the start year.
        discount_rate (int): The discount rate that represents the cost of capital.

    Returns:
        float: A float value of the overall discount factor to apply.
    """
    return (1 + discount_rate) ** (cycle_year - start_year)


def calculate_lcox_cost(
    capex_ref: dict,
    variable_cost_ref: dict,
    year: int,
    country_code: str,
    technology: str,
    year_span: range,
    discount_rate: float,
) -> float:
    """Calculates the levelised cost component from a capex ref and variable costs ref and inputted function arguments.

    Args:
        capex_ref (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        variable_cost_ref (dict): The dictionary of the variable costs.
        year (int): The year you want to calculate the capital charge for.
        country_code (str): The country_code to calculate the variable costs for.
        technology (str): The technology you want to capex and variable costs for.
        year_span (range): The year span for the levelised cost.
        discount_rate (float): The discount rate to apply to the capital charge amounts.

    Returns:
        float: The levelised cost capital charge.
    """
    year_start = year
    year_range = create_contracted_year_range(year_start, year_span)

    sum_container = []
    for cycle_year in year_range:
        cycle_discount_factor = calculate_cycle_discount_factor(
            year_start, cycle_year, discount_rate
        )
        brownfield_value = capex_ref["brownfield"][(cycle_year, technology)]
        greenfield_value = capex_ref["greenfield"][(cycle_year, technology)]
        fixed_opex_value = capex_ref["other_opex"][(cycle_year, technology)]
        variable_opex_value = variable_cost_ref[(cycle_year, country_code, technology)]
        cost_factor = (
            brownfield_value + greenfield_value + fixed_opex_value + variable_opex_value
        ) / cycle_discount_factor
        sum_container.append(cost_factor)
    return sum(sum_container)


def get_lcost_costs(
    row: pd.Series, variable_cost_ref: pd.DataFrame, combined_capex_ref: dict
) -> pd.Series:
    """Applies the Levelized Cost function to a given row in a DataFrame.

    Args:
        row (pd.Series): A vectorized DataFrame row from .apply function.
        variable_cost_ref (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        combined_capex_ref (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.

    Returns:
        pd.Series: The levelized costs vectorized from a DataFrame row from the apply function.
    """
    lcox_cost = calculate_lcox_cost(
        combined_capex_ref,
        variable_cost_ref,
        row.year,
        row.country_code,
        row.technology,
        STEEL_PLANT_LIFETIME_YEARS,
        DISCOUNT_RATE,
    )
    row["costs"] = lcox_cost
    return row


def get_lcost_capacity(plant_df: pd.DataFrame) -> dict:
    """Calculates the capacity for each plant and year for the purpose of calculating the Levelized Cost.

    Args:
        plant_df (pd.DataFrame): The DataFrame containing the Steel plant metadata.

    Returns:
        dict: A dictoinary reference for each plant containing [plant_id][year] keys.
    """
    ref_container = {}
    for row in tqdm(
        plant_df.itertuples(), total=len(plant_df), desc="LCOX Capacity Ref"
    ):
        ref_container[row.plant_id] = {}
        for year_start in MODEL_YEAR_RANGE:
            year_range = range(year_start, year_start + STEEL_PLANT_LIFETIME_YEARS)
            sum_container = []
            for cycle_year in year_range:
                capacity = row.plant_capacity / KILOTON_TO_TON_FACTOR
                cycle_discount_factor = calculate_cycle_discount_factor(
                    year_start, cycle_year, DISCOUNT_RATE
                )
                sum_container.append(capacity / cycle_discount_factor)
            ref_container[row.plant_id][year_start] = sum(sum_container)
    return ref_container


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


def combined_levelized_cost(
    lev_cost_df: pd.DataFrame, capacity_ref: dict, plant_df: pd.DataFrame
) -> pd.DataFrame:
    """Combined the costs and capacity compoenents of levelized costs to form a new levelized_cost column.

    Args:
        lev_cost_df (pd.DataFrame): The levelized cost DataFrame containing the `cost` component of levelized cost.
        capacity_ref (dict): The capacity reference dict for the levelized costs.
        plant_df (pd.DataFrame): The Steel plant DataFrame.

    Returns:
        pd.DataFrame: The combined dataframe for the levelised cost.
    """
    plant_df_c = plant_df.set_index(["plant_id"]).copy()
    lev_cost_df_c = lev_cost_df.set_index(["year", "country_code", "technology"]).sort_index(ascending=True).copy()
    product_ref = list(itertools.product(capacity_ref.keys(), MODEL_YEAR_RANGE))
    df_container = []
    for plant_id, year in tqdm(
        product_ref, total=len(product_ref), desc="Combining LCOX"
    ):
        country_code = plant_df_c.loc[plant_id, "country_code"]
        capacity_value = capacity_ref[plant_id][year]
        new_df = lev_cost_df_c.loc[(year, country_code)].copy()
        new_df["capacity"] = capacity_value
        new_df["levelized_cost"] = new_df["costs"] / capacity_value
        new_df["year"] = year
        new_df["country_code"] = country_code
        new_df["plant_id"] = plant_id
        df_container.append(new_df)
    return pd.concat(df_container).reset_index()


def create_levelized_cost(
    plant_df: pd.DataFrame, variable_costs: pd.DataFrame, capex_ref: dict
) -> pd.DataFrame:
    """Generate a DataFrame with Levelized Cost values.
    Args:
        plant_df: Plant DataFrame containing Plant Metadata.
        variable_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_ref (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.

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
    other_opex_ref = (
        capex_ref["other_opex"]
        .reset_index()
        .set_index(["Year", "Technology"])
        .to_dict()["value"]
    )
    variable_cost_ref = (
        variable_costs.reset_index()
        .set_index(["year", "country_code", "technology"])
        .to_dict()["cost"]
    )

    combined_capex_ref = {
        "brownfield": brownfield_capex_ref,
        "greenfield": greenfield_capex_ref,
        "other_opex": other_opex_ref,
    }

    df_reference = create_df_reference(plant_df, ["levelized_cost", "costs"])

    tqdma.pandas(desc="Creating Cost Reference")
    lev_cost = df_reference.progress_apply(
        get_lcost_costs,
        variable_cost_ref=variable_cost_ref,
        combined_capex_ref=combined_capex_ref,
        axis=1,
    )

    capacity_ref = get_lcost_capacity(plant_df)
    plant_lev_cost_reference = combined_levelized_cost(lev_cost, capacity_ref, plant_df)
    return summarise_levelized_cost(plant_lev_cost_reference)


@timer_func
def generate_levelized_cost_results(
    scenario_dict: dict, serialize: bool = False, steel_plant_df=None
) -> dict:
    """Full flow to create the Levelized Cost DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary with the Levelized Cost DataFrame.
    """
    intermediate_path = get_scenario_pkl_path(
        scenario_dict["scenario_name"], "intermediate"
    )
    variable_costs_regional = read_pickle_folder(
        intermediate_path, "variable_costs_regional", "df"
    )
    capex_dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    if not isinstance(steel_plant_df, pd.DataFrame):
        steel_plant_df = read_pickle_folder(
            PKL_DATA_FORMATTED, "steel_plants_processed", "df"
        )
    lcos_data = create_levelized_cost(
        steel_plant_df, variable_costs_regional, capex_dict
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(lcos_data, intermediate_path, "levelized_cost")
    return lcos_data
