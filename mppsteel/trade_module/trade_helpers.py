"""Module that contains the trade helper functions"""

from copy import deepcopy
from enum import Enum

import pandas as pd

from mppsteel.config.model_config import (
    MAIN_REGIONAL_SCHEMA,
    MODEL_YEAR_START,
    TRADE_ROUNDING_NUMBER
)
from mppsteel.model_solver.solver_classes import (
    CapacityContainerClass,
    UtilizationContainerClass,
    MarketContainerClass,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.utils import join_list_as_string

logger = get_logger(__name__)


class TradeStatus(Enum):
    DOMESTIC = "Domestic"
    EXPORTER = "Exporter"
    IMPORTER = "Importer"


def return_trade_status(cos_close_to_mean: bool, trade_balance: float):
    has_overproduction = trade_balance > 0
    if not cos_close_to_mean and has_overproduction:
        return TradeStatus.DOMESTIC
    elif cos_close_to_mean and has_overproduction:
        return TradeStatus.EXPORTER
    elif not cos_close_to_mean and not has_overproduction:
        return TradeStatus.IMPORTER
    return TradeStatus.DOMESTIC


def check_relative_production_cost(
    cos_df: pd.DataFrame, value_col: str, pct_boundary_dict: dict, year: int
) -> pd.DataFrame:
    """Adds new columns to a dataframe that groups regional cost of steelmaking dataframe around whether the region's COS are above or below the average.

    Args:
        cos_df (pd.DataFrame): Cost of Steelmaking (COS) DataFrame
        value_col (str): The value column in the cost of steelmaking function
        pct_boundary (float): The percentage boundary to use based around the mean value to determine the overall column boundary

    Returns:
        pd.DataFrame: The COS DataFrame with new columns `relative_cost_below_avg` and `relative_cost_close_to_mean`
    """
    def apply_upper_boundary(row: pd.DataFrame, mean_value: float, value_range: float, pct_boundary_dict: dict):
        return mean_value + (value_range * (pct_boundary_dict[row['rmi_region']]))

    def close_to_mean_test(row: pd.DataFrame):
        return row['cost_of_steelmaking'] < row['upper_boundary']

    df_c = cos_df.copy()
    mean_val = df_c[value_col].mean()
    value_range = df_c[value_col].max() - df_c[value_col].min()
    df_c.reset_index(inplace=True)
    df_c['upper_boundary'] = df_c.apply(
        apply_upper_boundary, mean_value=mean_val, value_range=value_range, pct_boundary_dict=pct_boundary_dict, axis=1)
    df_c["relative_cost_below_avg"] = df_c[value_col].apply(lambda x: x <= mean_val)
    df_c["relative_cost_close_to_mean"] = df_c.apply(close_to_mean_test, axis=1)
    df_c["year"] = year
    return df_c.set_index('rmi_region')


def single_year_cos(
    plant_capacity: float,
    utilization_rate: float,
    variable_cost: float,
    other_opex_cost: float,
) -> float:
    """Applies the Cost of Steelmaking function to a given row in a DataFrame.

    Args:
        row (_type_): A vectorized DataFrame row from .apply function.
        year (int): The current year.
        v_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_costs (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        production_df (pd.DataFrame): A DataFrame containing the production values.
        steel_scenario (str): A string containing the scenario to be used in the steel.

    Returns:
        float: The cost of Steelmaking value to be applied.
    """
    return (
        0
        if utilization_rate == 0
        else (plant_capacity * utilization_rate * (variable_cost + other_opex_cost)) 
    )


def cos_value_generator(
    row: pd.Series,
    year: int,
    utilization_container: float,
    v_costs: pd.DataFrame,
    capacity_dict: dict,
    tech_choices: dict,
    capex_costs: dict,
) -> float:
    """Generates COS values per steel plant row.

    Args:
        row (pd.Series): A series containing metadata on a steel plant.
        year (int): The current model cycle year.
        utilization_container (UtilizationContainerClass): The open close metadata dictionary.
        v_costs (pd.DataFrame): Variable costs DataFrame.
        capacity_dict (dict): Dictionary of Capacity Values.
        tech_choices (dict): Technology choices dictionary.
        capex_costs (dict): Capex costs dictionary.

    Returns:
        float: The COS value for the plant
    """
    technology = tech_choices[year][row.plant_name]
    plant_capacity = capacity_dict[row.plant_name]
    utilization_rate = utilization_container.get_utilization_values(year, row.rmi_region)
    variable_cost = v_costs.loc[row.country_code, year, technology]["cost"]
    other_opex_cost = capex_costs["other_opex"].loc[technology, year]["value"]
    return single_year_cos(
        plant_capacity, utilization_rate, variable_cost, other_opex_cost
    )


def calculate_cos(
    plant_df: pd.DataFrame,
    year: int,
    utilization_container: UtilizationContainerClass,
    v_costs: pd.DataFrame,
    tech_choices: dict,
    capex_costs: dict,
    capacity_container: CapacityContainerClass,
) -> pd.DataFrame:
    """Calculates the COS as a column in a steel plant DataFrame.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        year (int): The current model cycle year.
        utilization_container (UtilizationContainerClass): The open close metadata dictionary.
        v_costs (pd.DataFrame): Variable costs DataFrame.
        tech_choices (dict): Technology choices dictionary.
        capex_costs (dict): Capex costs dictionary.
        capacity_dict (dict): Dictionary of Capacity Values.

    Returns:
        pd.DataFrame: A Dataframe grouped by region and sorted by the new cost_of_steelmaking function.
    """
    plant_df_c = plant_df.copy()
    capacity_dict = capacity_container.return_plant_capacity(year)
    reference_year = year if year == MODEL_YEAR_START else year - 1
    plant_df_c["cost_of_steelmaking"] = plant_df_c.apply(
        cos_value_generator,
        year=reference_year,
        utilization_container=utilization_container,
        v_costs=v_costs,
        capacity_dict=capacity_dict,
        tech_choices=tech_choices,
        capex_costs=capex_costs,
        axis=1,
    )
    regional_capacity_dict = capacity_container.return_regional_capacity(reference_year)
    regional_utilization_dict = utilization_container.get_utilization_values(reference_year)
    cos_df = (
        plant_df_c[[MAIN_REGIONAL_SCHEMA, "cost_of_steelmaking"]]
        .groupby([MAIN_REGIONAL_SCHEMA])
        .sum()
        .sort_values(by="cost_of_steelmaking", ascending=True)
        .reset_index()
        .copy()
    )
    def final_cos_adjustment(row):
        return row.cost_of_steelmaking / (regional_capacity_dict[row.rmi_region] * regional_utilization_dict[row.rmi_region])
    cos_df["cost_of_steelmaking"] = cos_df.apply(final_cos_adjustment, axis=1)
    return cos_df.set_index(MAIN_REGIONAL_SCHEMA)

def get_initial_utilization(utilization_container: UtilizationContainerClass, year: int, region: str):
    return utilization_container.get_utilization_values(year, region) if year == MODEL_YEAR_START else utilization_container.get_utilization_values(year - 1, region)


def modify_production_demand_dict(
    production_demand_dict: dict, data_entry_dict: dict, region: str
) -> dict:
    """Modifies the open close dictionary by taking an ordered list of `data_entry_values`, mapping them to their corresponding dictionary keys and amending the original demand dictionary values.

    Args:
        production_demand_dict (dict): The original open close dictionary reference.
        data_entry_dict (dict): The dict of values to update the production_demand_dict with.
        region (str): The selected region to update the `production_demand_dict` with the `data_entry_values`.

    Returns:
        dict: The modified open close dictionary.
    """
    production_demand_dict_c = deepcopy(production_demand_dict)

    for dict_key in data_entry_dict:
        production_demand_dict_c[region][dict_key] = data_entry_dict[dict_key]
    return production_demand_dict_c

def utilization_boundary(utilization_figure: float, util_min: float, util_max: float):
    return max(min(utilization_figure, util_max), util_min)

def concat_region_year(year: int, region: str):
    return f"{region.replace(' ', '').replace(',', '')}_{year}"

def merge_trade_status_col_to_rpc_df(
    rpc_df: pd.DataFrame, trade_status_container: dict, initial_overproduction_container: dict
):
    rpc_df.reset_index(inplace=True)
    rpc_df["initial_overproduction"] = rpc_df[MAIN_REGIONAL_SCHEMA].apply(lambda region: initial_overproduction_container[region])
    rpc_df["initial_trade_status"] = rpc_df[MAIN_REGIONAL_SCHEMA].apply(lambda region: trade_status_container[region])
    return rpc_df.set_index(MAIN_REGIONAL_SCHEMA)

def create_empty_market_dict(
    year: int, region: str, capacity: float, 
    demand: float, initial_utilization: float,
    initial_balance: float,
    avg_plant_capacity_value: float,
):
    return {
        "year": year,
        "region": region,
        "capacity": capacity,
        "initial_utilized_capacity": capacity * initial_utilization,
        "demand": demand,
        "initial_balance": initial_balance,
        "initial_utilization": initial_utilization,
        "avg_plant_capacity": avg_plant_capacity_value,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 0,
        "new_utilized_capacity": 0,
        "new_balance": 0,
        "new_utilization": 0,
        "unit": "Mt",
        "cases": []
    }

def test_market_dict_output(plant_change_dict: dict, util_min: float, util_max: float):
    assert round(plant_change_dict["new_total_capacity"], TRADE_ROUNDING_NUMBER) > 0, plant_change_dict
    assert round(plant_change_dict["new_utilized_capacity"], TRADE_ROUNDING_NUMBER) > 0, plant_change_dict
    assert util_min <= round(plant_change_dict["new_utilization"], TRADE_ROUNDING_NUMBER) <= util_max, plant_change_dict

    if plant_change_dict["new_capacity_required"] > 0:
        assert plant_change_dict["plants_required"] > 0, plant_change_dict

    if plant_change_dict["plants_to_close"] > 0:
        assert plant_change_dict["capacity"] > plant_change_dict["new_total_capacity"], plant_change_dict

def test_capacity_values(results_container: dict, capacity_dict: dict, cases: dict):
    for region in results_container:
        results_container_value = results_container[region]["new_total_capacity"]
        capacity_dict_value = capacity_dict[region]
        if round(results_container_value, TRADE_ROUNDING_NUMBER) != round(capacity_dict_value, TRADE_ROUNDING_NUMBER):
            raise AssertionError(f"Capacity Value Test | Region: {region} - container_result (capacity_dict_result): {results_container_value: .2f} ({capacity_dict_value: .2f}) Cases: {cases[region]}")

def test_production_equals_demand(global_production: float, global_demand: float):
    assert round(global_production, TRADE_ROUNDING_NUMBER) == round(global_demand, TRADE_ROUNDING_NUMBER), f"global_production: {global_production} | global_demand: {global_demand}"

def test_production_values(results_container: dict, market_container: MarketContainerClass, cases: dict, year: int):
    for region in results_container:
        dict_result = results_container[region]["new_utilized_capacity"]
        container_result = market_container.return_trade_balance(year, region, account_type="production")
        if round(dict_result, TRADE_ROUNDING_NUMBER) != round(container_result, TRADE_ROUNDING_NUMBER):
            raise AssertionError(f"Production Value Test | Year: {year} | Region: {region} | Dict Value (Container Value): {dict_result} ({container_result: .2f}) | Cases: {cases[region]}")

def test_utilization_values(utilization_container: UtilizationContainerClass, results_container: dict, year: int, util_min: float, util_max: float, cases: dict = None):
    util_container = utilization_container.get_utilization_values(year)
    overutilized_regions = [key for key in util_container if round(util_container[key], TRADE_ROUNDING_NUMBER) > util_max]
    underutilized_regions = [key for key in util_container if round(util_container[key], TRADE_ROUNDING_NUMBER) < util_min]
    cases = cases or {region: "" for region in util_container}
    if overutilized_regions:
        string_container = [f"{region}: {util_container[region]: .2f} - {cases[region]}" for region in overutilized_regions]
        raise AssertionError(f"Regional utilization rates: {util_container} | Regions Overutilized in {year}: {util_container} {join_list_as_string(string_container)}")
    if underutilized_regions:
        string_container = [f"{region}: {util_container[region]: .2f} - {cases[region]}" for region in underutilized_regions]
        raise AssertionError(f"Regional utilization rates: {util_container} | Regions Underutilized in {year}: {join_list_as_string(string_container)}")

def test_open_close_plants(results_container: dict, cases: dict):
    incorrect_open_plants = [region for region in results_container if results_container[region]["plants_required"] < 0]
    incorrect_close_plants = [region for region in results_container if results_container[region]["plants_to_close"] < 0]
    if incorrect_open_plants:
        string_container = [f"{region}: {results_container[region]['plants_required']} - {cases[region]}" for region in incorrect_open_plants]
        raise AssertionError(f"Incorrect number of plants_required {join_list_as_string(string_container)}")
    if incorrect_close_plants:
        string_container = [f"{region}: {results_container[region]['plants_to_close']} - {cases[region]}" for region in incorrect_close_plants]
        raise AssertionError(f"Incorrect number of plants_to_close in: {join_list_as_string(string_container)}")

def print_demand_production_balance(market_container: MarketContainerClass, demand_dict: dict, year):
    for region, demand in demand_dict.items():
        production = market_container.return_trade_balance(year, region, "production")
        print(f"region: {region} | demand: {demand} | production: {production} | trade_balance: {production - demand}")

def test_regional_production(results_container: dict, rpc_df: pd.DataFrame, cases: dict):
    for region in results_container:
        summary_dict = results_container[region]
        assert round(summary_dict["new_utilized_capacity"], TRADE_ROUNDING_NUMBER) > 0, f"Production > 0 test failed for {region} -> cases: {cases[region]} -> summary dictionary: {summary_dict}, rpc: {rpc_df.loc[region]}"
