"""Module that contains the trade functions"""

import math
from copy import deepcopy

import pandas as pd

from mppsteel.config.model_config import (
    MAIN_REGIONAL_SCHEMA,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    TRADE_PCT_BOUNDARY_DICT,
    MODEL_YEAR_END,
    MODEL_YEAR_START,
)
from mppsteel.model_solver.solver_classes import (
    CapacityContainerClass,
    UtilizationContainerClass,
    MarketContainerClass,
)
from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.utils import join_list_as_string

logger = get_logger(__name__)



DATA_ENTRY_DICT_KEYS = [
    "new_capacity_required",
    "plants_required",
    "plants_to_close",
    "new_total_capacity",
    "new_utilized_capacity",
    "new_balance",
    "new_utilization",
]


def check_relative_production_cost(
    cos_df: pd.DataFrame, value_col: str, pct_boundary_dict: dict
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
        return mean_value + (value_range * pct_boundary_dict[row['rmi_region']])

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
        else (plant_capacity * utilization_rate * variable_cost) + other_opex_cost
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
    technology = (
        tech_choices[year][row.plant_name]
        if year == MODEL_YEAR_START
        else tech_choices[year - 1][row.plant_name]
    )
    plant_capacity = capacity_dict[row.plant_name]
    reference_year = year if year == 2020 else year - 1
    utilization_rate = utilization_container.get_utilization_values(reference_year, row.rmi_region)
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
    capacity_dict: dict,
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
    plant_df_c["cost_of_steelmaking"] = plant_df_c.apply(
        cos_value_generator,
        year=year,
        utilization_container=utilization_container,
        v_costs=v_costs,
        capacity_dict=capacity_dict,
        tech_choices=tech_choices,
        capex_costs=capex_costs,
        axis=1,
    )
    return (
        plant_df_c[[MAIN_REGIONAL_SCHEMA, "cost_of_steelmaking"]]
        .groupby([MAIN_REGIONAL_SCHEMA])
        .mean()
        .sort_values(by="cost_of_steelmaking", ascending=True)
        .copy()
    )

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


def trade_flow(
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: CapacityContainerClass,
    steel_demand_df: pd.DataFrame,
    variable_cost_df: pd.DataFrame,
    plant_df: pd.DataFrame,
    capex_dict: dict,
    tech_choices_ref: dict,
    year: int,
    util_min: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    util_max: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
) -> dict:
    """Modifies an open close dictionary of metadata for each region. The following optimization steps are taken by the algorithm.
    1) Determine whether a plant can meet its current regional demand with its current utilization levels.
    2) Optimize the utilization levels accordingly if possible
    3) Engage in interregional trade until there is no imbalance remaining.

    Args:
        material_container (MaterialUsage): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): The UtilizationContainerClass Instance containing the utilization state.
        capacity_container (CapacityContainerClass): The CapacityContainerClass Instance containing the capacity state.
        steel_demand_df (pd.DataFrame): The steel demand DataFrame.
        variable_costs_df (pd.DataFrame): The variable costs reference DataFrame.
        plant_df (pd.DataFrame): The steel plant DataFrame.
        capex_dict (dict): The capex reference dictionary.
        tech_choices_container (PlantChoices): The PlantChoices Instance containing the Technology Choices state.
        year (int): The current model year.
        util_min (float, optional): The minimum capacity utilization that plants are allowed to reach before having to close existing plants. Defaults to CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION.
        util_max (float, optional): The maximum capacity utilization that plants are allowed to reach before having to open new plants. Defaults to CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION.

    Returns:
        dict: A dictionary of open close metadata for each region.
    """
    capacity_dict = capacity_container.return_plant_capacity(year)
    cos_df = calculate_cos(
        plant_df,
        year,
        utilization_container,
        variable_cost_df,
        tech_choices_ref,
        capex_dict,
        capacity_dict,
    )
    relative_production_cost_df = check_relative_production_cost(
        cos_df, "cost_of_steelmaking", TRADE_PCT_BOUNDARY_DICT
    )

    region_list = list(plant_df[MAIN_REGIONAL_SCHEMA].unique())

    results_container = {}
    regional_capacity_dict = {}
    plant_change_dict = {}
    cases = {}

    avg_plant_capacity_value = capacity_container.return_avg_capacity_value()

    for region in region_list:
        plant_change_dict[region] = {
            "plants_required": 0,
            "plants_to_close": 0
        }
        cases[region] = []
        capacity = capacity_container.return_regional_capacity(year, region)
        demand = steel_demand_getter(
            steel_demand_df, year=year, metric="crude", region=region
        )
        initial_utilization = get_initial_utilization(utilization_container, year, region)
        utilization_cutoff = utilization_boundary(
            initial_utilization, util_min, util_max)
        regional_balance = (capacity * utilization_cutoff) - demand
        utilization = utilization_container.get_utilization_values(year, region)
        avg_plant_capacity = capacity_container.return_avg_capacity_value()
        avg_plant_capacity_at_max_production = avg_plant_capacity * util_max
        relative_cost_below_avg = relative_production_cost_df.loc[region][
            "relative_cost_below_avg"
        ]
        relative_cost_below_mean = relative_production_cost_df.loc[region][
            "relative_cost_close_to_mean"
        ]
        if regional_balance == 0:
            case_type = "BALANCED -> do nothing"
            market_tuple = market_container.return_market_entry(demand, 0, 0) # nothing
            regional_capacity_dict[region] = capacity
        elif (regional_balance > 0) and relative_cost_below_avg:
            case_type = "CHEAP EXCESS SUPPLY -> export"
            new_min_utilization_required = utilization_boundary(
                utilization, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * capacity
            export_contribution = new_utilized_capacity - demand
            plant_change_dict[region]["plants_required"] = 0
            plant_change_dict[region]["plants_to_close"] = 0
            market_tuple = market_container.return_market_entry(demand, 0, export_contribution) # should export
            regional_capacity_dict[region] = capacity
        elif (
            (regional_balance > 0)
            and not relative_cost_below_avg
            and (utilization > util_min) # could lead to excess demand
        ):
            case_type = "EXPENSIVE EXCESS SUPPLY -> reduce utilization if possible"
            new_min_utilization_required = demand / capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = capacity * new_min_utilization_required
            export_contribution = new_utilized_capacity - demand
            plant_change_dict[region]["plants_required"] = 0
            plant_change_dict[region]["plants_to_close"] = 0
            market_tuple = market_container.return_market_entry(demand, 0, export_contribution) # export if necessary
            regional_capacity_dict[region] = capacity
        elif (
            (regional_balance > 0)
            and not relative_cost_below_avg
            and (utilization <= util_min)
        ):
            case_type = "EXPENSIVE EXCESS SUPPLY -> close plant"
            excess_capacity = (capacity * util_min) - demand
            plants_to_close = math.ceil(
                excess_capacity / avg_plant_capacity
            )
            new_total_capacity = capacity - (plants_to_close * avg_plant_capacity)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * new_total_capacity
            export_contribution = new_utilized_capacity - demand
            plant_change_dict[region]["plants_required"] = 0
            plant_change_dict[region]["plants_to_close"] = plants_to_close
            market_tuple = market_container.return_market_entry(demand, 0, export_contribution) # export if necessary
            regional_capacity_dict[region] = new_total_capacity
        elif (regional_balance < 0) and (utilization < util_max):
            case_type = "INSUFFICIENT SUPPLY -> increase utilization (test)"
            new_min_utilization_required = demand / capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * capacity
            imports = demand - new_utilized_capacity
            if (imports > 0) and not relative_cost_below_avg:
                # STILL INSUFFICIENT SUPPLY
                # EXPENSIVE REGION -> import
                plant_change_dict[region]["plants_required"] = 0
                plant_change_dict[region]["plants_to_close"] = 0
                market_tuple = market_container.return_market_entry(demand - imports, imports, 0) # should import
                regional_capacity_dict[region] = capacity
            elif (imports > 0) and relative_cost_below_avg:
                case_type = "CHEAP REGION -> open plant"
                new_capacity_required = demand - (capacity * util_max)
                new_plants_required = math.ceil(
                    new_capacity_required / avg_plant_capacity_at_max_production
                )
                new_total_capacity = capacity + (
                    new_plants_required * avg_plant_capacity
                )
                new_min_utilization_required = demand / new_total_capacity
                new_min_utilization_required = utilization_boundary(
                    new_min_utilization_required, util_min, util_max
                )
                new_utilized_capacity = (
                    new_min_utilization_required * new_total_capacity
                )
                export_contribution = new_utilized_capacity - demand
                plant_change_dict[region]["plants_required"] = new_plants_required
                plant_change_dict[region]["plants_to_close"] = 0
                market_tuple = market_container.return_market_entry(demand, 0, export_contribution) # export if necessary
                regional_capacity_dict[region] = new_total_capacity
            else:
                case_type = "SUFFICIENT SUPPLY -> increase utilization"
                plant_change_dict[region]["plants_required"] = 0
                plant_change_dict[region]["plants_to_close"] = 0
                market_tuple = market_container.return_market_entry(demand, 0, 0) # should be balanced
                regional_capacity_dict[region] = capacity
        elif (
            (regional_balance < 0)
            and (utilization >= util_max)
            and relative_cost_below_mean
        ):
            case_type = "INSUFFICIENT SUPPLY, CHEAP REGION, MAX UTILIZATION -> open plants"
            new_capacity_required = demand - (capacity * util_max)
            new_plants_required = math.ceil(
                new_capacity_required / avg_plant_capacity_at_max_production
            )
            new_total_capacity = capacity + (new_plants_required * avg_plant_capacity)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * new_total_capacity
            export_contribution = new_utilized_capacity - demand
            plant_change_dict[region]["plants_required"] = new_plants_required
            plant_change_dict[region]["plants_to_close"] = 0
            market_tuple = market_container.return_market_entry(demand, 0, export_contribution) # export if necessary
            regional_capacity_dict[region] = new_total_capacity
        elif (
            (regional_balance < 0)
            and (utilization >= util_max)
            and not relative_cost_below_mean
        ):
            case_type = "INSUFFICIENT SUPPLY, EXPENSIVE REGION, MAX UTILIZATION -> import"
            new_min_utilization_required = utilization_boundary(
                utilization, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * capacity
            imports = demand - new_utilized_capacity
            plant_change_dict[region]["plants_required"] = 0
            plant_change_dict[region]["plants_to_close"] = 0
            market_tuple = market_container.return_market_entry(demand - imports, imports, 0) # should import
            regional_capacity_dict[region] = capacity

        market_container.assign_market_tuple(year, region, market_tuple)
        utilization_container.update_region(year, region, new_min_utilization_required)
        new_capacity = regional_capacity_dict[region]
        cases[region].append(f"Initial: {case_type}")

        region_result = {
            "year": year,
            "region": region,
            "capacity": capacity,
            "initial_utilized_capacity": capacity * initial_utilization,
            "demand": demand,
            "initial_balance": regional_balance,
            "initial_utilization": initial_utilization,
            "avg_plant_capacity": avg_plant_capacity_value,
            "new_capacity_required": new_capacity - capacity,
            "plants_required": plant_change_dict[region]["plants_required"],
            "plants_to_close": plant_change_dict[region]["plants_to_close"],
            "new_total_capacity": new_capacity,
            "new_utilized_capacity": market_container.return_trade_balance(year, region, "production"),
            "new_balance": market_container.return_trade_balance(year, region, "trade"),
            "new_utilization": utilization_container.get_utilization_values(year, region),
            "unit": "Mt",
        }
        results_container[region] = region_result
        total_production_container = market_container.return_trade_balance(year, region, "production")
        total_production_dict = region_result["new_utilized_capacity"]

        assert total_production_dict == total_production_container, f"regional production test - Initial Round: {region} dict {total_production_dict} | container {total_production_container}"

    global_trade_balance = market_container.trade_container_aggregator(year, "trade")
    importing_regions = market_container.list_regional_types(year, "imports")
    exporting_regions = market_container.list_regional_types(year, "exports")
    balanced_regions = market_container.check_if_trade_balance(year)

    logger.info(
        f"TRADE BALANCING ROUND 1: Importing Regions: {join_list_as_string(importing_regions)} | Exporting Regions: {join_list_as_string(exporting_regions)} | Balanced Regions: {join_list_as_string(balanced_regions)}"
    )

    if round(global_trade_balance, 5) == 0:
        logger.info(
            f"Trade Balance is completely balanced at {global_trade_balance: 4f} Mt in year {year}"
        )
        pass

    elif round(global_trade_balance, 5) > 0:
        logger.info(
            f"TRADE BALANCING ROUND 1: Trade Balance Surplus of {global_trade_balance} Mt in year {year}. Balancing to zero."
        )
        for region in exporting_regions:
            if global_trade_balance > 0:
                current_utilization = utilization_container.get_utilization_values(year, region)
                capacity = regional_capacity_dict[region]
                if current_utilization > util_min:
                    case_type = "R1: Reducing excess capacity via lowering utilization"
                    current_balance = market_container.trade_container_aggregator(year, "trade", region)
                    value_to_subtract_from_global = min(current_balance, global_trade_balance)
                    max_removable_value = (capacity * util_max) - demand
                    value_to_subtract_from_global = min(value_to_subtract_from_global, max_removable_value)
                    market_tuple = market_container.return_market_entry(0, 0, -value_to_subtract_from_global)
                    market_container.assign_market_tuple(year, region, market_tuple)
                    new_balance = market_container.trade_container_aggregator(year, "trade", region)
                    total_production = market_container.trade_container_aggregator(year, "production", region)
                    new_utilization = total_production / capacity
                    regional_capacity_dict[region] = capacity
                    data_entry_dict = {
                        "new_balance": new_balance,
                        "new_utilized_capacity": capacity * new_utilization,
                        "new_utilization": new_utilization
                    }
                    results_container = modify_production_demand_dict(
                        results_container, data_entry_dict, region
                    )
                    utilization_container.update_region(
                            year, region, new_utilization
                    )
                    global_trade_balance -= value_to_subtract_from_global
                    cases[region].append(case_type)

    elif round(global_trade_balance, 5) < 0:
        logger.info(
            f"TRADE BALANCING ROUND 2: Trade Balance Deficit of {global_trade_balance} Mt in year {year}, balancing to zero via utilization optimization."
        )
        rpc_df = relative_production_cost_df[
            relative_production_cost_df["relative_cost_close_to_mean"] == True
        ].sort_values(["cost_of_steelmaking"], ascending=True)
        for region in rpc_df.index:
            # increase utilization
            current_utilization = utilization_container.get_utilization_values(year, region)
            total_capacity = regional_capacity_dict[region]
            current_utilized_capacity = market_container.trade_container_aggregator(year, "production", region)
            potential_extra_production = (total_capacity * util_max) - current_utilized_capacity
            if potential_extra_production <= 0:
                pass
            elif potential_extra_production >= abs(global_trade_balance):
                logger.info(
                    f"TRADE BALANCING ROUND 2 - A: {region} can supply all of the import demand."
                )
                case_type = "R2-A: moving all import demand to region"
                value_to_add_to_global = abs(global_trade_balance)
                market_tuple = market_container.return_market_entry(0, 0, value_to_add_to_global)
                market_container.assign_market_tuple(year, region, market_tuple)
                new_utilized_capacity = (
                    current_utilized_capacity + value_to_add_to_global
                )
                new_min_utilization_required = (
                    new_utilized_capacity / total_capacity
                )
                new_min_utilization_required = utilization_boundary(
                    new_min_utilization_required, util_min, util_max
                )
                new_balance = market_container.trade_container_aggregator(year, "trade", region)
                data_entry_dict = {
                    "new_utilized_capacity": new_utilized_capacity,
                    "new_balance": new_balance,
                    "new_utilization": new_min_utilization_required
                }
                results_container = modify_production_demand_dict(
                    results_container, data_entry_dict, region
                )
                utilization_container.update_region(
                    year, region, new_min_utilization_required
                )
                regional_capacity_dict[region] = total_capacity
                global_trade_balance = 0
                cases[region].append(case_type)
            elif potential_extra_production < abs(global_trade_balance):
                logger.info(
                    f"TRADE BALANCING ROUND 2 - B: {region} can supply {potential_extra_production :0.2f} of the import demand of {global_trade_balance :0.2f}."
                )
                case_type = "R2-B: moving partial import demand to region"
                market_tuple = market_container.return_market_entry(0, 0, potential_extra_production)
                market_container.assign_market_tuple(year, region, market_tuple)
                total_production = market_container.trade_container_aggregator(year, "production", region)
                new_min_utilization_required = total_production / total_capacity
                trade_balance = market_container.trade_container_aggregator(year, "trade", region)
                data_entry_dict = {
                    "new_utilized_capacity": total_production,
                    "new_balance": trade_balance,
                    "new_utilization": new_min_utilization_required,
                }
                results_container = modify_production_demand_dict(
                    results_container, data_entry_dict, region
                )
                utilization_container.update_region(
                    year, region, new_min_utilization_required
                )
                regional_capacity_dict[region] = total_capacity
                global_trade_balance += potential_extra_production
                cases[region].append(case_type)
            

        # ROUND 2: Open new plants in cheapest region
        if global_trade_balance < 0:
            cheapest_region = rpc_df["cost_of_steelmaking"].idxmin()
            logger.info(
                f"TRADE BALANCING ROUND 3: Assigning trade balance of {global_trade_balance :0.2f} Mt to cheapest region: {cheapest_region}"
            )
            case_type = "R3: moving remaining import demand to cheapest region"
            current_utilization = utilization_container.get_utilization_values(year, region)
            total_capacity = regional_capacity_dict[cheapest_region]
            value_to_add_to_global = abs(global_trade_balance)
            market_tuple = market_container.return_market_entry(0, 0, value_to_add_to_global)
            market_container.assign_market_tuple(year, cheapest_region, market_tuple)
            total_production = market_container.trade_container_aggregator(year, "production", cheapest_region)
            new_balance = market_container.trade_container_aggregator(
                year, "trade", cheapest_region
            )
            new_capacity_required = total_production - (total_capacity * util_max)
            new_plants_required = math.ceil(
                new_capacity_required / avg_plant_capacity_at_max_production
            )
            plants_to_close = 0
            if new_plants_required < 0:
                plants_to_close = -deepcopy(new_plants_required)
                new_plants_required = 0
            new_total_capacity = total_capacity + (new_plants_required * avg_plant_capacity)
            regional_capacity_dict[cheapest_region] = new_total_capacity
            new_min_utilization_required = total_production / new_total_capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_total_capacity * new_min_utilization_required
            data_entry_dict = {
                "new_capacity_required": new_capacity_required,
                "plants_required": new_plants_required,
                "plants_to_close": plants_to_close,
                "new_total_capacity": new_total_capacity,
                "new_utilized_capacity": new_utilized_capacity,
                "new_balance": new_balance,
                "new_utilization": new_min_utilization_required
            }
            utilization_container.update_region(
                year, cheapest_region, new_min_utilization_required
            )
            results_container = modify_production_demand_dict(
                results_container, data_entry_dict, cheapest_region
            )
            global_trade_balance = 0
            cases[cheapest_region].append(case_type)

    # final trade balance
    global_trade_balance = market_container.trade_container_aggregator(year, "trade")

    if round(global_trade_balance, 5) == 0:
        logger.info(
            f"Trade Balance is completely balanced at {global_trade_balance: 4f} Mt in year {year}"
        )
        pass

    elif round(global_trade_balance, 5) > 0:
        logger.info(
            f"TRADE BALANCING ROUND 4-A: Reducing excess trade balance of {global_trade_balance :0.2f} via lower utilization"
        )
        exporting_regions = market_container.list_regional_types(year, "exports")
        for region in exporting_regions:
            demand = steel_demand_getter(
                steel_demand_df, year=year, metric="crude", region=region
            )
            region_trade_balance = market_container.trade_container_aggregator(year, "trade", region)
            current_utilization = utilization_container.get_utilization_values(year, region)
            capacity = regional_capacity_dict[region]
            if current_utilization > util_min:
                case_type = "R4-A: EXCESS SUPPLY -> lower utilization"
                value_to_subtract_from_global = min(region_trade_balance, global_trade_balance)
                max_removable_value = (capacity * util_max) - demand
                value_to_subtract_from_global = min(value_to_subtract_from_global, max_removable_value)
                market_tuple = market_container.return_market_entry(0, 0, -value_to_subtract_from_global)
                market_container.assign_market_tuple(year, region, market_tuple)
                new_balance = market_container.trade_container_aggregator(year, "trade", region)
                total_production = market_container.trade_container_aggregator(year, "production", region)
                new_min_utilization_required = total_production / capacity
                new_min_utilization_required = utilization_boundary(
                    new_min_utilization_required, util_min, util_max
                )
                regional_capacity_dict[region] = capacity
                data_entry_dict = {
                    "new_balance": new_balance,
                    "new_utilized_capacity": total_production,
                    "new_utilization": new_min_utilization_required
                }
                results_container = modify_production_demand_dict(
                    results_container, data_entry_dict, region
                )
                utilization_container.update_region(
                        year, region, new_min_utilization_required
                )
                global_trade_balance -= value_to_subtract_from_global
                cases[region].append(case_type)

    # final trade balance
    global_trade_balance = market_container.trade_container_aggregator(year, "trade")

    if round(global_trade_balance, 5) == 0:
        logger.info(
            f"Trade Balance is completely balanced at {global_trade_balance: 4f} Mt in year {year}"
        )
        pass

    elif global_trade_balance > 0:
        logger.info(
            f"TRADE BALANCING ROUND 4-B: Reducing excess trade balance of {global_trade_balance :0.2f} via closing plants"
        )
        exporting_regions = market_container.list_regional_types(year, "exports")
        for region in exporting_regions:
            demand = steel_demand_getter(
                steel_demand_df, year=year, metric="crude", region=region
            )
            region_trade_balance = market_container.trade_container_aggregator(year, "trade", region)
            current_utilization = utilization_container.get_utilization_values(year, region)
            capacity = regional_capacity_dict[region]
            if current_utilization <= util_min:
                case_type = "R4-B: EXCESS SUPPLY, MIN UTILZATION -> close plants"
                trade_prodution_to_close = min(region_trade_balance, global_trade_balance)
                market_tuple = market_container.return_market_entry(
                    0, 0, -trade_prodution_to_close
                ) # export if necessary
                market_container.assign_market_tuple(year, region, market_tuple)
                total_required_production = market_container.return_trade_balance(year, region, "production")
                total_required_capacity = total_required_production / util_min
                excess_capacity = capacity - total_required_capacity
                plants_to_close = math.ceil(
                    excess_capacity / avg_plant_capacity
                )
                new_total_capacity = capacity - (plants_to_close * avg_plant_capacity)
                regional_capacity_dict[region] = new_total_capacity
                new_min_utilization_required = total_required_production / new_total_capacity
                new_utilized_capacity = new_min_utilization_required * new_total_capacity
                export_contribution = new_utilized_capacity - demand
                plants_to_open = 0
                if plants_to_close < 0:
                    plants_to_open = -deepcopy(plants_to_close)
                    plants_to_close = 0

                data_entry_dict = {
                    "new_capacity_required": 0,
                    "plants_required": plants_to_open,
                    "plants_to_close": plants_to_close,
                    "new_total_capacity": new_total_capacity,
                    "new_utilized_capacity": new_utilized_capacity,
                    "new_balance": export_contribution,
                    "new_utilization": new_min_utilization_required,
                }
                utilization_container.update_region(
                    year, region, new_min_utilization_required
                )
                results_container = modify_production_demand_dict(
                    results_container, data_entry_dict, region
                )
                global_trade_balance -= trade_prodution_to_close
                cases[region].append(case_type)

    global_capacity = sum(regional_capacity_dict.values())
    global_production = market_container.trade_container_aggregator(year, "production")
    global_demand = steel_demand_getter(
        steel_demand_df, year=year, metric="crude", region="World"
    )
    utilization_values = utilization_container.get_utilization_values(year)
    utilization_values = {region: round(utilization_values[region], 2) for region in utilization_values}

    test_open_close_plants(results_container, cases)
    test_production_values(results_container, market_container, cases, year)
    test_capacity_values(results_container, regional_capacity_dict, cases)
    test_production_equals_demand(global_demand, global_production)
    test_utilization_values(utilization_container, year, util_min, util_max, cases)

    logger.info(f"Final Trade Balance is {global_trade_balance} Mt in year {year}")
    return results_container

def test_capacity_values(results_container: dict, capacity_dict: dict, cases: dict):
    for region in results_container:
        results_container_value = results_container[region]["new_total_capacity"]
        capacity_dict_value = capacity_dict[region]
        if round(results_container_value, 2) != round(capacity_dict_value, 2):
            raise AssertionError(f"Capacity Value Test | Region: {region} - container_result (capacity_dict_result): {results_container_value} ({capacity_dict_value}) Cases: {cases[region]}")

def test_production_equals_demand(global_production: float, global_demand: float):
    assert round(global_production, 2) == round(global_demand, 2)


def test_production_values(results_container: dict, market_container: MarketContainerClass, cases: dict, year: int):
    for region in results_container:
        dict_result = results_container[region]["new_utilized_capacity"]
        container_result = market_container.return_trade_balance(year, region, account_type="production")
        if round(dict_result, 2) != round(container_result, 2):
            raise AssertionError(f"Production Value Test | Year: {year} - Region: {region} - Dict Value (Container Value): {dict_result} ({container_result}) Cases: {cases[region]}")

def test_utilization_values(utilization_container: UtilizationContainerClass, year, util_min: float, util_max: float, cases: dict = None):
    container = utilization_container.get_utilization_values(year)
    overcapacity_regions = [key for key in container if round(container[key], 5) > util_max]
    undercapacity_regions = [key for key in container if round(container[key], 5) < util_min]
    cases = cases if cases else {region: "" for region in container}
    if overcapacity_regions:
        string_container = [f"{region}: {container[region]: 2f} - {cases[region]}" for region in overcapacity_regions]
        raise AssertionError(f"Regions Over Capacity in {year}: {join_list_as_string(string_container)}")
    if undercapacity_regions:
        string_container = [f"{region}: {container[region]: 2f} - {cases[region]}" for region in undercapacity_regions]
        raise AssertionError(f"Regions Under Capacity in {year}: {join_list_as_string(string_container)}")

def test_open_close_plants(results_container: dict, cases: dict):
    incorrect_open_plants = [region for region in results_container if results_container[region]["plants_required"] < 0]
    incorrect_close_plants = [region for region in results_container if results_container[region]["plants_to_close"] < 0]
    if incorrect_open_plants:
        string_container = [f"{region}: {results_container[region]['plants_required']} - {cases[region]}" for region in incorrect_open_plants]
        raise AssertionError(f"Incorrect number of plants_required {join_list_as_string(string_container)}")
    if incorrect_close_plants:
        string_container = [f"{region}: {results_container[region]['plants_to_close']} - {cases[region]}" for region in incorrect_close_plants]
        raise AssertionError(f"Incorrect number of plants_to_close in: {join_list_as_string(string_container)}")
