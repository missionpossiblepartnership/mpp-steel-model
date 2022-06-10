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
    production_demand_dict: dict,
    v_costs: pd.DataFrame,
    capacity_dict: dict,
    tech_choices: dict,
    capex_costs: dict,
) -> float:
    """Generates COS values per steel plant row.

    Args:
        row (pd.Series): A series containing metadata on a steel plant.
        year (int): The current model cycle year.
        production_demand_dict (dict): The open close metadata dictionary.
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
    utilization_rate = production_demand_dict[row.rmi_region]["initial_utilization"]
    variable_cost = v_costs.loc[row.country_code, year, technology]["cost"]
    other_opex_cost = capex_costs["other_opex"].loc[technology, year]["value"]
    return single_year_cos(
        plant_capacity, utilization_rate, variable_cost, other_opex_cost
    )


def calculate_cos(
    plant_df: pd.DataFrame,
    year: int,
    production_demand_dict: dict,
    v_costs: pd.DataFrame,
    tech_choices: dict,
    capex_costs: dict,
    capacity_dict: dict,
) -> pd.DataFrame:
    """Calculates the COS as a column in a steel plant DataFrame.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        year (int): The current model cycle year.
        production_demand_dict (dict): The open close metadata dictionary.
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
        production_demand_dict=production_demand_dict,
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


def trade_flow(
    market_container: MarketContainerClass,
    production_demand_dict: dict,
    utilization_container: UtilizationContainerClass,
    capacity_container: CapacityContainerClass,
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
        production_demand_dict (dict): The original open close metadata dictionary.
        utilization_container (UtilizationContainerClass): The UtilizationContainerClass Instance containing the utilization state.
        capacity_container (CapacityContainerClass): The CapacityContainerClass Instance containing the capacity state.
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
    production_demand_dict_c = deepcopy(production_demand_dict)
    cos_df = calculate_cos(
        plant_df,
        year,
        production_demand_dict_c,
        variable_cost_df,
        tech_choices_ref,
        capex_dict,
        capacity_dict,
    )
    relative_production_cost_df = check_relative_production_cost(
        cos_df, "cost_of_steelmaking", TRADE_PCT_BOUNDARY_DICT
    )

    region_list = list(plant_df[MAIN_REGIONAL_SCHEMA].unique())

    for region in region_list:
        regional_balance = production_demand_dict_c[region]["initial_balance"]
        utilization = production_demand_dict_c[region]["initial_utilization"]
        demand = production_demand_dict_c[region]["demand"]
        capacity = production_demand_dict_c[region]["capacity"]
        avg_plant_capacity = production_demand_dict_c[region]["avg_plant_capacity"]
        avg_plant_capacity_at_max_production = avg_plant_capacity * util_max
        relative_cost_below_avg = relative_production_cost_df.loc[region][
            "relative_cost_below_avg"
        ]
        relative_cost_below_mean = relative_production_cost_df.loc[region][
            "relative_cost_close_to_mean"
        ]

        # COL ORDER FOR LIST
        # 'new_capacity_required', 'plants_required', 'plants_to_close',
        # 'new_total_capacity', 'new_utilized_capacity', 'new_balance', 'new_utilization'

        export_contribution = 0
        data_entry_dict = {}
        if (regional_balance > 0) and relative_cost_below_avg:
            # CHEAP EXCESS SUPPLY -> export
            new_min_utilization_required = utilization
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * capacity
            export_contribution = new_utilized_capacity - demand
            data_entry_dict = {
                "new_capacity_required": 0,
                "plants_required": 0,
                "plants_to_close": 0,
                "new_total_capacity": capacity,
                "new_utilized_capacity": new_utilized_capacity,
                "new_balance": export_contribution,
                "new_utilization": new_min_utilization_required,
            }

        elif (
            (regional_balance > 0)
            and not relative_cost_below_avg
            and (utilization > util_min)
        ):
            # EXPENSIVE EXCESS SUPPLY -> reduce utilization if possible
            new_min_utilization_required = demand / capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = capacity * new_min_utilization_required
            export_contribution = new_utilized_capacity - demand
            data_entry_dict = {
                "new_capacity_required": 0,
                "plants_required": 0,
                "plants_to_close": 0,
                "new_total_capacity": capacity,
                "new_utilized_capacity": new_utilized_capacity,
                "new_balance": export_contribution,
                "new_utilization": new_min_utilization_required
            }

        elif (
            (regional_balance > 0)
            and not relative_cost_below_avg
            and (utilization <= util_min)
        ):
            # EXPENSIVE EXCESS SUPPLY -> close plant
            excess_capacity = (capacity * util_min) - demand
            plants_to_close = math.ceil(
                excess_capacity / avg_plant_capacity_at_max_production
            )
            new_total_capacity = capacity - (plants_to_close * avg_plant_capacity)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * new_total_capacity
            export_contribution = new_utilized_capacity - demand
            data_entry_dict = {
                "new_capacity_required": 0,
                "plants_required": 0,
                "plants_to_close": plants_to_close,
                "new_total_capacity": new_total_capacity,
                "new_utilized_capacity": new_utilized_capacity,
                "new_balance": export_contribution,
                "new_utilization": new_min_utilization_required,
            }

        elif (regional_balance < 0) and (utilization < util_max):
            # INSUFFICIENT SUPPLY -> increase utilization (test)
            new_min_utilization_required = demand / capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * capacity
            export_contribution = new_utilized_capacity - demand
            if (export_contribution < 0) and not relative_cost_below_avg:
                # STILL INSUFFICIENT SUPPLY
                # EXPENSIVE REGION -> import
                data_entry_dict = {
                    "new_capacity_required": 0,
                    "plants_required": 0,
                    "plants_to_close": 0,
                    "new_total_capacity": capacity,
                    "new_utilized_capacity": new_utilized_capacity,
                    "new_balance": export_contribution,
                    "new_utilization": new_min_utilization_required
                }
            elif (export_contribution < 0) and relative_cost_below_avg:
                # CHEAP REGION -> open plant
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
                data_entry_dict = {
                    "new_capacity_required": new_capacity_required,
                    "plants_required": new_plants_required,
                    "plants_to_close": 0,
                    "new_total_capacity": new_total_capacity,
                    "new_utilized_capacity": new_utilized_capacity,
                    "new_balance": export_contribution,
                    "new_utilization": new_min_utilization_required
                }

            else:
                # SUFFICIENT SUPPLY -> increase utilization
                data_entry_dict = {
                    "new_capacity_required": 0,
                    "plants_required": 0,
                    "plants_to_close": 0,
                    "new_total_capacity": capacity,
                    "new_utilized_capacity": new_utilized_capacity,
                    "new_balance": export_contribution,
                    "new_utilization": new_min_utilization_required
                }

        elif (
            (regional_balance < 0)
            and (utilization >= util_max)
            and relative_cost_below_mean
        ):
            # INSUFFICIENT SUPPLY, CHEAP REGION, MAX UTILIZATION -> open plants
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
            data_entry_dict = {
                "new_capacity_required": new_capacity_required,
                "plants_required": new_plants_required,
                "plants_to_close": 0,
                "new_total_capacity": new_total_capacity,
                "new_utilized_capacity": new_utilized_capacity,
                "new_balance": export_contribution,
                "new_utilization": new_min_utilization_required
            }

        elif (
            (regional_balance < 0)
            and (utilization >= util_max)
            and not relative_cost_below_mean
        ):
            # INSUFFICIENT SUPPLY, EXPENSIVE REGION, MAX UTILIZATION -> import
            new_min_utilization_required = utilization
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * capacity
            export_contribution = new_utilized_capacity - demand
            data_entry_dict = {
                "new_capacity_required": 0,
                "plants_required": 0,
                "plants_to_close": 0,
                "new_total_capacity": capacity,
                "new_utilized_capacity": new_utilized_capacity,
                "new_balance": export_contribution,
                "new_utilization": new_min_utilization_required
            }

        market_container.assign_trade_balance(year, region, export_contribution)
        utilization_container.update_region(year, region, new_min_utilization_required)
        production_demand_dict_c = modify_production_demand_dict(
            production_demand_dict_c, data_entry_dict, region
        )

    global_trade_balance = round(market_container.trade_container_getter(year), 3)
    global_trade_balance_regions = market_container.trade_container_getter(
        year, agg=True
    )
    importing_regions = [
        region
        for region in global_trade_balance_regions
        if global_trade_balance_regions[region] < 0
    ]
    exporting_regions = [
        region
        for region in global_trade_balance_regions
        if global_trade_balance_regions[region] > 0
    ]
    balanced_regions = [
        region
        for region in global_trade_balance_regions
        if global_trade_balance_regions[region] == 0
    ]

    logger.info(
        f"TRADE BALANCING ROUND 1: Importing Regions: {join_list_as_string(importing_regions)} | Exporting Regions: {join_list_as_string(exporting_regions)} | Balanced Regions: {join_list_as_string(balanced_regions)}"
    )

    if global_trade_balance > 0:
        logger.info(
            f"TRADE BALANCING ROUND 1: Trade Balance Surplus of {global_trade_balance} Mt in year {year}. Balancing to zero."
        )
        for region in exporting_regions:
            if global_trade_balance > 0:
                demand = production_demand_dict_c[region]["demand"]
                capacity = production_demand_dict_c[region]["capacity"]
                current_balance = market_container.trade_container_getter(year, region)
                value_to_subtract = min(current_balance, global_trade_balance)
                new_balance = current_balance - value_to_subtract
                market_container.assign_trade_balance(year, region, new_balance)
                global_trade_balance -= value_to_subtract
                total_production = demand + new_balance
                data_entry_dict = {
                    "new_balance": new_balance,
                    "new_utilized_capacity": total_production,
                    "new_utilization": total_production / capacity
                }
                production_demand_dict_c = modify_production_demand_dict(
                    production_demand_dict_c, data_entry_dict, region
                )
    elif global_trade_balance < 0:
        logger.info(
            f"TRADE BALANCING ROUND 2: Trade Balance Deficit of {global_trade_balance} Mt in year {year}, balancing to zero via utilization optimization."
        )
        rpc_df = relative_production_cost_df[
            relative_production_cost_df["relative_cost_close_to_mean"] == True
        ].sort_values(["cost_of_steelmaking"], ascending=True)
        for region in rpc_df.index:
            if global_trade_balance >= 0:
                pass
            else:
                # increase utilization
                current_utilization = production_demand_dict_c[region][
                    "new_utilization"
                ]
                total_capacity = production_demand_dict_c[region]["new_total_capacity"]
                current_utilized_capacity = production_demand_dict_c[region][
                    "new_utilized_capacity"
                ]
                potential_extra_production = (
                    util_max - current_utilization
                ) * total_capacity
                demand = production_demand_dict_c[region]["demand"]
                if potential_extra_production <= 0:
                    pass
                elif potential_extra_production > abs(global_trade_balance):
                    logger.info(
                        f"TRADE BALANCING ROUND 2 - A: {region} can supply all of the import demand."
                    )
                    new_utilized_capacity = (
                        abs(global_trade_balance) + current_utilized_capacity
                    )
                    new_min_utilization_required = (
                        new_utilized_capacity / total_capacity
                    )
                    new_min_utilization_required = utilization_boundary(
                        new_min_utilization_required, util_min, util_max
                    )
                    combined_balance = abs(
                        global_trade_balance
                    ) + market_container.trade_container_getter(year, region)
                    data_entry_dict = {
                        "new_capacity_required": 0,
                        "plants_required": 0,
                        "plants_to_close": 0,
                        "new_total_capacity": total_capacity,
                        "new_utilized_capacity": new_utilized_capacity,
                        "new_balance": combined_balance,
                        "new_utilization": new_min_utilization_required
                    }

                    market_container.assign_trade_balance(
                        year, region, combined_balance
                    )
                    production_demand_dict_c = modify_production_demand_dict(
                        production_demand_dict_c, data_entry_dict, region
                    )
                    utilization_container.update_region(
                        year, region, new_min_utilization_required
                    )
                    global_trade_balance = 0
                else:
                    logger.info(
                        f"TRADE BALANCING ROUND 2 - B: {region} can supply {potential_extra_production :0.2f} of the import demand of {global_trade_balance :0.2f}."
                    )
                    new_utilized_capacity = (
                        potential_extra_production + current_utilized_capacity
                    )
                    new_min_utilization_required = (
                        new_utilized_capacity / total_capacity
                    )
                    new_min_utilization_required = utilization_boundary(
                        new_min_utilization_required, util_min, util_max
                    )
                    combined_balance = (
                        potential_extra_production
                        + market_container.trade_container_getter(year, region)
                    )
                    data_entry_dict = {
                        "new_capacity_required": 0,
                        "plants_required": 0,
                        "plants_to_close": 0,
                        "new_total_capacity": total_capacity,
                        "new_utilized_capacity": new_utilized_capacity,
                        "new_balance": combined_balance,
                        "new_utilization": new_min_utilization_required,
                    }

                    market_container.assign_trade_balance(
                        year, region, combined_balance
                    )
                    production_demand_dict_c = modify_production_demand_dict(
                        production_demand_dict_c, data_entry_dict, region
                    )
                    utilization_container.update_region(
                        year, region, new_min_utilization_required
                    )
                    global_trade_balance += potential_extra_production

        # ROUND 2: Open new plants in cheapest region
        if global_trade_balance < 0:
            cheapest_region = rpc_df["cost_of_steelmaking"].idxmin()
            logger.info(
                f"TRADE BALANCING ROUND 3: Assigning trade balance of {global_trade_balance :0.2f} Mt to cheapest region: {cheapest_region}"
            )
            capacity = production_demand_dict_c[cheapest_region]["new_total_capacity"]
            regional_demand = production_demand_dict_c[cheapest_region]["demand"]
            prior_trade_value = market_container.trade_container_getter(
                year, cheapest_region
            )
            combined_balance = abs(global_trade_balance) + prior_trade_value
            combined_demand = regional_demand + combined_balance
            new_capacity_required = combined_demand - (capacity * util_max)
            new_plants_required = math.ceil(
                new_capacity_required / avg_plant_capacity_at_max_production
            )
            new_total_capacity = capacity + (new_plants_required * avg_plant_capacity)
            new_min_utilization_required = combined_demand / new_total_capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, util_min, util_max
            )
            new_utilized_capacity = new_min_utilization_required * new_total_capacity
            data_entry_dict = {
                "new_capacity_required": new_capacity_required,
                "plants_required": new_plants_required,
                "plants_to_close": 0,
                "new_total_capacity": new_total_capacity,
                "new_utilized_capacity": new_utilized_capacity,
                "new_balance": combined_balance,
                "new_utilization": new_min_utilization_required
            }
            market_container.assign_trade_balance(
                year, cheapest_region, combined_balance
            )
            utilization_container.update_region(
                year, cheapest_region, new_min_utilization_required
            )
            production_demand_dict_c = modify_production_demand_dict(
                production_demand_dict_c, data_entry_dict, cheapest_region
            )
            global_trade_balance = 0

    elif global_trade_balance == 0:
        logger.info(
            f"Trade Balance is completely balanced at {global_trade_balance} Mt in year {year}"
        )

    for region in region_list:
        production_demand_dict_c[region]["trade_balance"] = production_demand_dict_c[
            region
        ].pop("new_balance")
    logger.info(
        f"Final Trade Balance is {round(market_container.trade_container_getter(year), 3)} Mt in year {year}"
    )

    return production_demand_dict_c
