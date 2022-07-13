"""Module that contains the trade functions"""

import pandas as pd

from mppsteel.config.model_config import (
    MAIN_REGIONAL_SCHEMA,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    TRADE_PCT_BOUNDARY_FACTOR_DICT,
    TRADE_ROUNDING_NUMBER
)
from mppsteel.model_solver.solver_classes import (
    CapacityContainerClass,
    UtilizationContainerClass,
    MarketContainerClass,
)
from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.trade_module.trade_helpers import (
    calculate_cos,
    check_relative_production_cost,
    test_capacity_values,
    test_open_close_plants,
    test_production_equals_demand,
    test_production_values,
    test_utilization_values,
)
from mppsteel.trade_module.trade_logic import (
    assign_all_import_demand, 
    assign_partial_import_demand, 
    balanced_regional_balance, 
    cheap_excess_supply_export, 
    close_plants, 
    close_plants_for_exporters, 
    create_plant_change_dict, 
    excess_production_lower_utilization, 
    expensive_excess_supply_lower_utilization, 
    lower_utilization_for_exporters, 
    open_plants, 
    open_plants_cheapest_region, 
    supply_deficit_import, 
    supply_deficit_increase_utilization
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.utils import join_list_as_string

logger = get_logger(__name__)


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
    cos_df = calculate_cos(
        plant_df,
        year,
        utilization_container,
        variable_cost_df,
        tech_choices_ref,
        capex_dict,
        capacity_container,
    )
    relative_production_cost_df = check_relative_production_cost(
        cos_df, "cost_of_steelmaking", TRADE_PCT_BOUNDARY_FACTOR_DICT
    )
    region_list = list(plant_df[MAIN_REGIONAL_SCHEMA].unique())
    results_container = {}
    regional_capacity_dict = {region: 0 for region in region_list}
    cases = {region: [] for region in region_list}
    demand_dict = {
        region: steel_demand_getter(steel_demand_df, year=year, metric="crude", region=region) for region in region_list
    }
    avg_plant_capacity_value = capacity_container.return_avg_capacity_value()

    for region in region_list:
        avg_plant_capacity_at_max_production = avg_plant_capacity_value * util_max
        relative_cost_close_to_mean = relative_production_cost_df.loc[region][
            "relative_cost_close_to_mean"
        ]
        plant_change_dict = create_plant_change_dict(
            capacity_container, utilization_container, 
            year, region, demand_dict, util_min, util_max
        )
        initial_balance = round(plant_change_dict["initial_balance"], TRADE_ROUNDING_NUMBER)

        capacity = plant_change_dict["capacity"]
        demand = plant_change_dict["demand"]
        new_min_utilization_required = round(demand / capacity, TRADE_ROUNDING_NUMBER)

        # BALANCED
        if initial_balance == 0:
            plant_change_dict, regional_capacity_dict, cases = balanced_regional_balance(
                plant_change_dict, market_container, regional_capacity_dict, cases, year, region, util_min, util_max
            )

        # EXPORTERS
        elif (initial_balance > 0) and relative_cost_close_to_mean:
            plant_change_dict, regional_capacity_dict, cases = cheap_excess_supply_export(
                plant_change_dict, market_container, utilization_container, regional_capacity_dict, cases, year, region,
                util_min, util_max
            )
        elif (initial_balance > 0) and not relative_cost_close_to_mean:
            if util_min <= new_min_utilization_required:
                plant_change_dict, regional_capacity_dict, cases = expensive_excess_supply_lower_utilization(
                    plant_change_dict, market_container, utilization_container, regional_capacity_dict, cases, year, region,
                    util_min, util_max
                )
            elif new_min_utilization_required < util_min:
                plant_change_dict, regional_capacity_dict, cases = close_plants(
                    plant_change_dict, market_container, utilization_container, regional_capacity_dict, cases, year, region,
                    util_min, util_max, avg_plant_capacity_value
                )

        # IMPORTERS
        elif (initial_balance < 0) and relative_cost_close_to_mean:
            if new_min_utilization_required < util_max:
                plant_change_dict, regional_capacity_dict, cases = supply_deficit_increase_utilization(
                    plant_change_dict, market_container, utilization_container, regional_capacity_dict, cases, year, region,
                    util_min, util_max,
                )
            elif new_min_utilization_required > util_max:
                plant_change_dict, regional_capacity_dict, cases = open_plants(
                    plant_change_dict, market_container, utilization_container, regional_capacity_dict, cases, year, region,
                    util_min, util_max, avg_plant_capacity_value, avg_plant_capacity_at_max_production
                )
        elif (initial_balance < 0) and not relative_cost_close_to_mean:
                plant_change_dict, regional_capacity_dict, cases = supply_deficit_import(
                    plant_change_dict, market_container, utilization_container, regional_capacity_dict, cases, year, region,
                    util_min, util_max
                )

        results_container[region] = plant_change_dict
        total_production_container = round(market_container.return_trade_balance(year, region, "production"), TRADE_ROUNDING_NUMBER)
        total_production_dict = round(plant_change_dict["new_utilized_capacity"], TRADE_ROUNDING_NUMBER)
        assert total_production_dict == total_production_container, f"regional production test - Initial Round: {region} dict {total_production_dict} | container {total_production_container}"

    global_trade_balance = market_container.trade_container_aggregator(year, "trade")
    importing_regions = market_container.list_regional_types(year, "imports")
    exporting_regions = market_container.list_regional_types(year, "exports")
    balanced_regions = market_container.check_if_trade_balance(year)

    rpc_df = relative_production_cost_df[
        relative_production_cost_df["relative_cost_close_to_mean"] == True
    ]

    logger.info(
        f"TRADE BALANCING ROUND 1: Importing Regions: {join_list_as_string(importing_regions)} | Exporting Regions: {join_list_as_string(exporting_regions)} | Balanced Regions: {join_list_as_string(balanced_regions)}"
    )

    if round(global_trade_balance, TRADE_ROUNDING_NUMBER) == 0:
        logger.info(
            f"TRADE BALANCING ROUND 2: Trade Balance is completely balanced at {global_trade_balance: 2f} Mt in year {year}"
        )

    elif round(global_trade_balance, TRADE_ROUNDING_NUMBER) > 0:
        logger.info(
            f"TRADE BALANCING ROUND 2-A: Trade Balance Surplus of {global_trade_balance: 2f} Mt in year {year}. Balancing to zero."
        )
        for region in exporting_regions:
            current_utilization = utilization_container.get_utilization_values(year, region)
            regional_trade_balance = market_container.trade_container_aggregator(year, "trade", region)
            if (round(global_trade_balance, TRADE_ROUNDING_NUMBER) > 0) and (current_utilization > util_min) and (regional_trade_balance > 0):
                plant_change_dict, regional_capacity_dict, cases, global_trade_balance = excess_production_lower_utilization(
                    results_container, market_container, utilization_container, regional_capacity_dict, cases, year, region, 
                    global_trade_balance, util_min, util_max
                )

        logger.info(
            f"TRADE BALANCING ROUND 2-B: Reducing excess trade balance of {global_trade_balance :0.2f} via closing plants"
        )
        exporting_regions = market_container.list_regional_types(year, "exports")
        for region in rpc_df.loc[exporting_regions].sort_values(["cost_of_steelmaking"], ascending=False).index:
            if round(global_trade_balance, TRADE_ROUNDING_NUMBER) > 0:
                results_container, regional_capacity_dict, cases, global_trade_balance = close_plants_for_exporters(
                    results_container, market_container, utilization_container, regional_capacity_dict, cases,
                    year, region, global_trade_balance, avg_plant_capacity_value, util_min, util_max
                )

    elif round(global_trade_balance, TRADE_ROUNDING_NUMBER) < 0:
        logger.info(
            f"TRADE BALANCING ROUND 3: Trade Balance Deficit of {global_trade_balance: 2f} Mt in year {year}, balancing to zero via utilization optimization."
        )
        for region in rpc_df.sort_values(["cost_of_steelmaking"], ascending=True).index:
            # increase utilization
            current_utilization = utilization_container.get_utilization_values(year, region)
            total_capacity = regional_capacity_dict[region]
            current_utilized_capacity = market_container.trade_container_aggregator(year, "production", region)
            potential_extra_production = (total_capacity * util_max) - current_utilized_capacity
            if round(potential_extra_production, TRADE_ROUNDING_NUMBER) <= 0:
                pass
            elif round(potential_extra_production, TRADE_ROUNDING_NUMBER) >= abs(global_trade_balance) and round(global_trade_balance, TRADE_ROUNDING_NUMBER) < 0:
                logger.info(
                    f"TRADE BALANCING ROUND 3-A: {region} can supply all of the import demand."
                )
                results_container, regional_capacity_dict, cases, global_trade_balance = assign_all_import_demand(
                    results_container, market_container, utilization_container, regional_capacity_dict, cases, year, region, global_trade_balance, util_min, util_max
                )
            elif round(potential_extra_production, TRADE_ROUNDING_NUMBER) < abs(global_trade_balance):
                logger.info(
                    f"TRADE BALANCING ROUND 3-B: {region} can supply {potential_extra_production :0.2f} of the import demand of {global_trade_balance :0.2f}."
                )
                results_container, regional_capacity_dict, cases, global_trade_balance = assign_partial_import_demand(
                    results_container, market_container, utilization_container, regional_capacity_dict, cases, year, region, 
                    util_min, util_max, global_trade_balance, potential_extra_production
                )

        # ROUND 3: Open new plants in cheapest region
        if round(global_trade_balance, TRADE_ROUNDING_NUMBER) < 0:
            cheapest_region = rpc_df["cost_of_steelmaking"].idxmin()
            logger.info(
                f"TRADE BALANCING ROUND 3-C: Assigning trade balance of {global_trade_balance :2f} Mt to cheapest region: {cheapest_region}"
            )
            results_container, regional_capacity_dict, cases, global_trade_balance = open_plants_cheapest_region(
                results_container, market_container, utilization_container, regional_capacity_dict, cases, year, cheapest_region, global_trade_balance,
                avg_plant_capacity_value, avg_plant_capacity_at_max_production, util_min, util_max
            )

    # final trade balance
    global_trade_balance = market_container.trade_container_aggregator(year, "trade")

    if round(global_trade_balance, TRADE_ROUNDING_NUMBER) == 0:
        logger.info(
            f"Trade Balance is completely balanced at {global_trade_balance: 4f} Mt in year {year}"
        )
    else:
        raise AssertionError(f"Trade Balance is not equal to zero after all rounds complete -> {global_trade_balance: 2f} ||| {market_container.trade_container_getter(year)}")

    global_production = market_container.trade_container_aggregator(year, "production")
    global_demand = sum(demand_dict.values())
    assert round(global_production, TRADE_ROUNDING_NUMBER) == round(global_demand, TRADE_ROUNDING_NUMBER), f"production: {global_production: 2f} | demand: {global_demand: 2f} --- {market_container.trade_container_getter(year)}"

    test_open_close_plants(results_container, cases)
    test_production_values(results_container, market_container, cases, year)
    test_capacity_values(results_container, regional_capacity_dict, cases)
    test_production_equals_demand(global_demand, global_production)
    test_utilization_values(utilization_container, year, util_min, util_max, cases)

    logger.info(f"Final Trade Balance is {global_trade_balance: 2f} Mt in year {year}")
    return results_container