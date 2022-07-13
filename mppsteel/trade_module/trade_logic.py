"""Module that contains the trade functions"""

from collections import namedtuple
import math
from mppsteel.config.model_config import TRADE_ROUNDING_NUMBER

from mppsteel.model_solver.solver_classes import (
    CapacityContainerClass,
    UtilizationContainerClass,
    MarketContainerClass,
)
from mppsteel.trade_module.trade_helpers import (
    create_empty_market_dict, 
    get_initial_utilization, 
    test_market_dict_output, 
    utilization_boundary
)
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


MarketBalanceContainer = namedtuple(
    'MarketBalance', 
    ['mkt_balance', 'import_adjusted_demand', 'imports', 'exports']
)
ClosePlantsContainer = namedtuple(
    'ClosePlants', 
    [ 'plants_to_close', 'new_total_capacity', 'new_min_utilization_required', 'new_utilized_capacity']
)
OpenPlantsContainer = namedtuple(
    'OpenPlants', 
    ['new_plants_required', 'new_total_capacity', 'new_min_utilization_required', 'new_utilized_capacity']
)

def create_plant_change_dict(
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    year: int,
    region: str,
    demand_dict: dict,
    util_min: float,
    util_max: float
):
    avg_plant_capacity_value = capacity_container.return_avg_capacity_value()
    capacity = capacity_container.return_regional_capacity(year, region)
    demand = demand_dict[region]
    initial_utilization = get_initial_utilization(utilization_container, year, region)
    bounded_utilization = utilization_boundary(initial_utilization, util_min, util_max)
    regional_balance = (capacity * bounded_utilization) - demand
    return create_empty_market_dict(
        year, region, capacity, demand, bounded_utilization, regional_balance, avg_plant_capacity_value)

# Helper Logic Functions
def calculate_mkt_balance(production: float, demand: float):
    mkt_balance_calc = production - demand
    imports = 0 if mkt_balance_calc > 0 else abs(mkt_balance_calc)
    exports = 0 if mkt_balance_calc < 0 else abs(mkt_balance_calc)
    return MarketBalanceContainer(
        mkt_balance_calc,
        demand - imports,
        imports,
        exports
    )

def assign_mkt_balance_to_mkt_container(market_container: MarketContainerClass, mkt_balance: MarketBalanceContainer, year: int, region: str):
    market_container.assign_market_tuple(
        year, 
        region, 
        market_container.return_market_entry(
            mkt_balance.import_adjusted_demand,
            mkt_balance.imports,
            mkt_balance.exports
        )
    )

def calculate_plants_to_close(
    initial_capacity: float, excess_capacity: float, 
    avg_plant_capacity_value: float, production: float, 
    util_min: float, util_max: float
):
    plants_to_close = math.ceil(
        excess_capacity / avg_plant_capacity_value
    )
    new_total_capacity = initial_capacity - (plants_to_close * avg_plant_capacity_value)
    new_min_utilization_required = production / new_total_capacity
    assert new_min_utilization_required >= util_min, f"{new_min_utilization_required}"
    new_min_utilization_required = utilization_boundary(
        new_min_utilization_required, util_min, util_max
    )
    new_utilized_capacity = new_min_utilization_required * new_total_capacity

    return ClosePlantsContainer(
        plants_to_close,
        new_total_capacity,
        new_min_utilization_required,
        new_utilized_capacity
    )

def calculate_plants_to_open(
    initial_capacity: float, new_capacity_required: float, 
    avg_plant_capacity_value: float, avg_plant_capacity_at_max_production: float,
    production: float, util_min: float, util_max: float
):
    new_plants_required = math.ceil(
        new_capacity_required / avg_plant_capacity_at_max_production
    )
    new_total_capacity = initial_capacity + (
        new_plants_required * avg_plant_capacity_value
    )
    new_min_utilization_required = production / new_total_capacity
    new_min_utilization_required = utilization_boundary(
        new_min_utilization_required, util_min, util_max
    )
    new_utilized_capacity = (
        new_min_utilization_required * new_total_capacity
    )
    return OpenPlantsContainer(
        new_plants_required,
        new_total_capacity,
        new_min_utilization_required,
        new_utilized_capacity
    )

def balanced_regional_balance(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float
):
    cases[region].append("R0: BALANCED -> do nothing")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    initial_utilization = plant_change_dict["initial_utilization"]
    new_utilized_capacity = capacity * initial_utilization
    mkt_balance = calculate_mkt_balance(new_utilized_capacity, demand)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    capacity_container[region] = capacity
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = initial_utilization
    test_market_dict_output(plant_change_dict, util_min, util_max)
    return plant_change_dict, capacity_container, cases


def cheap_excess_supply_export(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float
):
    cases[region].append("R0: CHEAP EXCESS SUPPLY -> export")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    initial_utilization = plant_change_dict["initial_utilization"]
    initial_production = capacity * initial_utilization
    mkt_balance = calculate_mkt_balance(initial_production, demand)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = initial_production
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = initial_utilization
    test_market_dict_output(plant_change_dict, util_min, util_max)
    capacity_container[region] = capacity
    utilization_container.update_region(year, region, initial_utilization)
    return plant_change_dict, capacity_container, cases



def expensive_excess_supply_lower_utilization(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max = float
):
    cases[region].append("R0: EXPENSIVE EXCESS SUPPLY -> reduce utilization if possible")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    new_min_utilization_required = demand / capacity
    new_min_utilization_required = utilization_boundary(
        new_min_utilization_required, util_min, util_max
    )
    new_utilized_capacity = capacity * new_min_utilization_required
    mkt_balance = calculate_mkt_balance(new_utilized_capacity, demand)
    market_container.assign_market_tuple(
        year, 
        region, 
        market_container.return_market_entry(
            mkt_balance.import_adjusted_demand,
            mkt_balance.imports,
            mkt_balance.exports
        )
    )
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = new_min_utilization_required
    test_market_dict_output(plant_change_dict, util_min, util_max)
    capacity_container[region] = capacity
    utilization_container.update_region(year, region, new_min_utilization_required)
    return plant_change_dict, capacity_container, cases


def close_plants(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float,
    avg_plant_capacity: float,
):
    cases[region].append("R0: EXPENSIVE EXCESS SUPPLY -> close plant")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    required_capacity = demand / util_min
    excess_capacity = capacity - required_capacity
    closed_plant_metadata = calculate_plants_to_close(
        capacity, excess_capacity, avg_plant_capacity, 
        demand, util_min, util_max
    )
    mkt_balance = calculate_mkt_balance(closed_plant_metadata.new_utilized_capacity, demand)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["plants_to_close"] = closed_plant_metadata.plants_to_close
    plant_change_dict["new_capacity_required"] = -excess_capacity
    plant_change_dict["new_total_capacity"] = closed_plant_metadata.new_total_capacity
    plant_change_dict["new_utilized_capacity"] = closed_plant_metadata.new_utilized_capacity
    plant_change_dict["new_balance"] = closed_plant_metadata.new_utilized_capacity - demand
    plant_change_dict["new_utilization"] = closed_plant_metadata.new_min_utilization_required
    test_market_dict_output(plant_change_dict, util_min, util_max)
    capacity_container[region] = closed_plant_metadata.new_total_capacity
    utilization_container.update_region(year, region, closed_plant_metadata.new_min_utilization_required)
    return plant_change_dict, capacity_container, cases


def supply_deficit_increase_utilization(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float
):
    cases[region].append("R0: INSUFFICIENT SUPPLY -> increase utilization (test)")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    new_min_utilization_required = demand / capacity
    new_min_utilization_required = utilization_boundary(
        new_min_utilization_required, util_min, util_max
    )
    new_utilized_capacity = new_min_utilization_required * capacity
    mkt_balance = calculate_mkt_balance(new_utilized_capacity, demand)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = new_min_utilization_required
    test_market_dict_output(plant_change_dict, util_min, util_max)
    capacity_container[region] = capacity
    utilization_container.update_region(year, region, new_min_utilization_required)
    return plant_change_dict, capacity_container, cases


def open_plants(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float,
    avg_plant_capacity_value: float,
    avg_plant_capacity_at_max_production: float
):
    cases[region].append("R0: CHEAP REGION -> open plant")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    new_capacity_required = demand - (capacity * util_max)
    open_plants_metadata = calculate_plants_to_open(
        capacity, new_capacity_required, avg_plant_capacity_value, 
        avg_plant_capacity_at_max_production, demand, util_min, util_max
    )
    mkt_balance = calculate_mkt_balance(open_plants_metadata.new_utilized_capacity, demand)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["plants_required"] = open_plants_metadata.new_plants_required
    plant_change_dict["new_capacity_required"] = new_capacity_required
    plant_change_dict["new_total_capacity"] = open_plants_metadata.new_total_capacity
    plant_change_dict["new_utilized_capacity"] = open_plants_metadata.new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = open_plants_metadata.new_min_utilization_required
    test_market_dict_output(plant_change_dict, util_min, util_max)
    capacity_container[region] = open_plants_metadata.new_total_capacity
    utilization_container.update_region(year, region, open_plants_metadata.new_min_utilization_required)
    return plant_change_dict, capacity_container, cases


def supply_deficit_import(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float
):
    cases[region].append("R0: INSUFFICIENT SUPPLY, EXPENSIVE REGION, MAX UTILIZATION -> import")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    new_min_utilization_required = demand / capacity
    new_min_utilization_required = utilization_boundary(
        new_min_utilization_required, util_min, util_max
    )
    new_utilized_capacity = new_min_utilization_required * capacity
    mkt_balance = calculate_mkt_balance(new_utilized_capacity, demand)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = new_min_utilization_required
    test_market_dict_output(plant_change_dict, util_min, util_max)
    capacity_container[region] = capacity
    utilization_container.update_region(year, region, new_min_utilization_required)
    return plant_change_dict, capacity_container, cases


def excess_production_lower_utilization(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    global_trade_balance: float,
    util_min: float,
    util_max: float
):
    cases[region].append("R1: Reducing excess production via lowering utilization")
    current_utilization = utilization_container.get_utilization_values(year, region)
    capacity = capacity_container[region]
    current_balance = market_container.trade_container_aggregator(year, "trade", region)
    value_to_subtract_from_global = min(current_balance, global_trade_balance)
    max_removable_value = (current_utilization - util_min) * capacity
    value_to_subtract_from_global = min(value_to_subtract_from_global, max_removable_value)
    market_container.assign_market_tuple(
        year, 
        region, 
        market_container.return_market_entry(
            0,
            0,
            -value_to_subtract_from_global
        )
    )
    new_balance = market_container.trade_container_aggregator(year, "trade", region)
    total_production = market_container.trade_container_aggregator(year, "production", region)
    new_utilization = total_production / capacity
    assert round(new_utilization, TRADE_ROUNDING_NUMBER) >= util_min
    plant_change_dict[region]["new_total_capacity"] = capacity
    plant_change_dict[region]["new_utilization"] = new_utilization
    plant_change_dict[region]["new_utilized_capacity"] = total_production
    plant_change_dict[region]["new_balance"] = new_balance
    test_market_dict_output(plant_change_dict[region], util_min, util_max)
    capacity_container[region] = capacity
    utilization_container.update_region(year, region, new_utilization)
    global_trade_balance -= value_to_subtract_from_global
    return plant_change_dict, capacity_container, cases, global_trade_balance


def assign_all_import_demand(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    global_trade_balance: float,
    util_min: float,
    util_max: float
):
    cases[region].append("R2-A: moving all import demand to region")
    value_to_add_to_global = abs(global_trade_balance)
    current_utilized_capacity = market_container.trade_container_aggregator(
        year, "production", region
    )
    market_container.assign_market_tuple(
        year, 
        region, 
        market_container.return_market_entry(
            0,
            0,
            value_to_add_to_global
        )
    )
    capacity = capacity_container[region]
    new_utilized_capacity = (
        current_utilized_capacity + value_to_add_to_global
    )
    new_min_utilization_required = (
        new_utilized_capacity / capacity
    )
    assert new_min_utilization_required <= util_max
    new_min_utilization_required = utilization_boundary(
        new_min_utilization_required, util_min, util_max
    )
    new_balance = market_container.trade_container_aggregator(year, "trade", region)
    plant_change_dict[region]["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict[region]["new_balance"] = new_balance
    plant_change_dict[region]["new_utilization"] = new_min_utilization_required
    test_market_dict_output(plant_change_dict[region], util_min, util_max)
    utilization_container.update_region(year, region, new_min_utilization_required)
    capacity_container[region] = capacity
    global_trade_balance = 0
    return plant_change_dict, capacity_container, cases, global_trade_balance


def assign_partial_import_demand(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float,
    global_trade_balance: float,
    potential_extra_production: float
):
    cases[region].append("R2-B: moving partial import demand to region")
    total_capacity = capacity_container[region]
    market_container.assign_market_tuple(
        year, 
        region, 
        market_container.return_market_entry(
            0,
            0,
            potential_extra_production
        )
    )
    total_production = market_container.trade_container_aggregator(year, "production", region)
    new_min_utilization_required = total_production / total_capacity
    trade_balance = market_container.trade_container_aggregator(year, "trade", region)
    plant_change_dict[region]["new_utilized_capacity"] = total_production
    plant_change_dict[region]["new_balance"] = trade_balance
    plant_change_dict[region]["new_utilization"] = new_min_utilization_required
    test_market_dict_output(plant_change_dict[region], util_min, util_max)
    utilization_container.update_region(year, region, new_min_utilization_required)
    capacity_container[region] = total_capacity
    global_trade_balance += potential_extra_production
    return plant_change_dict, capacity_container, cases, global_trade_balance


def open_plants_cheapest_region(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    global_trade_balance: float,
    avg_plant_capacity_value: float,
    avg_plant_capacity_at_max_production: float,
    util_min: float,
    util_max: float
):
    cases[region].append("R3: moving remaining import demand to cheapest region")
    initial_capacity = capacity_container[region]
    demand = plant_change_dict[region]["demand"]
    value_to_add_to_global = abs(global_trade_balance)
    market_container.assign_market_tuple(
        year, 
        region, 
        market_container.return_market_entry(
            0,
            0,
            value_to_add_to_global
        )
    )
    total_production = market_container.trade_container_aggregator(year, "production", region)
    new_capacity_required = total_production - (initial_capacity * util_max)
    open_plants_metadata = calculate_plants_to_open(
        initial_capacity, new_capacity_required, avg_plant_capacity_value, 
        avg_plant_capacity_at_max_production, total_production, util_min, util_max
    )
    mkt_balance = calculate_mkt_balance(open_plants_metadata.new_utilized_capacity, demand)
    plant_change_dict[region]["new_capacity_required"] = new_capacity_required
    plant_change_dict[region]["plants_required"] = open_plants_metadata.new_plants_required
    plant_change_dict[region]["new_total_capacity"] = open_plants_metadata.new_total_capacity
    plant_change_dict[region]["new_utilized_capacity"] = open_plants_metadata.new_utilized_capacity
    plant_change_dict[region]["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict[region]["new_utilization"] = open_plants_metadata.new_min_utilization_required
    test_market_dict_output(plant_change_dict[region], util_min, util_max)
    capacity_container[region] = open_plants_metadata.new_total_capacity
    utilization_container.update_region(year, region, open_plants_metadata.new_min_utilization_required)
    global_trade_balance = 0
    return plant_change_dict, capacity_container, cases, global_trade_balance


def close_plants_for_exporters(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    cases: dict,
    year: int,
    region: str,
    global_trade_balance: float,
    avg_plant_capacity_value: float,
    util_min: float,
    util_max: float
):
    cases[region].append("R4-B: EXCESS SUPPLY, MIN UTILZATION -> close plants")
    capacity = capacity_container[region]
    demand = plant_change_dict[region]["demand"]
    region_trade_balance = market_container.trade_container_aggregator(year, "trade", region)
    trade_prodution_to_close = min(region_trade_balance, global_trade_balance)
    market_container.assign_market_tuple(
        year, 
        region, 
        market_container.return_market_entry(
            0,
            0,
            -trade_prodution_to_close
        )
    )
    total_required_production = market_container.return_trade_balance(year, region, "production")
    excess_capacity = capacity - (total_required_production / util_min)
    close_plant_metadata = calculate_plants_to_close(
        capacity, excess_capacity, avg_plant_capacity_value, 
        total_required_production, util_min, util_max
    )
    mkt_balance = calculate_mkt_balance(close_plant_metadata.new_utilized_capacity, demand)
    assert round(total_required_production, TRADE_ROUNDING_NUMBER) == round(close_plant_metadata.new_utilized_capacity, TRADE_ROUNDING_NUMBER), f"Production Stats || region: {region} | container: {total_required_production} | dict: {close_plant_metadata.new_utilized_capacity}"
    plant_change_dict[region]["plants_to_close"] = close_plant_metadata.plants_to_close
    plant_change_dict[region]["new_total_capacity"] = close_plant_metadata.new_total_capacity
    plant_change_dict[region]["new_utilized_capacity"] = close_plant_metadata.new_utilized_capacity
    plant_change_dict[region]["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict[region]["new_utilization"] = close_plant_metadata.new_min_utilization_required
    test_market_dict_output(plant_change_dict[region], util_min, util_max)
    capacity_container[region] = close_plant_metadata.new_total_capacity
    utilization_container.update_region(year, region, close_plant_metadata.new_min_utilization_required)
    global_trade_balance -= trade_prodution_to_close
    return plant_change_dict, capacity_container, cases, global_trade_balance
