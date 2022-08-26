"""Module that contains the trade functions"""

from collections import namedtuple
import math
from typing import Tuple
from mppsteel.config.model_config import TRADE_ROUNDING_NUMBER

from mppsteel.plant_classes.capacity_container_class import CapacityContainerClass
from mppsteel.model_solver.market_container_class import MarketContainerClass
from mppsteel.plant_classes.regional_utilization_class import UtilizationContainerClass
from mppsteel.trade_module.trade_helpers import (
    TradeStatus,
    create_empty_market_dict,
    get_initial_utilization,
    test_market_dict_output,
    utilization_boundary,
)
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


MarketBalanceContainer = namedtuple(
    "MarketBalanceContainer", ["mkt_balance", "import_adjusted_demand", "imports", "exports"]
)
ClosePlantsContainer = namedtuple(
    "ClosePlantsContainer",
    [
        "plants_to_close",
        "new_total_capacity",
        "new_min_utilization_required",
        "new_utilized_capacity",
        "new_capacity_required",
    ],
)
OpenPlantsContainer = namedtuple(
    "OpenPlantsContainer",
    [
        "new_plants_required",
        "new_total_capacity",
        "new_min_utilization_required",
        "new_utilized_capacity",
    ],
)


def create_plant_change_dict(
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    year: int,
    region: str,
    demand_dict: dict,
    util_min: float,
    util_max: float,
) -> dict:
    """Initializes the data/metadata dictionary that is used for the trade flow.

    Args:
        capacity_container (CapacityContainerClass): The CapacityContainerClass Instance containing the capacity state.
        utilization_container (UtilizationContainerClass): The UtilizationContainerClass Instance containing the utilization state.
        year (int): The current model year.
        region (str): The region to get trade values for.
        demand_dict (dict): A dictionary containing the demand values for each region.
        util_min (float): The minimum capacity utilization that plants are allowed to reach before having to close existing plants.
        util_max (float): The maximum capacity utilization that plants are allowed to reach before having to open new plants.

    Returns:
        dict: The dictionary containing the data/metadata for the region.
    """
    avg_plant_capacity_value = capacity_container.return_avg_capacity_value()
    capacity = capacity_container.return_regional_capacity(year, region)
    demand = demand_dict[region]
    initial_utilization = get_initial_utilization(utilization_container, year, region)
    bounded_utilization = utilization_boundary(initial_utilization, util_min, util_max)
    regional_balance = (capacity * bounded_utilization) - demand
    return create_empty_market_dict(
        year,
        region,
        capacity,
        demand,
        bounded_utilization,
        regional_balance,
        avg_plant_capacity_value,
    )


# Helper Logic Functions
def calculate_mkt_balance(production: float, demand: float, trade_status: TradeStatus) -> MarketBalanceContainer:
    """Determines a new market balance given the inputs of production, demand and a region's trade_status.

    Args:
        production (float): Post-adjusted production value.
        demand (float): Post-adjusted demand value.
        trade_status (TradeStatus): The initial trade status of the region.

    Returns:
        MarketBalanceContainer: A namedtuple object containing the market balance data.
    """
    mkt_balance_calc = production - demand
    imports = 0 if mkt_balance_calc >= 0 else abs(mkt_balance_calc)
    exports = max(mkt_balance_calc, 0)

    if trade_status is TradeStatus.IMPORTER:
        exports = 0

    if (trade_status is TradeStatus.EXPORTER) or (trade_status is TradeStatus.DOMESTIC):
        imports = 0

    return MarketBalanceContainer(mkt_balance_calc, demand - imports, imports, exports)


def assign_mkt_balance_to_mkt_container(
    market_container: MarketContainerClass,
    mkt_balance: MarketBalanceContainer,
    year: int,
    region: str,
) -> None:
    """Adjusts the market data/entry for a specific region and year based on a MarketBalanceContainer entry.

    Args:
        market_container (MarketContainerClass): The instance of the MarketContainerClass.
        mkt_balance (MarketBalanceContainer): The namedtuple produced by calculate_mkt_balance.
        year (int): The current year of the model.
        region (str): region (str): The region to update.
    """
    market_container.assign_market_tuple(
        year,
        region,
        market_container.return_market_entry(
            mkt_balance.import_adjusted_demand, mkt_balance.imports, mkt_balance.exports
        ),
    )


def post_logic_updates(
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    plant_change_dict: dict,
    cases: dict,
    year: int,
    region: str,
    capacity: float,
    utilization: float,
    util_min: float,
    util_max: float,
) -> Tuple[dict, dict]:
    """Runs through a number of steps after a main logic step has been completed.
    1) Tests that the market dictionary produced by create_plant_change_dict is within certain boundaries
    2) Updates the utilization container with a new utilization value.
    3) Updates the capacity container with a new value.
    4) Updates the cases dictionary with new cases
    5) Returns the capacity container and the cases dictionary.

    Args:
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        capacity (float): The updated capacity of the region.
        utilization (float): The updated utilization of the region.
        util_min (float): The minimum utilization boundary.
        util_max (float): The maximum utilization boundary.

    Returns:
        Tuple[dict, dict]: Returns the capacity container and the cases dictionary.
    """
    test_market_dict_output(plant_change_dict, util_min, util_max)
    utilization_container.update_region(year, region, utilization)
    capacity_container[region] = capacity
    plant_change_dict["cases"] = cases[region]
    return capacity_container, cases


def calculate_plants_to_close(
    region: str,
    initial_capacity: float,
    production: float,
    avg_plant_capacity_value: float,
    util_min: float,
) -> ClosePlantsContainer:
    """Calculates the number of plants to close based on avg_plant_capacity_value.

    Args:
        region (str): The region for which to close plants.
        initial_capacity (float): The initial capacity of the region.
        production (float): The production value of the region.
        avg_plant_capacity_value (float): The average plant capacity for all regions.
        util_min (float): The minimum utilization value boundary.

    Returns:
        ClosePlantsContainer: A namedtuple object containing the plant close data.
    """
    capacity_required = production / util_min
    would_be_utilization = production / initial_capacity
    assert (
        would_be_utilization < util_min
    ), f"Region: {region} | Initial Capacity {initial_capacity} is able to cover production requirements because min utilization would be {would_be_utilization}"
    excess_capacity = capacity_required - initial_capacity
    plants_to_close = math.ceil(-excess_capacity / avg_plant_capacity_value)
    capacity_to_close = plants_to_close * avg_plant_capacity_value
    assert capacity_to_close > 0
    new_total_capacity = initial_capacity - (plants_to_close * avg_plant_capacity_value)
    new_min_utilization_required = production / new_total_capacity
    assert (
        new_min_utilization_required >= util_min
    ), f"Region: {region} | {new_min_utilization_required} is less than min utilization {util_min}"
    new_utilized_capacity = new_total_capacity * new_min_utilization_required
    new_capacity_required = new_total_capacity - initial_capacity
    assert (
        new_capacity_required < 0
    ), f"Region: {region} | new capacity required {new_capacity_required} is not less than 0"

    return ClosePlantsContainer(
        plants_to_close,
        new_total_capacity,
        new_min_utilization_required,
        new_utilized_capacity,
        -capacity_to_close,
    )


def calculate_plants_to_open(
    initial_capacity: float,
    new_capacity_required: float,
    avg_plant_capacity_value: float,
    avg_plant_capacity_at_max_production: float,
    production: float,
    util_min: float,
    util_max: float,
) -> OpenPlantsContainer:
    """Calculates the number of plants to open based on avg_plant_capacity_value and avg_plant_capacity_at_max_production.

    Args:
        initial_capacity (float): The initial capacity of the region.
        new_capacity_required (float): The new capacity required in the region.
        avg_plant_capacity_value (float): The average plant capacity for all regions.
        avg_plant_capacity_at_max_production (float): The average plant capacity when production is maximised at util_max.
        production (float): The production value of the region.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        OpenPlantsContainer: A namedtuple object containing the open plant open data.
    """
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
    new_utilized_capacity = new_min_utilization_required * new_total_capacity
    return OpenPlantsContainer(
        new_plants_required,
        new_total_capacity,
        new_min_utilization_required,
        new_utilized_capacity,
    )


def balanced_regional_balance(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    trade_status: TradeStatus,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float,
) -> Tuple[dict, dict, dict]:
    """Manages the case of production being equal to demand.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        trade_status (TradeStatus): The initial trade status of the region.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        Tuple[dict, dict, dict]: A tuple containing the modified plant_change_dict, capacity_container and cases
    """
    cases[region].append("R0: BALANCED -> do nothing")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    initial_utilization = plant_change_dict["initial_utilization"]
    new_utilized_capacity = capacity * initial_utilization
    mkt_balance = calculate_mkt_balance(new_utilized_capacity, demand, trade_status)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = initial_utilization
    test_market_dict_output(plant_change_dict, util_min, util_max)
    capacity_container[region] = capacity
    utilization_container.update_region(year, region, initial_utilization)
    return plant_change_dict, capacity_container, cases


def cheap_excess_supply_export(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    trade_status: TradeStatus,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float,
) -> Tuple[dict, dict, dict]:
    """Manages the case of a region having cheap excess supply thus exporting.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        trade_status (TradeStatus): The initial trade status of the region.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        Tuple[dict, dict, dict]: A tuple containing the modified plant_change_dict, capacity_container and cases
    """
    cases[region].append("R0: CHEAP EXCESS SUPPLY -> export")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    initial_utilization = plant_change_dict["initial_utilization"]
    bounded_utilization = utilization_boundary(initial_utilization, util_min, util_max)
    initial_production = capacity * bounded_utilization
    mkt_balance = calculate_mkt_balance(initial_production, demand, trade_status)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = initial_production
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = bounded_utilization
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict,
        cases,
        year,
        region,
        capacity,
        bounded_utilization,
        util_min,
        util_max,
    )
    return plant_change_dict, capacity_container, cases


def close_plants(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    trade_status: TradeStatus,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float,
    avg_plant_capacity: float,
) -> Tuple[dict, dict, dict]:
    """Manages the case where plants in a rgeion need to close.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        trade_status (TradeStatus): The initial trade status of the region.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.
        avg_plant_capacity_value (float): The average plant capacity for all regions.

    Returns:
        Tuple[dict, dict, dict]: A tuple containing the modified plant_change_dict, capacity_container and cases
    """
    cases[region].append("R0: EXPENSIVE EXCESS SUPPLY -> close plant")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    closed_plant_metadata = calculate_plants_to_close(
        region, capacity, demand, avg_plant_capacity, util_min
    )
    assert closed_plant_metadata.new_capacity_required < 0
    mkt_balance = calculate_mkt_balance(
        closed_plant_metadata.new_utilized_capacity, demand, trade_status
    )
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["plants_to_close"] = closed_plant_metadata.plants_to_close
    plant_change_dict[
        "new_capacity_required"
    ] = closed_plant_metadata.new_capacity_required
    plant_change_dict["new_total_capacity"] = closed_plant_metadata.new_total_capacity
    plant_change_dict[
        "new_utilized_capacity"
    ] = closed_plant_metadata.new_utilized_capacity
    plant_change_dict["new_balance"] = (
        closed_plant_metadata.new_utilized_capacity - demand
    )
    plant_change_dict[
        "new_utilization"
    ] = closed_plant_metadata.new_min_utilization_required
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict,
        cases,
        year,
        region,
        closed_plant_metadata.new_total_capacity,
        closed_plant_metadata.new_min_utilization_required,
        util_min,
        util_max,
    )
    return plant_change_dict, capacity_container, cases


def adjust_utilization(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    trade_status: TradeStatus,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max=float,
) -> Tuple[dict, dict, dict]:
    """Manages the case where a domestic region needs to adjust its utilization levels.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        trade_status (TradeStatus): The initial trade status of the region.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        Tuple[dict, dict, dict]: A tuple containing the modified plant_change_dict, capacity_container and cases
    """
    cases[region].append("R0: Domestic Producer -> Adjust Utilization")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    new_utilization_required = demand / capacity
    new_utilized_capacity = capacity * new_utilization_required
    mkt_balance = calculate_mkt_balance(new_utilized_capacity, demand, trade_status)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = new_utilization_required
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict,
        cases,
        year,
        region,
        capacity,
        new_utilization_required,
        util_min,
        util_max,
    )
    return plant_change_dict, capacity_container, cases


def open_plants(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    trade_status: TradeStatus,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float,
    avg_plant_capacity_value: float,
    avg_plant_capacity_at_max_production: float,
) -> Tuple[dict, dict, dict]:
    """Manages the case where a region must open plants.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        trade_status (TradeStatus): The initial trade status of the region.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.
        avg_plant_capacity_value (float): The average plant capacity for all regions.
        avg_plant_capacity_at_max_production (float): The average plant capacity when production is maximised at util_max.

    Returns:
        Tuple[dict, dict, dict]: A tuple containing the modified plant_change_dict, capacity_container and cases
    """
    cases[region].append("R0: CHEAP REGION -> open plant")
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    new_capacity_required = demand - (capacity * util_max)
    open_plants_metadata = calculate_plants_to_open(
        capacity,
        new_capacity_required,
        avg_plant_capacity_value,
        avg_plant_capacity_at_max_production,
        demand,
        util_min,
        util_max,
    )
    mkt_balance = calculate_mkt_balance(
        open_plants_metadata.new_utilized_capacity, demand, trade_status
    )
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["plants_required"] = open_plants_metadata.new_plants_required
    plant_change_dict["new_capacity_required"] = new_capacity_required
    plant_change_dict["new_total_capacity"] = open_plants_metadata.new_total_capacity
    plant_change_dict[
        "new_utilized_capacity"
    ] = open_plants_metadata.new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict[
        "new_utilization"
    ] = open_plants_metadata.new_min_utilization_required
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict,
        cases,
        year,
        region,
        open_plants_metadata.new_total_capacity,
        open_plants_metadata.new_min_utilization_required,
        util_min,
        util_max,
    )
    return plant_change_dict, capacity_container, cases


def supply_deficit_import(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    trade_status: TradeStatus,
    cases: dict,
    year: int,
    region: str,
    util_min: float,
    util_max: float,
) -> Tuple[dict, dict, dict]:
    """Manages the case where a region faces a supply deficit and must import.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        trade_status (TradeStatus): The initial trade status of the region.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        Tuple[dict, dict, dict]: A tuple containing the modified plant_change_dict, capacity_container and cases
    """
    cases[region].append(
        "R0: INSUFFICIENT SUPPLY, EXPENSIVE REGION, MAX UTILIZATION -> import"
    )
    capacity = plant_change_dict["capacity"]
    demand = plant_change_dict["demand"]
    new_utilization_required = demand / capacity
    new_utilization_required = utilization_boundary(
        new_utilization_required, util_min, util_max
    )
    new_utilized_capacity = new_utilization_required * capacity
    mkt_balance = calculate_mkt_balance(new_utilized_capacity, demand, trade_status)
    assign_mkt_balance_to_mkt_container(market_container, mkt_balance, year, region)
    plant_change_dict["new_total_capacity"] = capacity
    plant_change_dict["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict["new_utilization"] = new_utilization_required
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict,
        cases,
        year,
        region,
        capacity,
        new_utilization_required,
        util_min,
        util_max,
    )
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
    util_max: float,
) -> Tuple[dict, dict, dict, float]:
    """Manages the post-regional trade adjustment state where there is excess production and the solution is to lower production.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        global_trade_balance (float): The current balance of trade across all regions.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        Tuple[dict, dict, dict, float]: A tuple containing the modified plant_change_dict, capacity_container, cases and global_trade_balance
    """
    cases[region].append("R1: Reducing excess production via lowering utilization")
    current_utilization = utilization_container.get_utilization_values(year, region)
    capacity = capacity_container[region]
    current_balance = market_container.trade_container_aggregator(year, "trade", region)
    value_to_subtract_from_global = min(current_balance, global_trade_balance)
    max_removable_value = (current_utilization - util_min) * capacity
    value_to_subtract_from_global = min(
        value_to_subtract_from_global, max_removable_value
    )
    market_container.assign_market_tuple(
        year,
        region,
        market_container.return_market_entry(0, 0, -value_to_subtract_from_global),
    )
    new_balance = market_container.trade_container_aggregator(year, "trade", region)
    production = market_container.trade_container_aggregator(year, "production", region)
    new_min_utilization_required = production / capacity
    assert round(new_min_utilization_required, TRADE_ROUNDING_NUMBER) >= util_min
    plant_change_dict[region]["new_total_capacity"] = capacity
    plant_change_dict[region]["new_utilization"] = new_min_utilization_required
    plant_change_dict[region]["new_utilized_capacity"] = production
    plant_change_dict[region]["new_balance"] = new_balance
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict[region],
        cases,
        year,
        region,
        capacity,
        new_min_utilization_required,
        util_min,
        util_max,
    )
    test_market_dict_output(plant_change_dict[region], util_min, util_max)
    capacity_container[region] = capacity
    utilization_container.update_region(year, region, new_min_utilization_required)
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
    util_max: float,
) -> Tuple[dict, dict, dict, float]:
    """Manages the post-regional trade adjustment state where there is insufficient production to meet our import requirements.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        global_trade_balance (float): The current balance of trade across all regions.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        Tuple[dict, dict, dict, float]: A tuple containing the modified plant_change_dict, capacity_container, cases and global_trade_balance
    """
    cases[region].append("R2-A: moving all import demand to region")
    value_to_add_to_global = abs(global_trade_balance)
    current_utilized_capacity = market_container.trade_container_aggregator(
        year, "production", region
    )
    market_container.assign_market_tuple(
        year, region, market_container.return_market_entry(0, 0, value_to_add_to_global)
    )
    capacity = capacity_container[region]
    new_utilized_capacity = current_utilized_capacity + value_to_add_to_global
    new_min_utilization_required = new_utilized_capacity / capacity
    assert (
        round(new_min_utilization_required, TRADE_ROUNDING_NUMBER) <= util_max
    ), f"Utilization Prosed {new_min_utilization_required} is higher than max {util_max}"
    new_min_utilization_required = utilization_boundary(
        new_min_utilization_required, util_min, util_max
    )
    new_balance = market_container.trade_container_aggregator(year, "trade", region)
    plant_change_dict[region]["new_utilized_capacity"] = new_utilized_capacity
    plant_change_dict[region]["new_balance"] = new_balance
    plant_change_dict[region]["new_utilization"] = new_min_utilization_required
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict[region],
        cases,
        year,
        region,
        capacity,
        new_min_utilization_required,
        util_min,
        util_max,
    )
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
    potential_extra_production: float,
) -> Tuple[dict, dict, dict, float]:
    """Manages the case where a region can handle some of the import requirements.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.
        global_trade_balance (float): The current balance of trade across all regions.
        potential_extra_production (float): The potential extra production in a region that can be produced above their current production without increasing capacity.

    Returns:
        Tuple[dict, dict, dict, float]: A tuple containing the modified plant_change_dict, capacity_container, cases and global_trade_balance
    """
    cases[region].append("R2-B: moving partial import demand to region")
    capacity = capacity_container[region]
    market_container.assign_market_tuple(
        year,
        region,
        market_container.return_market_entry(0, 0, potential_extra_production),
    )
    total_production = market_container.trade_container_aggregator(
        year, "production", region
    )
    new_min_utilization_required = total_production / capacity
    trade_balance = market_container.trade_container_aggregator(year, "trade", region)
    plant_change_dict[region]["new_utilized_capacity"] = total_production
    plant_change_dict[region]["new_balance"] = trade_balance
    plant_change_dict[region]["new_utilization"] = new_min_utilization_required
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict[region],
        cases,
        year,
        region,
        capacity,
        new_min_utilization_required,
        util_min,
        util_max,
    )
    global_trade_balance += potential_extra_production
    return plant_change_dict, capacity_container, cases, global_trade_balance


def open_plants_cheapest_region(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    trade_status: TradeStatus,
    cases: dict,
    year: int,
    region: str,
    global_trade_balance: float,
    avg_plant_capacity_value: float,
    avg_plant_capacity_at_max_production: float,
    util_min: float,
    util_max: float,
) -> Tuple[dict, dict, dict, float]:
    """Manages the case where there is still insufficient production to meet import demand, so the solution is to open plants.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        trade_status (TradeStatus): The initial trade status of the region.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        global_trade_balance (float): The current balance of trade across all regions.
        avg_plant_capacity_value (float): The average plant capacity for all regions.
        avg_plant_capacity_at_max_production (float): The average plant capacity when production is maximised at util_max.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        Tuple[dict, dict, dict, float]: A tuple containing the modified plant_change_dict, capacity_container, cases and global_trade_balance.
    """
    cases[region].append("R3: moving remaining import demand to cheapest region")
    initial_capacity = capacity_container[region]
    demand = plant_change_dict[region]["demand"]
    value_to_add_to_global = abs(global_trade_balance)
    market_container.assign_market_tuple(
        year, region, market_container.return_market_entry(0, 0, value_to_add_to_global)
    )
    total_production = market_container.trade_container_aggregator(
        year, "production", region
    )
    new_capacity_required = total_production - (initial_capacity * util_max)
    open_plants_metadata = calculate_plants_to_open(
        initial_capacity,
        new_capacity_required,
        avg_plant_capacity_value,
        avg_plant_capacity_at_max_production,
        total_production,
        util_min,
        util_max,
    )
    mkt_balance = calculate_mkt_balance(
        open_plants_metadata.new_utilized_capacity, demand, trade_status
    )
    plant_change_dict[region]["new_capacity_required"] = new_capacity_required
    plant_change_dict[region][
        "plants_required"
    ] = open_plants_metadata.new_plants_required
    plant_change_dict[region][
        "new_total_capacity"
    ] = open_plants_metadata.new_total_capacity
    plant_change_dict[region][
        "new_utilized_capacity"
    ] = open_plants_metadata.new_utilized_capacity
    plant_change_dict[region]["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict[region][
        "new_utilization"
    ] = open_plants_metadata.new_min_utilization_required
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict[region],
        cases,
        year,
        region,
        open_plants_metadata.new_total_capacity,
        open_plants_metadata.new_min_utilization_required,
        util_min,
        util_max,
    )
    global_trade_balance = 0
    return plant_change_dict, capacity_container, cases, global_trade_balance


def close_plants_for_exporters(
    plant_change_dict: dict,
    market_container: MarketContainerClass,
    utilization_container: UtilizationContainerClass,
    capacity_container: dict,
    trade_status: TradeStatus,
    cases: dict,
    year: int,
    region: str,
    global_trade_balance: float,
    avg_plant_capacity_value: float,
    util_min: float,
    util_max: float,
) -> Tuple[dict, dict, dict, float]:
    """Manages the case where there is still excess production, so the solution is to close plants.

    Args:
        plant_change_dict (dict): A dictionary containing the data/meatadata for the region, created by create_plant_change_dict.
        market_container (MarketContainerClass): The MaterialUsage Instance containing the material usage state.
        utilization_container (UtilizationContainerClass): An instance of the UtilizationContainerClass.
        capacity_container (dict): A dictionary containing each region's capacity.
        trade_status (TradeStatus): The initial trade status of the region.
        cases (dict): A dictionary in the form Dict[region, cases list] containing records of each logic step a particular region has gone through.
        year (int): The current year of the model.
        region (str): The region to complete post_logic updates for.
        global_trade_balance (float): The current balance of trade across all regions.
        avg_plant_capacity_value (float): The average plant capacity for all regions.
        util_min (float): The minimum utilization value boundary.
        util_max (float): The maximum utilization value boundary.

    Returns:
        Tuple[dict, dict, dict, float]: A tuple containing the modified plant_change_dict, capacity_container, cases and global_trade_balance.
    """
    cases[region].append("R4-B: EXCESS SUPPLY, MIN UTILZATION -> close plants")
    capacity = capacity_container[region]
    demand = plant_change_dict[region]["demand"]
    region_trade_balance = market_container.trade_container_aggregator(
        year, "trade", region
    )
    trade_prodution_to_close = min(region_trade_balance, global_trade_balance)
    market_container.assign_market_tuple(
        year,
        region,
        market_container.return_market_entry(0, 0, -trade_prodution_to_close),
    )
    total_required_production = market_container.return_trade_balance(
        year, region, "production"
    )
    closed_plant_metadata = calculate_plants_to_close(
        region, capacity, total_required_production, avg_plant_capacity_value, util_min
    )
    assert closed_plant_metadata.new_capacity_required < 0
    mkt_balance = calculate_mkt_balance(
        closed_plant_metadata.new_utilized_capacity, demand, trade_status
    )
    assert round(total_required_production, TRADE_ROUNDING_NUMBER) == round(
        closed_plant_metadata.new_utilized_capacity, TRADE_ROUNDING_NUMBER
    ), f"Production Stats || region: {region} | container: {total_required_production} | dict: {closed_plant_metadata.new_utilized_capacity}"
    plant_change_dict[region]["plants_to_close"] = closed_plant_metadata.plants_to_close
    plant_change_dict[region][
        "new_capacity_required"
    ] = closed_plant_metadata.new_capacity_required
    plant_change_dict[region][
        "new_total_capacity"
    ] = closed_plant_metadata.new_total_capacity
    plant_change_dict[region][
        "new_utilized_capacity"
    ] = closed_plant_metadata.new_utilized_capacity
    plant_change_dict[region]["new_balance"] = mkt_balance.mkt_balance
    plant_change_dict[region][
        "new_utilization"
    ] = closed_plant_metadata.new_min_utilization_required
    capacity_container, cases = post_logic_updates(
        utilization_container,
        capacity_container,
        plant_change_dict[region],
        cases,
        year,
        region,
        closed_plant_metadata.new_total_capacity,
        closed_plant_metadata.new_min_utilization_required,
        util_min,
        util_max,
    )
    global_trade_balance -= trade_prodution_to_close
    return plant_change_dict, capacity_container, cases, global_trade_balance
