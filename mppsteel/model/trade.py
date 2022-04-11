"""Module that contains the trade functions"""

import math
from copy import deepcopy

import pandas as pd

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.config.model_config import (
    MAIN_REGIONAL_SCHEMA,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    RELATIVE_REGIONAL_COST_BOUNDARY_FROM_MEAN_PCT
)
from mppsteel.model.solver_classes import (
    CapacityContainerClass, UtilizationContainerClass, MarketContainerClass
)
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def check_relative_production_cost(cost_df: pd.DataFrame, value_col: str, pct_boundary: float):
    df_c = cost_df.copy()
    mean_val = df_c[value_col].mean()
    value_range = df_c[value_col].max() - df_c[value_col].min()
    value_range_boundary = value_range * pct_boundary
    upper_boundary = mean_val + value_range_boundary
    df_c['relative_cost_below_avg'] = df_c[value_col].apply(lambda x: True if x <= mean_val else False)
    df_c[f'relative_cost_close_to_mean'] = df_c[value_col].apply(lambda x: True if x < upper_boundary else False)
    return df_c


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
    return 0 if utilization_rate == 0 else (plant_capacity * utilization_rate * variable_cost) + other_opex_cost

def create_cos_table(
    row, year: int, production_demand_dict: dict, v_costs: pd.DataFrame, 
    capacity_dict: dict, tech_choices: dict, capex_costs: dict):

    
    technology = ''
    if year == 2020:
        technology = tech_choices[str(year)][row.plant_name]
    else:
        technology = tech_choices[str(year - 1)][row.plant_name]
    plant_capacity = capacity_dict[row.plant_name]
    utilization_rate = production_demand_dict[row.rmi_region]['initial_utilization']
    variable_cost = v_costs.loc[row.country_code, year, technology]["cost"]
    other_opex_cost = capex_costs["other_opex"].loc[technology, year]["value"]
    return single_year_cos(plant_capacity, utilization_rate, variable_cost, other_opex_cost)

def calculate_cos(
    plant_df: pd.DataFrame, year: int, production_demand_dict: dict, 
    v_costs: pd.DataFrame, tech_choices: dict, capex_costs: dict,
    capacity_dict: dict):

    plant_df_c = plant_df.copy()
    tqdma.pandas(desc="Cost of Steelmaking (Single Year)")
    plant_df_c['cost_of_steelmaking'] = plant_df_c.progress_apply(
        create_cos_table, 
        year=year, 
        production_demand_dict=production_demand_dict, 
        v_costs=v_costs, 
        capacity_dict=capacity_dict, 
        tech_choices=tech_choices, 
        capex_costs=capex_costs, 
        axis=1
    )
    return plant_df_c[[MAIN_REGIONAL_SCHEMA, 'cost_of_steelmaking']].groupby([MAIN_REGIONAL_SCHEMA]).mean().sort_values(by='cost_of_steelmaking', ascending=True).copy()


DATA_ENTRY_DICT_KEYS = [
    'new_capacity_required',
    'plants_required',
    'plants_to_close',
    'new_total_capacity',
    'new_utilized_capacity',
    'new_balance',
    'new_utilization',
]

def modify_production_demand_dict(production_demand_dict: dict, data_entry_values: list, region: str):
    production_demand_dict_c = deepcopy(production_demand_dict)
    data_entry_dict = dict(zip(DATA_ENTRY_DICT_KEYS, data_entry_values))
    
    for dict_key in [
        'new_capacity_required', 'plants_required',
        'plants_to_close', 'new_total_capacity',
        'new_utilized_capacity', 'new_balance', 
        'new_utilization']:
        production_demand_dict_c[region][dict_key] = data_entry_dict[dict_key]
    return production_demand_dict_c


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
    relative_boundary_from_mean: float = RELATIVE_REGIONAL_COST_BOUNDARY_FROM_MEAN_PCT
):
    capacity_dict = capacity_container.return_plant_capacity(year)
    production_demand_dict_c = deepcopy(production_demand_dict)
    cos_df = calculate_cos(plant_df, year, production_demand_dict_c, variable_cost_df, tech_choices_ref, capex_dict, capacity_dict)
    relative_production_cost_df = check_relative_production_cost(cos_df, 'cost_of_steelmaking', relative_boundary_from_mean)

    region_list = list(plant_df[MAIN_REGIONAL_SCHEMA].unique())

    for region in tqdm(region_list, total=len(region_list), desc=f'Trade Region Flow for {year}'):
        trade_balance = production_demand_dict_c[region]['initial_balance']
        utilization = utilization_container.get_utilization_values(year, region)
        demand = production_demand_dict_c[region]['demand']
        capacity = production_demand_dict_c[region]['capacity']
        avg_plant_capacity = production_demand_dict_c[region]['avg_plant_capacity']
        relative_cost_below_avg = relative_production_cost_df.loc[region]['relative_cost_below_avg']
        relative_cost_below_mean = relative_production_cost_df.loc[region]['relative_cost_close_to_mean']

        # COL ORDER FOR LIST
        # 'new_capacity_required', 'plants_required', 'plants_to_close', 
        # 'new_total_capacity', 'new_utilized_capacity', 'new_balance', 'new_utilization'
        data_entry_dict_values = None
        if (trade_balance > 0) and relative_cost_below_avg:
            # CHEAP EXCESS SUPPLY -> export
            market_container.assign_trade_balance(year, region, trade_balance)
            data_entry_dict_values = [0, 0, 0, capacity, capacity * utilization, 0, utilization]
            utilization_container.update_region(year, region, utilization)

        elif (trade_balance > 0) and not relative_cost_below_avg and (utilization > util_min):
            # EXPENSIVE EXCESS SUPPLY -> reduce utilization if possible
            new_min_utilization_required = demand / capacity
            new_utilized_capacity = capacity * new_min_utilization_required
            data_entry_dict_values = [0, 0, 0, capacity, new_utilized_capacity, 0, new_min_utilization_required]
            utilization_container.update_region(year, region, new_min_utilization_required)

        elif (trade_balance > 0) and not relative_cost_below_avg and (utilization <= util_min):
            # EXPENSIVE EXCESS SUPPLY -> close plant
            excess_capacity = (capacity * util_min) - demand
            plants_to_close = math.ceil(excess_capacity / avg_plant_capacity)
            new_total_capacity = capacity - (plants_to_close * avg_plant_capacity)
            new_min_utilization_required = demand / new_total_capacity
            new_utilized_capacity = new_min_utilization_required * new_total_capacity
            data_entry_dict_values = [0, 0, plants_to_close, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
            utilization_container.update_region(year, region, new_min_utilization_required)

        elif (trade_balance < 0) and (utilization < util_max):
            # INSUFFICIENT SUPPLY -> increase utilization (test)
            min_utilization_reqiured = demand / capacity
            new_min_utilization_required = min(min_utilization_reqiured, util_max)
            new_utilized_capacity = new_min_utilization_required * capacity
            new_balance = new_utilized_capacity - demand
            if (new_balance < 0) and not relative_cost_below_avg:
                # STILL INSUFFICIENT SUPPLY
                # EXPENSIVE REGION -> import
                market_container.assign_trade_balance(year, region, trade_balance)
                data_entry_dict_values = [0, 0, 0, capacity, new_utilized_capacity, 0, new_min_utilization_required]
            elif (new_balance < 0) and relative_cost_below_avg:
                # CHEAP REGION -> import
                new_plants_required = math.ceil(-new_balance / avg_plant_capacity)
                new_total_capacity = capacity + (new_plants_required * avg_plant_capacity)
                new_min_utilization_required = demand / new_total_capacity
                new_min_utilization_required = min(new_min_utilization_required, util_max)
                new_utilized_capacity = new_min_utilization_required * capacity
                data_entry_dict_values = [-new_balance, new_plants_required, 0, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
            else:
                # SUFFICIENT SUPPLY -> increase utilization
                data_entry_dict_values = [0, 0, 0, capacity, new_utilized_capacity, 0, new_min_utilization_required]
            utilization_container.update_region(year, region, new_min_utilization_required)

        elif (trade_balance < 0) and (utilization >= util_max) and relative_cost_below_mean:
            # INSUFFICIENT SUPPLY, CHEAP REGION, MAX UTILIZATION -> open plants
            new_capacity_required = demand - (capacity * util_max)
            new_plants_required = math.ceil(new_capacity_required / avg_plant_capacity)
            new_total_capacity = capacity + (new_plants_required * avg_plant_capacity)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = min(new_min_utilization_required, util_max)
            new_utilized_capacity = new_min_utilization_required * capacity
            data_entry_dict_values = [new_capacity_required, new_plants_required, 0, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
            new_min_utilization_required
            utilization_container.update_region(year, region, new_min_utilization_required)

        elif (trade_balance < 0) and (utilization >= util_max) and not relative_cost_below_mean:
            # INSUFFICIENT SUPPLY, EXPENSIVE REGION, MAX UTILIZATION -> import
            market_container.assign_trade_balance(year, region, trade_balance)
            data_entry_dict_values = [0, 0, 0, capacity, capacity * utilization, 0, utilization]
            utilization_container.update_region(year, region, utilization)
        
        if data_entry_dict_values:
            # APPLY FINAL CHANGE, ELSE NO CHANGE
            production_demand_dict_c = modify_production_demand_dict(production_demand_dict_c, data_entry_dict_values, region)

    global_trade_balance = round(market_container.trade_container_getter(year), 3)
    if global_trade_balance > 0:
        logger.info(f'Trade Balance Surplus of {global_trade_balance} Mt in year {year}. No balancing to zero.')
    elif global_trade_balance < 0:
        logger.info(f'Trade Balance Deficit of {global_trade_balance} Mt in year {year}, balancing to zero.')
        rpc_df = relative_production_cost_df[relative_production_cost_df['relative_cost_close_to_mean'] == True].sort_values(['cost_of_steelmaking'], ascending=True)
        while global_trade_balance < 0:
            for region in rpc_df.index:
                # increase utilization
                current_utilization = utilization_container.get_utilization_values(year, region)
                total_capacity = production_demand_dict_c[region]['new_total_capacity']
                current_utilized_capacity = production_demand_dict_c[region]['new_utilized_capacity']
                potential_extra_production = (util_max - current_utilization) * total_capacity
                # Instantiate capacity and utilization
                new_utilized_capacity = 0
                new_utilization = 0
                if potential_extra_production > abs(global_trade_balance):
                    # fulfill all unmet demand
                    new_utilized_capacity = abs(global_trade_balance) + current_utilized_capacity
                    new_utilization = new_utilized_capacity / total_capacity
                    data_entry_dict_values = [0, 0, 0, total_capacity, new_utilized_capacity, 0, new_utilization]
                    utilization_container.update_region(year, region, new_utilization)
                    global_trade_balance = 0
                else:
                    # fulfill partial unmet demand
                    new_utilized_capacity = potential_extra_production + current_utilized_capacity
                    new_utilization = new_utilized_capacity / total_capacity
                    data_entry_dict_values = [0, 0, 0, total_capacity, new_utilized_capacity, 0, new_utilization]
                    utilization_container.update_region(year, region, new_utilization)
                    global_trade_balance = global_trade_balance + potential_extra_production
                
                production_demand_dict_c = modify_production_demand_dict(production_demand_dict_c, data_entry_dict_values, region)
            
            if global_trade_balance < 0:
                # build new plant
                cheapest_region = rpc_df['cost_of_steelmaking'].idxmin()
                capacity = production_demand_dict_c[region]['new_total_capacity']
                new_capacity_required = abs(global_trade_balance)
                new_plants_required = math.ceil(new_capacity_required / avg_plant_capacity)
                new_total_capacity = capacity + (new_plants_required * avg_plant_capacity)
                new_min_utilization_required = min(demand / new_total_capacity, util_max)
                new_utilized_capacity = new_min_utilization_required * new_total_capacity
                data_entry_dict_values = [new_capacity_required, new_plants_required, 0, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
                utilization_container.update_region(year, cheapest_region, new_min_utilization_required)
                production_demand_dict_c = modify_production_demand_dict(production_demand_dict_c, data_entry_dict_values, cheapest_region)
                global_trade_balance = 0

    elif global_trade_balance == 0:
        logger.info(f'Trade Balance is completely balanced at {global_trade_balance} Mt in year {year}')

    return production_demand_dict_c
