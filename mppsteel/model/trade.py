"""Module that contains the trade functions"""

import math
from copy import deepcopy

import pandas as pd
import numpy as np

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    MAIN_REGIONAL_SCHEMA,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    RELATIVE_REGIONAL_COST_BOUNDARY_FROM_MEAN_PCT
)

from mppsteel.utility.log_utility import get_logger
from mppsteel.data_loading.steel_plant_formatter import create_plant_capacities_dict

logger = get_logger(__name__)

def get_xcost_from_region(lcost_df: pd.DataFrame, year: int, region: str = None, value_type: str = 'min', return_type: str = 'value'):
    lcost_df_c = lcost_df.copy()
    
    if region:
        lcost_df_c.set_index(['year', 'region', 'technology'], inplace=True)
        lcost_df_c.sort_values(['year', 'region', 'technology'], ascending=True, inplace=True)
        lcost_df_c_s = lcost_df_c.loc[year, region]
        
    else:
        lcost_df_c.set_index(['year', 'region'], inplace=True)
        lcost_df_c_s = lcost_df_c.loc[year]
    
    if (value_type == 'min') & (return_type == 'value'):
        return lcost_df_c_s['levelised_cost'].idxmin()
    elif (value_type == 'max') & (return_type == 'value'):
        return lcost_df_c_s['levelised_cost'].idxmax()
    if (value_type == 'min') & (return_type == 'list'):
        return lcost_df_c_s['levelised_cost'].sort_values(by='levelised_cost', ascending=True).index.tolist()
    elif (value_type == 'max') & (return_type == 'list'):
        return lcost_df_c_s['levelised_cost'].sort_values(by='levelised_cost', ascending=False).index.tolist()

def get_regional_balance(prod_balance_df: pd.DataFrame, year: int, region: str):
    return prod_balance_df.loc[year, region]['initial_utilization']

def get_utilization(prod_balance_df: pd.DataFrame, year: int, region: str):
    return prod_balance_df.loc[year, region]['initial_balance']

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
    if utilization_rate == 0:
        return 0

    return plant_capacity * (
            (variable_cost * utilization_rate) + other_opex_cost
        )

def create_cos_table(
    row, year: int, production_demand_df: pd.DataFrame, v_costs: pd.DataFrame, 
    capacity_dict: dict, tech_choices: dict, capex_costs: dict):

    plant_capacity = capacity_dict[row.plant_name]
    technology = ''
    if year == 2020:
        technology = tech_choices[str(year)][row.plant_name]
    else:
        technology = tech_choices[str(year - 1)][row.plant_name]
    utilization_rate = get_utilization(production_demand_df, year, row.rmi_region)
    variable_cost = v_costs.loc[row.country_code, year, technology]["cost"]
    other_opex_cost = capex_costs["other_opex"].loc[technology, year]["value"]
    return single_year_cos(plant_capacity, utilization_rate, variable_cost, other_opex_cost)

def calculate_cos(
    plant_df: pd.DataFrame, year: int, production_demand_df: pd.DataFrame, 
    v_costs: pd.DataFrame, tech_choices: dict, capex_costs: dict):

    plant_df_c = plant_df.copy()
    capacity_dict = create_plant_capacities_dict(plant_df_c)
    tqdma.pandas(desc="Cost of Steelmaking (Single Year)")
    plant_df_c['cost_of_steelmaking'] = plant_df_c.progress_apply(
        create_cos_table, 
        year=year, 
        production_demand_df=production_demand_df, 
        v_costs=v_costs, 
        capacity_dict=capacity_dict, 
        tech_choices=tech_choices, 
        capex_costs=capex_costs, 
        axis=1
    )
    return plant_df_c[[MAIN_REGIONAL_SCHEMA, 'cost_of_steelmaking']].groupby([MAIN_REGIONAL_SCHEMA]).mean().sort_values(by='cost_of_steelmaking', ascending=True).copy()


class TradeBalance:
    def __init__(self):
        self.trade_container = {}
        self.results = []
        
    def __repr__(self):
        return "Trade Container"
    
    def __str__(self):
        return "Trade Container Class"
    
    def initiate_years(self, year_range: range):
        self.trade_container = {year: {} for year in year_range}
        
    def initiate_regions(self, region_list: list):
        for year in self.trade_container:
            self.trade_container[year] = {region: 0 for region in region_list}
    
    def return_container(self):
        return self.trade_container
    
    def full_instantiation(self, year_range: range, region_list: list):
        self.initiate_years(year_range)
        self.initiate_regions(region_list)
    
    def trade_container_getter(self, year: int, region: str = None, agg: bool = False):
        if year and not region:
            if agg:
                return self.trade_container[year]
            return sum(list(self.trade_container[year].values()))
        if year and region:
            return self.trade_container[year][region]
        
    def assign_trade_balance(self, year: int, region: str, value: float):
        self.trade_container[year][region] = value
        
    def output_trade_summary_to_df(self):
        df = pd.DataFrame(self.trade_container).reset_index().melt(id_vars=['index'], var_name='year', value_name='trade_balance')
        df.rename({'index': 'region'}, axis=1, inplace=True)
        return df.set_index(['year', 'region'])

    def record_results(self, results_df: pd.DataFrame):
        self.results.append(results_df)

    def output_trade_calculations_to_df(self):
        return pd.concat(self.results, axis=1)

DATA_ENTRY_DICT_KEYS = [
    'new_capacity_required',
    'plants_required',
    'plants_to_close',
    'new_total_capacity',
    'new_utilized_capacity',
    'new_balance',
    'new_utilization',
]

def modify_prod_df(df: pd.DataFrame, data_entry_values: list, year: int, region: str):
    df_c = df.copy()
    data_entry_dict = dict(zip(DATA_ENTRY_DICT_KEYS, data_entry_values))
    df_c.loc[(year, region), 'new_capacity_required'] = data_entry_dict['new_capacity_required']
    df_c.loc[(year, region), 'plants_required'] = data_entry_dict['plants_required']
    df_c.loc[(year, region), 'plants_to_close'] = data_entry_dict['plants_to_close']
    df_c.loc[(year, region), 'new_total_capacity'] = data_entry_dict['new_total_capacity']
    df_c.loc[(year, region), 'new_utilized_capacity'] = data_entry_dict['new_utilized_capacity']
    df_c.loc[(year, region), 'new_balance'] = data_entry_dict['new_balance']
    df_c.loc[(year, region), 'new_utilization'] = data_entry_dict['new_utilization']
    return df_c


def trade_flow(
    trade_container: TradeBalance, 
    production_demand_df: pd.DataFrame, 
    util_dict: dict,
    variable_cost_df: pd.DataFrame,
    plant_df: pd.DataFrame,
    capex_dict: dict,
    tech_choices_ref: dict,
    year: int, 
    util_min: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION, 
    util_max: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    relative_boundary_from_mean: float = RELATIVE_REGIONAL_COST_BOUNDARY_FROM_MEAN_PCT
):
    production_demand_df_c = production_demand_df.copy()
    cos_df = calculate_cos(plant_df, year, production_demand_df, variable_cost_df, tech_choices_ref, capex_dict)
    relative_production_cost_df = check_relative_production_cost(cos_df, 'cost_of_steelmaking', relative_boundary_from_mean)

    region_list = list(plant_df[MAIN_REGIONAL_SCHEMA].unique())
    util_dict_c = deepcopy(util_dict)

    for region in tqdm(region_list, total=len(region_list), desc=f'Trade Flow for {year}'):
        trade_balance = get_regional_balance(production_demand_df_c, year, region)
        utilization = get_utilization(production_demand_df_c, year, region)
        demand = production_demand_df_c.loc[(year, region), 'demand']
        capacity = production_demand_df_c.loc[(year, region), 'capacity']
        avg_plant_capacity = production_demand_df_c.loc[(year, region), 'avg_plant_capacity']
        relative_cost_below_avg = relative_production_cost_df.loc[region]['relative_cost_below_avg']
        relative_cost_below_mean = relative_production_cost_df.loc[region]['relative_cost_close_to_mean']

        # COL ORDER FOR LIST
        # 'new_capacity_required', 'plants_required', 'plants_to_close', 
        # 'new_total_capacity', 'new_utilized_capacity', 'new_balance', 'new_utilization'

        if (trade_balance > 0) and relative_cost_below_avg:
            # export
            trade_container.assign_trade_balance(year, region, trade_balance)
            data_entry_dict_values = [0, 0, 0, capacity, capacity * utilization, 0, utilization]
            util_dict_c[region] = utilization

        elif (trade_balance > 0) and not relative_cost_below_avg and (utilization > util_min):
            # reduce utilization
            new_min_utilization_required = demand / capacity
            new_utilized_capacity = capacity * new_min_utilization_required
            data_entry_dict_values = [0, 0, 0, capacity, new_utilized_capacity, 0, new_min_utilization_required]
            util_dict_c[region] = new_min_utilization_required

        elif (trade_balance > 0) and not relative_cost_below_avg and (utilization <= util_min):
            # close plant(s)
            excess_capacity = (capacity * util_min) - demand
            plants_to_close = math.ceil(excess_capacity / avg_plant_capacity)
            new_total_capacity = capacity - (plants_to_close * avg_plant_capacity)
            new_min_utilization_required = demand / new_total_capacity
            new_utilized_capacity = new_min_utilization_required * new_total_capacity
            data_entry_dict_values = [0, 0, plants_to_close, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
            util_dict_c[region] = new_min_utilization_required

        elif (trade_balance < 0) and (utilization < util_max):
            min_utilization_reqiured = demand / capacity
            new_min_utilization_required = min(min_utilization_reqiured, util_max)
            new_utilized_capacity = new_min_utilization_required * capacity
            new_balance = new_utilized_capacity - demand
            if (new_balance < 0) and not relative_cost_below_avg:
                # if still not enough after utilization has increased
                # import
                trade_container.assign_trade_balance(year, region, trade_balance)
                data_entry_dict_values = [0, 0, 0, capacity, new_utilized_capacity, 0, new_min_utilization_required]
            elif (new_balance < 0) and relative_cost_below_avg:
                # build plant(s)
                new_plants_required = math.ceil(-new_balance / avg_plant_capacity)
                new_total_capacity = capacity + (new_plants_required * avg_plant_capacity)
                new_min_utilization_required = demand / new_total_capacity
                new_min_utilization_required = min(new_min_utilization_required, util_max)
                new_utilized_capacity = new_min_utilization_required * capacity
                data_entry_dict_values = [-new_balance, new_plants_required, 0, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
            else:
                # just increase utilization
                data_entry_dict_values = [0, 0, 0, capacity, new_utilized_capacity, 0, new_min_utilization_required]
            util_dict_c[region] = new_min_utilization_required

        elif (trade_balance < 0) and (utilization >= util_max) and relative_cost_below_mean:
            # build plant(s)
            new_capacity_required = demand - (capacity * util_max)
            new_plants_required = math.ceil(new_capacity_required / avg_plant_capacity)
            new_total_capacity = new_total_capacity + (new_plants_required * avg_plant_capacity)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = min(new_min_utilization_required, util_max)
            new_utilized_capacity = new_min_utilization_required * capacity
            data_entry_dict_values = [new_capacity_required, new_plants_required, 0, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
            util_dict_c[region] = new_min_utilization_required

        elif (trade_balance < 0) and (utilization >= util_max) and not relative_cost_below_mean:
            # import
            trade_container.assign_trade_balance(year, region, trade_balance)
            data_entry_dict_values = [0, 0, 0, capacity, capacity * utilization, 0, utilization]
            util_dict_c[region] = utilization

        production_demand_df_c = modify_prod_df(production_demand_df_c, data_entry_dict_values, year, region)

    global_trade_balance = round(trade_container.trade_container_getter(year), 3)
    if global_trade_balance > 0:
        logger.info(f'Trade Balance Surplus of {global_trade_balance} Mt in year {year}')
    elif global_trade_balance < 0:
        logger.info(f'Trade Balance Deficit of {global_trade_balance} Mt in year {year}')
        rpc_df = relative_production_cost_df[relative_production_cost_df['relative_cost_close_to_mean'] == True].sort_values(['cost_of_steelmaking'], ascending=True)
        while global_trade_balance < 0:
            for region in rpc_df.index:
                # increase utilization
                current_utilization = production_demand_df_c.loc[(year, region), 'new_utilization']
                total_capacity = production_demand_df_c.loc[(year, region), 'new_total_capacity']
                current_utilized_capacity = production_demand_df_c.loc[(year, region), 'new_utilized_capacity']
                potential_extra_production = (util_max - current_utilization) * total_capacity
                new_utilized_capacity = 0
                new_utilization = 0
                if potential_extra_production > abs(global_trade_balance):
                    # fulfill all unmet demand
                    new_utilized_capacity = abs(global_trade_balance) + current_utilized_capacity
                    new_utilization = new_utilized_capacity / total_capacity
                    data_entry_dict_values = [0, 0, 0, total_capacity, new_utilized_capacity, 0, new_utilization]
                    util_dict_c[region] = new_utilization
                    global_trade_balance = 0
                else:
                    # fulfill partial unmet demand
                    new_utilized_capacity = potential_extra_production + current_utilized_capacity
                    new_utilization = new_utilized_capacity / total_capacity
                    data_entry_dict_values = [0, 0, 0, total_capacity, new_utilized_capacity, 0, new_utilization]
                    util_dict_c[region] = new_utilization
                    global_trade_balance = global_trade_balance + potential_extra_production
                
                production_demand_df_c = modify_prod_df(production_demand_df_c, data_entry_dict_values, year, region)
            
            if global_trade_balance < 0:
                # build new plant
                cheapest_region = rpc_df['cost_of_steelmaking'].idxmin()
                total_capacity = production_demand_df_c.loc[(year, cheapest_region), 'new_total_capacity']
                new_capacity_required = demand - (total_capacity * util_max)
                new_plants_required = math.ceil(new_capacity_required / avg_plant_capacity)
                new_total_capacity = new_total_capacity + (new_plants_required * avg_plant_capacity)
                new_min_utilization_required = demand / new_total_capacity
                new_min_utilization_required = min(new_min_utilization_required, util_max)
                new_utilized_capacity = new_min_utilization_required * capacity
                data_entry_dict_values = [new_capacity_required, new_plants_required, 0, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
                util_dict_c[region] = new_min_utilization_required
                production_demand_df_c = modify_prod_df(production_demand_df_c, data_entry_dict_values, year, cheapest_region)
                global_trade_balance = 0

    elif global_trade_balance == 0:
        logger.info(f'Trade Balance is completely balanced at {global_trade_balance} Mt in year {year}')

    util_dict_c['World'] = np.mean(list(util_dict_c.values()))

    return {'trade_container': trade_container, 'production_demand_df': production_demand_df_c, 'util_dict': util_dict_c}
