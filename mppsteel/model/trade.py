"""Module that contains the trade functions"""

import math
from copy import deepcopy

import pandas as pd
import numpy as np

from copy import deepcopy

from mppsteel.utility.location_utility import get_region_from_country_code, get_countries_from_group
from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.utility.plant_container_class import PlantIdContainer
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.data_loading.country_reference import country_df_formatter

from mppsteel.config.model_config import (
    PKL_DATA_IMPORTS,
    PKL_DATA_INTERMEDIATE,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
)

from mppsteel.data_loading.steel_plant_formatter import (
    calculate_primary_and_secondary, create_plant_capacities_dict
)

from mppsteel.utility.log_utility import get_logger

logger = get_logger("Trade Functions")

def get_xcost_from_region(lcost_df: pd.DataFrame, year: int, region: str = None, value_type: str = 'min', return_type: str = 'value'):
    lcost_df_c = lcost_df.copy()
    
    if region:
        lcost_df_c.set_index(['year', 'region', 'technology'], inplace=True)
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

def check_relative_production_cost(lcox_df: pd.DataFrame, year: int, pct_boundary: float = 0.1):
    df_c = lcox_df.groupby(['year', 'region']).mean().loc[year].sort_values(by='levelised_cost', ascending=True).copy()
    mean_val = df_c['levelised_cost'].mean()
    value_range = df_c['levelised_cost'].max() - df_c['levelised_cost'].min()
    value_range_boundary = value_range * pct_boundary
    upper_boundary = mean_val + value_range_boundary
    df_c['relative_cost_below_avg'] = df_c['levelised_cost'].apply(lambda x: True if x <= mean_val else False)
    df_c[f'relative_cost_close_to_mean'] = df_c['levelised_cost'].apply(lambda x: True if x < upper_boundary else False)
    return df_c


class TradeBalance:
    def __init__(self):
        self.trade_container = {}
        
    def __repr__(self):
        return "Trade Container"
    
    def __str__(self):
        return "Trade Container Class"
    
    def initiate_years(self, year_start: int, year_end: int):
        self.trade_container = {year: {} for year in range(year_start, year_end)}
        
    def initiate_regions(self, region_list: list):
        for year in self.trade_container:
            self.trade_container[year] = {region: 0 for region in region_list}
    
    def return_container(self):
        return self.trade_container
    
    def full_instantiation(self, year_start: int, year_end: int, region_list: list):
        self.initiate_years(year_start, year_end)
        self.initiate_regions(region_list)
    
    def trade_container_getter(self, year: int, region: str, agg: bool = False):
        if year and not region:
            if agg:
                return self.trade_container[year]
            return sum(list(self.trade_container[year].values()))
        if year and region:
            return self.trade_container[year][region]
        
    def assign_trade_balance(self, year: int, region: str, value: float):
        self.trade_container[year][region] = value
        
    def output_trade_to_df(self):
        df = pd.DataFrame(self.trade_container).reset_index().melt(id_vars=['index'], var_name='year', value_name='trade_balance')
        df.rename({'index': 'region'}, axis=1, inplace=True)
        return df.set_index(['year', 'region'])

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
    trade_container: TradeBalance, production_demand_df: pd.DataFrame, util_dict: dict,
    levelized_cost_df: pd.DataFrame, year: int, util_min: float, util_max: float,
):
    production_demand_df_c = production_demand_df.copy()
    # change to cos!!!!!!
    relative_production_cost_df = check_relative_production_cost(levelized_cost_df, year)
    # don't need to order this
    region_list = get_xcost_from_region(levelized_cost_df, year, value_type='max', return_type='list')
    util_dict_c = deepcopy(util_dict)

    for region in region_list:
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
            trade_container.assign_trade_balance(year, region, trade_balance)
            data_entry_dict_values = [0, 0, 0, capacity, capacity * utilization, 0, utilization]
            util_dict_c[region] = utilization

        elif (trade_balance > 0) and not relative_cost_below_avg and (utilization > util_min):
            new_min_utilization_required = demand / capacity
            # new flow if new_min_utilization_required < util_min
            new_min_utilization_required = max(new_min_utilization_required, util_min)
            new_utilized_capacity = capacity * new_min_utilization_required
            new_balance = new_utilized_capacity - demand
            data_entry_dict_values = [0, 0, 0, capacity, new_utilized_capacity, 0, new_min_utilization_required]
            util_dict_c[region] = new_min_utilization_required

        elif (trade_balance > 0) and not relative_cost_below_avg and (utilization <= util_min):
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
            # need flow to to increase if new_balance > 0
            # import if costly region else build plant
            data_entry_dict_values = [0, 0, 0, capacity, new_utilized_capacity, 0, new_min_utilization_required]
            util_dict_c[region] = new_min_utilization_required

        elif (trade_balance < 0) and (utilization >= util_max) and relative_cost_below_mean:
            new_capacity_required = demand - (capacity * util_max)
            new_plants_required = math.ceil(new_capacity_required / avg_plant_capacity)
            new_total_capacity = new_total_capacity + (new_plants_required * avg_plant_capacity)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = min(new_min_utilization_required, util_max)
            new_utilized_capacity = new_min_utilization_required * capacity
            data_entry_dict_values = [new_capacity_required, new_plants_required, 0, new_total_capacity, new_utilized_capacity, 0, new_min_utilization_required]
            util_dict_c[region] = new_min_utilization_required

        elif (trade_balance < 0) and (utilization >= util_max) and not relative_cost_below_mean:
            trade_container.assign_trade_balance(year, region, trade_balance)
            data_entry_dict_values = [0, 0, 0, capacity, capacity * utilization, 0, utilization]
            util_dict_c[region] = utilization

        production_demand_df_c = modify_prod_df(production_demand_df_c, data_entry_dict_values, year, region)
        
    
    util_dict_c['World'] = np.mean(list(util_dict_c.values()))

    return {'trade_container': trade_container, 'production_demand_df': production_demand_df_c, 'util_dict': util_dict_c}
