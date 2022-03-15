"""Module that determines functionality for opening and closing plants"""

import math
from typing import Tuple, Union

import pandas as pd
import numpy as np
from tqdm import tqdm

import random
import string
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

# Create logger
logger = get_logger("Plant Opening and Closing")

def replace_dict_items(base_dict: dict, repl_dict: dict): # GENERAL FUNCTION
    base_dict_c = deepcopy(base_dict)
    for col_entry in repl_dict:
        if col_entry in base_dict_c:
            base_dict_c[col_entry] = repl_dict[col_entry]
    return base_dict_c

def get_dict_keys_by_value(base_dict: dict, value): # GENERAL FUNCTION
    item_list = base_dict.items()
    return [item[0] for item in item_list if item[1] == value]


def pick_random_country_from_region(country_df: pd.DataFrame, region: str): # GENERAL FUNCTION
    country_list = get_countries_from_group(country_df, 'RMI Model Region', region)
    return random.choice(country_list)


def create_new_plant(plant_df: pd.DataFrame, plant_row_dict: dict):
    base_dict = {col: '' for col in plant_df.columns}
    base_dict = replace_dict_items(base_dict, plant_row_dict)
    plant_df_c = plant_df.copy()
    return plant_df_c.append(base_dict, ignore_index=True)


def agg_plant_capacity(plant_df: pd.DataFrame, avg: bool = False, region_string: str = 'rmi_region') -> float:
    """Returns the total capacity of all plants listed in the `plant_cap_dict` dictionary.

    Args:
        plant_cap_dict (dict): A dictionary containing plant: capacity/inital tech key:value pairs.

    Returns:
        float: Float value of the summation of all plant capacities using the `calculate_primary_and_secondary` function.
    """
    plant_df_c = plant_df.copy()
    plant_capacity_dict = create_plant_capacities_dict(plant_df_c)
    region_capacity_dict = {}
    regions = plant_df_c[region_string].unique()
    for region in regions:
        plants = plant_df_c[plant_df[region_string] == region]['plant_name']
        all_capacities = [
            calculate_primary_and_secondary(
                plant_capacity_dict, plant, plant_capacity_dict[plant]["2020_tech"]
            )
            for plant in plants
        ]
        capacities_list = [x for x in all_capacities if str(x) != "nan"]
        
        final_stat = sum(capacities_list)
        if avg:
            final_stat = final_stat / len(capacities_list)
        region_capacity_dict[region] = round(final_stat, 3)
        
    region_capacity_dict['Middle East'] = region_capacity_dict['Middle East'] + region_capacity_dict['Middle East ']
    region_capacity_dict.pop('Middle East ', None)
    return region_capacity_dict


def create_regional_plant_capacity_dict(plant_df: pd.DataFrame, as_avg: bool = False, rounding: int = 3):
    plant_df_c = plant_df.copy()
    df = plant_df_c[['rmi_region', 'total_capacity']].groupby(['rmi_region'])
    if as_avg:
        df = df.mean().round(rounding).reset_index()
    else:
        df = df.sum().round(rounding).reset_index()
    return dict(zip(df['rmi_region'], df['total_capacity']))


def regional_capacity_utilization_factor(supply_value: float, demand_value: float, rounding: bool = 3):
    if demand_value >= supply_value:
        return 1
    return round(supply_value / demand_value, rounding)


def supply_demand_gap(
    demand_df: pd.DataFrame, plant_df: pd.DataFrame, year: int, region: str = 'World', 
    demand_scenario: str = 'average', demand_metric: str = 'crude',
    rounding: int = 2, print_summary: bool = False, all_regions: bool = False, as_df: bool = False):
    
    # SUBSETTING & VALIDATION
    supply_dict_agg = agg_plant_capacity(plant_df)
    supply_dict_avg = agg_plant_capacity(plant_df, avg=True)
    valid_regions = list(demand_df['region'].unique())
    
    results_dict = {}
    
    region_list = [region,]
    if all_regions:
        region_list = valid_regions
        
    for region in region_list:
        
        if region in valid_regions:
            demand = steel_demand_getter(demand_df, year=year, scenario=demand_scenario, metric=demand_metric, region=region)
            if region == 'World':
                supply_agg = sum(supply_dict_agg.values())
                supply_avg = np.mean(list((supply_dict_avg.values())))
            else:
                supply_agg = supply_dict_agg[region]
                supply_avg = supply_dict_avg[region]
        else:
            raise AttributeError(f'Invalid region entered. You entered {region}. Possible options are: {valid_regions}')

        # FIX UNITS
        supply_agg = round(supply_agg / 1000, rounding)  # Initially Kt
        supply_avg = round(supply_avg / 1000, rounding)  # Initially Kt @ 100% utilization
        demand = round(demand, rounding) # Initially Mt
        balance_value = round(supply_agg - demand, rounding)
        new_plants_required = math.ceil(-balance_value / supply_avg)
        utilization = regional_capacity_utilization_factor(supply_agg, demand)

        # BALANCES
        balance_str = ''
        if balance_value < 0:
            balance_str = 'Undersupply'
        elif balance_value > 0:
            balance_str = 'Oversupply'
        else:
            balance_str = 'BALANCED'

        # PRINT RESULTS
        if print_summary:
            print(f'=============== RESULTS FOR {region} =================')
            print(f'{region} Demand: {demand} Mt')
            print(f'{region} Supply: {supply_agg} Mt')
            print(f'{balance_str} of {balance_value} Mt')
            print(f'Utilization Rate: {utilization}')
            print(f'Avg plant size in {region}: {supply_avg} Mt')
            print(f'Plants required in {region}: {new_plants_required}')
            
        # RETURN RESULTS
        results = {
            'year': year,
            'region': region,
            'supply_agg': supply_agg,
            'demand': demand,
            'balance': balance_value,
            'utilization_rate': utilization,
            'supply_avg': supply_avg,
            'plants_required': new_plants_required,
            'unit': 'Mt',
        }
        
        if not all_regions:
            return results
        
        results_dict[region] = results
        
    if as_df:
        return pd.DataFrame(results_dict.values()).set_index(['year', 'region'])

    return results_dict

def get_xcost_from_region(lcost_df: pd.DataFrame, year: int, region: str = None, value_type: str = 'min'):
    lcost_df_c = lcost_df.copy()
    
    if region:
        lcost_df_c.set_index(['year', 'region', 'technology'], inplace=True)
        lcost_df_c_s = lcost_df_c.loc[year, region]
        
    else:
        lcost_df_c.set_index(['year', 'region'], inplace=True)
        lcost_df_c_s = lcost_df_c.loc[year]
    
    if value_type == 'min':
        return lcost_df_c_s['levelised_cost'].idxmin()
    elif value_type == 'max':
        return lcost_df_c_s['levelised_cost'].idxmax()


def current_plant_year(inv_dict: pd.DataFrame, plant: str, current_year: int, last_inv_year: int = None, cycle_length: int = 20):
    main_cycle_years = [yr for yr in inv_dict[plant] if isinstance(yr, int)]
    first_inv_year = main_cycle_years[0]
    if len(main_cycle_years) == 2:
        second_inv_year = main_cycle_years[1]
        cycle_length = second_inv_year - first_inv_year
    
    trans_years = [yr for yr in inv_dict[plant] if isinstance(yr, range)]
    if trans_years:
        first_trans_years = list(trans_years[0])
        potential_start_date = first_trans_years[0]
        if current_year in first_trans_years:
            return current_year - potential_start_date

    if current_year < first_inv_year:
        potential_start_date = first_inv_year - cycle_length
        return current_year - potential_start_date
        
    if len(main_cycle_years) == 1:
        if current_year >= first_inv_year:
            return current_year - first_inv_year

    if len(main_cycle_years) == 2:
        if current_year >= first_inv_year <= second_inv_year:
            if not trans_years:
                return current_year - first_inv_year
            else:
                if current_year in first_trans_years:
                    return current_year - potential_start_date
                else:
                    return current_year - second_inv_year
        elif current_year > second_inv_year:
            return current_year - second_inv_year


def new_plant_metadata(
    plant_container: PlantIdContainer, plant_df: pd.DataFrame,
    levelized_cost_df: pd.DataFrame, country_df: pd.DataFrame, ng_mapper: dict,
    year: int, region: str = None, low_cost_region: bool = False):

    reg_cap_avg = create_regional_plant_capacity_dict(plant_df, as_avg=True)
    if region and low_cost_region:
        raise AttributeError('You entered a value for `region` and set `low_cost_region` to true. Select ONE or the other, NOT both.')
    new_id = plant_container.generate_plant_id(add_to_container=True)
    if low_cost_region:
        region = get_xcost_from_region(levelized_cost_df, year=year, value_type='min')
    regional_capacity = reg_cap_avg[region]
    country_specific_mapper = {'China': 'CHN', 'India': 'IND'}
    if region in country_specific_mapper:
        assigned_country = country_specific_mapper[region]
    else:
        assigned_country = pick_random_country_from_region(country_df, region)
    return {
        'plant_id': new_id,
        'plant_name': f'New Capacity Plant {new_id}',
        'parent': 'New Plant Construct',
        'status': f'operating {year}',
        'start_of_operation': year,
        'country_code': assigned_country,
        'cheap_natural_gas': ng_mapper[assigned_country],
        'total_capacity': regional_capacity,
        'rmi_region': region
    }

def current_plant_year(inv_dict: pd.DataFrame, plant: str, current_year: int, last_inv_year: int = None, cycle_length: int = 20):
    main_cycle_years = [yr for yr in inv_dict[plant] if isinstance(yr, int)]
    first_inv_year = main_cycle_years[0]
    if len(main_cycle_years) == 2:
        second_inv_year = main_cycle_years[1]
        cycle_length = second_inv_year - first_inv_year

    trans_years = [yr for yr in inv_dict[plant] if isinstance(yr, range)]
    if trans_years:
        first_trans_years = list(trans_years[0])
        potential_start_date = first_trans_years[0]
        if current_year in first_trans_years:
            return current_year - potential_start_date

    if current_year < first_inv_year:
        potential_start_date = first_inv_year - cycle_length
        return current_year - potential_start_date
        
    if len(main_cycle_years) == 1:
        if current_year >= first_inv_year:
            return current_year - first_inv_year

    if len(main_cycle_years) == 2:
        if current_year >= first_inv_year <= second_inv_year:
            if not trans_years:
                return current_year - first_inv_year
            else:
                if current_year in first_trans_years:
                    return current_year - potential_start_date
                else:
                    return current_year - second_inv_year
        elif current_year > second_inv_year:
            return current_year - second_inv_year


def return_oldest_plant(inv_dict: dict, current_year: int, plant_list: list = None):
    if not plant_list:
        plant_list = inv_dict.keys()
    plant_age_dict = {plant: current_plant_year(inv_dict, plant, current_year) for plant in plant_list}
    max_value = max(plant_age_dict.values())
    return random.choice(get_dict_keys_by_value(plant_age_dict, max_value))


def return_plants_from_region(plant_df: pd.DataFrame, region: str):
    return list(plant_df[plant_df['rmi_region'] == region]['plant_name'].values)


def tc_dict_editor(tc_dict: dict, year: int, plant: str, tech: str):
    tc_dict_c = deepcopy(tc_dict)
    tc_dict_c[str(year)][plant] = tech
    return tc_dict_c

def update_sd_df(df: pd.DataFrame, high_rate: float, low_rate: float):
    df_c = df.copy()
    
    def util_mapper(row):
        if row['balance'] > 0:
            return low_rate
        elif row['balance'] <= 0:
            return high_rate
    df_c['desired_utilization'] = df_c.apply(util_mapper, axis=1)
    
    def plant_required_mapper(row):
        return math.ceil(row['plants_required'] * row['desired_utilization'])
    df_c['updated_plants_required'] = df_c.apply(plant_required_mapper, axis=1)

    return df_c


def return_modified_plants(ocp_df: pd.DataFrame, year: int, change_type: str = 'open'):
    change_mapper = {'open': f'operating {year}', 'close': 'decomissioned'}
    return ocp_df[ocp_df['status'].str.contains(change_mapper[change_type])]


def ng_flag_mapper(plant_df: pd.DataFrame, country_ref: pd.DataFrame):
    df = plant_df[['country_code', 'cheap_natural_gas']].drop_duplicates().reset_index(drop=True).copy()
    initial_mapper = dict(zip(df['country_code'], df['cheap_natural_gas']))
    final_values = country_ref['country_code'].apply(
        lambda x: initial_mapper.get(x, 0)
    )
    return dict(zip(country_ref['country_code'], final_values))


def return_capacity_value(total_capacity: float, tech: str, return_type: str):
    if tech == 'EAF':
        if return_type == 'secondary':
            return total_capacity
        return 0
    elif return_type == 'secondary':
        return 0
    return total_capacity


def open_close_plants(
    demand_df: pd.DataFrame, steel_plant_df: pd.DataFrame, lev_cost_df: pd.DataFrame, country_df: pd.DataFrame,
    inv_dict: dict, tc_dict_ref: dict, ng_mapper: dict, plant_id_container: PlantIdContainer, year: int, 
    open_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    close_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION
):
    steel_plant_df_c = steel_plant_df.copy()
    tc_dict_ref_c = deepcopy(tc_dict_ref)

    # YEAR LOOP
    supply_demand_df = supply_demand_gap(demand_df, steel_plant_df, year, all_regions=True, as_df=True)
    supply_demand_df = update_sd_df(supply_demand_df, open_plant_util_cutoff, close_plant_util_cutoff)
    regions = list(supply_demand_df.index.get_level_values(1).unique())
    regions.remove('World')

    # REGION LOOP
    for region in tqdm(regions, total=len(regions), desc=f'Open Close Plants for {year}'):
        sd_df_c = supply_demand_df.loc[year, region].copy()
        plants_required = sd_df_c['updated_plants_required']

        # OPEN PLANT
        if plants_required > 0:
            for _ in range(plants_required):
                new_plant_meta = new_plant_metadata(plant_id_container, steel_plant_df_c, lev_cost_df, country_df, ng_mapper, year=year, region=region)
                steel_plant_df_c = create_new_plant(steel_plant_df_c, new_plant_meta)
                xcost_tech = get_xcost_from_region(lev_cost_df, year=year, region=region, value_type='min')
                idx_open = steel_plant_df_c.index[steel_plant_df_c['plant_id'] == new_plant_meta['plant_id']].tolist()[0]
                steel_plant_df_c.loc[idx_open, 'primary_capacity_2020'] = return_capacity_value(new_plant_meta['total_capacity'], xcost_tech, 'primary') 
                steel_plant_df_c.loc[idx_open, 'secondary_capacity_2020'] = return_capacity_value(new_plant_meta['total_capacity'], xcost_tech, 'secondary')
                steel_plant_df_c.loc[idx_open, 'technology_in_2020'] = xcost_tech
                tc_dict_ref_c = tc_dict_editor(tc_dict_ref_c, year, new_plant_meta['plant_name'], xcost_tech)
        
        # CLOSE PLANT
        if plants_required < 0:
            closed_list = []
            for _ in range(abs(plants_required)):
                plant_list = return_plants_from_region(steel_plant_df, region)
                plant_to_close = return_oldest_plant(inv_dict, year, plant_list)
                idx_close = steel_plant_df_c.index[steel_plant_df_c['plant_name'] == plant_to_close].tolist()[0]
                steel_plant_df_c.loc[idx_close, 'status'] = f'decomissioned ({year})'
                if plant_to_close in closed_list:
                    pass
                else:
                    tc_dict_ref_c = tc_dict_editor(tc_dict_ref_c, year, plant_to_close, 'Close plant')
    return {'tech_choice_dict': tc_dict_ref_c, 'plant_df': steel_plant_df_c}


def open_close_flow(plant_container: PlantIdContainer, plant_df: pd.DataFrame, tech_choice_dict: dict, investment_dict: dict, year: int) -> str:
    logger.info(f'Running open close decisions for {year}')
    country_df = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref", "df")
    country_df_f = country_df_formatter(country_df)
    steel_plants_processed = read_pickle_folder(PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df")
    ng_mapper = ng_flag_mapper(steel_plants_processed, country_df_f)
    regional_steel_demand_formatted = read_pickle_folder(PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted", "df")
    country_reference_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "country_reference_dict", "df")
    levelized_cost = read_pickle_folder(PKL_DATA_INTERMEDIATE, "levelized_cost", "df")
    levelized_cost["region"] = levelized_cost["country_code"].apply(
            lambda x: get_region_from_country_code(x, "rmi_region", country_reference_dict)
    )
    
    return open_close_plants(
        regional_steel_demand_formatted, plant_df, levelized_cost, 
        country_df, investment_dict, tech_choice_dict, ng_mapper, plant_container, year
    )
