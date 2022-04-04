"""Module that determines functionality for opening and closing plants"""

import math

import pandas as pd
import numpy as np
from tqdm import tqdm

import random
from copy import deepcopy

from mppsteel.utility.location_utility import get_countries_from_group
from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.utility.plant_container_class import PlantIdContainer
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter

from mppsteel.config.model_config import (
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
)

from mppsteel.data_loading.steel_plant_formatter import (
    create_plant_capacities_dict
)
from mppsteel.model.trade import TradeBalance, trade_flow, get_xcost_from_region

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

def pick_random_country_from_region_subset(plant_df: pd.DataFrame, region: str): # GENERAL FUNCTION
    country_list = plant_df[plant_df['rmi_region'] == region]['country_code'].unique()
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
        float: Float value of the summation of all plant capacities.
    """
    plant_df_c = plant_df.copy()
    region_capacity_dict = {}
    regions = plant_df_c[region_string].unique()
    for region in regions:
        plant_capacities = plant_df_c[plant_df[region_string] == region]['plant_capacity']
        final_stat = plant_capacities.sum()
        if avg:
            final_stat = plant_capacities.mean()
        region_capacity_dict[region] = round(final_stat, 3)
        
    region_capacity_dict['Middle East'] = region_capacity_dict['Middle East']
    return region_capacity_dict


def create_plant_capacity_dict(plant_df: pd.DataFrame, as_avg: bool = False, rounding: int = 3, as_mt: bool = False):
    logger.info('Deriving plant capacity statistics')
    plant_df_c = plant_df.copy()
    df = plant_df_c[['rmi_region', 'plant_capacity']].groupby(['rmi_region'])
    if as_avg:
        df = df.mean().round(rounding).reset_index()
    else:
        df = df.sum().round(rounding).reset_index()
    dict_obj = dict(zip(df['rmi_region'], df['plant_capacity']))
    if as_mt:
        return {region: value / 1000 for region, value in dict_obj.items()}
    return dict_obj


def regional_capacity_utilization_factor(capacity_value: float, demand_value: float, rounding: bool = 3):
    if demand_value >= capacity_value:
        return 1
    return round(capacity_value / demand_value, rounding)


def current_plant_year(inv_dict: pd.DataFrame, plant: str, current_year: int, cycle_length: int = 20):
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
    plant_container: PlantIdContainer, gap_analysis_df: pd.DataFrame,
    levelized_cost_df: pd.DataFrame, country_df: pd.DataFrame, plant_df: pd.DataFrame, ng_mapper: dict,
    year: int, region: str = None, low_cost_region: bool = False):

    if region and low_cost_region:
        raise AttributeError('You entered a value for `region` and set `low_cost_region` to true. Select ONE or the other, NOT both.')
    new_id = plant_container.generate_plant_id(add_to_container=True)
    if low_cost_region:
        region = get_xcost_from_region(levelized_cost_df, year=year, value_type='min')
    capacity_value = gap_analysis_df.loc[year, region]['avg_plant_capacity'] * 1000
    country_specific_mapper = {'China': 'CHN', 'India': 'IND'}
    if region in country_specific_mapper:
        assigned_country = country_specific_mapper[region]
    else:
        assigned_country = pick_random_country_from_region_subset(plant_df, region)
    return {
        'plant_id': new_id,
        'plant_name': f'{new_id} - {assigned_country}',
        'status': f'operating {year}',
        'start_of_operation': year,
        'country_code': assigned_country,
        'cheap_natural_gas': ng_mapper[assigned_country],
        'plant_capacity': capacity_value,
        'rmi_region': region
    }



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


def production_demand_gap(
    demand_df: pd.DataFrame, plant_df: pd.DataFrame, util_dict: dict, year: int, steel_demand_scenario: str,
    regional_capacity: bool = False, rounding: int = 3, print_summary: bool = False, all_regions: bool = False, 
    as_df: bool = False, capacity_util_max: float = 0.95, capacity_util_min: float = 0.6):
    
    logger.info(f'Defining the production demand gap for {year}')
    util_dict_c = deepcopy(util_dict)
    # SUBSETTING & VALIDATION
    capacity_dict_agg = create_plant_capacity_dict(plant_df, as_mt=True)
    capacity_dict_avg = create_plant_capacity_dict(plant_df, as_avg=True, as_mt=True)
    avg_plant_global_capacity = np.mean(list(capacity_dict_avg.values()))
    util_dict_c['World'] = np.mean(list(util_dict_c.values()))

    valid_regions = list(demand_df['region'].unique())
    results_container = {}
    region_list = ['World',]
    if all_regions:
        region_list = valid_regions

    for region in region_list:
        if region in valid_regions:
            initial_utilization = util_dict_c[region]
            demand = steel_demand_getter(
                demand_df, year=year, scenario=steel_demand_scenario, metric='crude', region=region)
            if region == 'World':
                capacity_agg = sum(list(capacity_dict_agg.values()))
            else:
                capacity_agg = capacity_agg = capacity_dict_agg[region]
                avg_plant_region_capacity = capacity_dict_avg[region]

        else:
            raise AttributeError(f'Invalid region entered. You entered {region}. Possible options are: {valid_regions}')
    
        avg_plant_capacity_value = avg_plant_global_capacity
        if regional_capacity:
            avg_plant_capacity_value = avg_plant_region_capacity

        new_capacity_required = 0
        excess_capacity = 0
        new_plants_required = 0
        plants_to_close = 0
        current_capacity = deepcopy(capacity_agg)
        new_total_capacity = 0

        initial_min_utilization_reqiured = demand / capacity_agg
        new_min_utilization_required = 0
        
        if capacity_util_min < initial_min_utilization_reqiured < capacity_util_max:
            new_total_capacity = current_capacity
            new_min_utilization_required = initial_min_utilization_reqiured

        if initial_min_utilization_reqiured < capacity_util_min:
            excess_capacity = (capacity_agg * capacity_util_min) - demand
            plants_to_close = math.ceil(excess_capacity / avg_plant_capacity_value)
            new_total_capacity = current_capacity - (plants_to_close * avg_plant_capacity_value)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = max(new_min_utilization_required, capacity_util_min)

        if initial_min_utilization_reqiured > capacity_util_max:
            new_capacity_required = demand - (capacity_agg * capacity_util_max)
            new_plants_required = math.ceil(new_capacity_required / avg_plant_capacity_value)
            new_total_capacity = current_capacity + (new_plants_required * avg_plant_capacity_value)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = min(new_min_utilization_required, capacity_util_max)
        
        util_dict_c[region] = new_min_utilization_required
        util_dict_c['World'] = round(np.mean(list(util_dict_c.values())), rounding)
        initial_utilized_capacity = capacity_agg * initial_utilization
        new_utilized_capacity = new_total_capacity * new_min_utilization_required
        initial_balance_value = initial_utilized_capacity - demand
        new_balance_value = new_utilized_capacity - demand

        # PRINT RESULTS
        if print_summary:
            print(f'=============== RESULTS FOR {region} =================')
            print(f'{region} Demand: {demand} Mt')
            print(f'{region} Initial Production: {initial_utilized_capacity} Mt')
            print(f'Initial Utilization Rate: {initial_utilization}')
            print(f'Initial balance: {initial_balance_value} Mt')
            print(f'Avg. plant capacity in {region}: {avg_plant_capacity_value} Mt')
            print(f'Plants required in {region}: {new_plants_required}')
            print(f'Plants to close in {region}: {plants_to_close}')
            print(f'Total new capacity in {region}: {new_total_capacity}')
            print(f'New utilization rate in {region}: {new_min_utilization_required}')
            print(f'New Production {region}: {new_utilized_capacity}')
            print(f'New balance {region}: {new_balance_value}')
            

        # RETURN RESULTS
        results = {
            'year': year,
            'region': region,
            'capacity': capacity_agg,
            'initial_utilized_capacity': initial_utilized_capacity,
            'demand': demand,
            'initial_balance': initial_balance_value,
            'initial_utilization': initial_utilization,
            'avg_plant_capacity': avg_plant_capacity_value,
            'new_capacity_required': new_capacity_required,
            'plants_required': new_plants_required,
            'plants_to_close': plants_to_close,
            'new_total_capacity': new_total_capacity,
            'new_utilized_capacity': new_utilized_capacity,
            'new_balance': new_balance_value,
            'new_utilization': new_min_utilization_required,
            'unit': 'Mt',
        }

        if not all_regions:
            return results

        results_container[region] = results
        

    if as_df:
        results_container = pd.DataFrame(results_container.values()).set_index(['year', 'region']).round(rounding)

    return {'results': results_container, 'utilization_dict': util_dict_c}

def open_close_plants(
    demand_df: pd.DataFrame, steel_plant_df: pd.DataFrame, 
    lev_cost_df: pd.DataFrame, country_df: pd.DataFrame,
    variable_costs_df: pd.DataFrame, capex_dict: dict,
    util_dict: dict, inv_dict: dict, tc_dict_ref: dict, 
    ng_mapper: dict, plant_id_container: PlantIdContainer, 
    trade_container: TradeBalance, year: int,
    trade_scenario: bool = False, steel_demand_scenario: bool = False,
    open_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    close_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
):

    logger.info(f'Iterating through the open close loops for {year}')
    steel_plant_df_c = steel_plant_df.copy()
    tc_dict_ref_c = deepcopy(tc_dict_ref)
    util_dict_c = deepcopy(util_dict)

    # YEAR LOOP
    gap_analysis = production_demand_gap(
        demand_df=demand_df,
        plant_df=steel_plant_df_c,
        util_dict=util_dict_c,
        year=year,
        steel_demand_scenario=steel_demand_scenario,
        all_regions=True,
        as_df=True,
        capacity_util_max=open_plant_util_cutoff,
        capacity_util_min=close_plant_util_cutoff
    )
    gap_analysis_df = gap_analysis['results']
    util_dict_c = gap_analysis['utilization_dict']

    if trade_scenario:
        logger.info(f"Starting the trade flow for {year}")
        trade_output = trade_flow(
            trade_container=trade_container,
            production_demand_df=gap_analysis_df,
            util_dict=util_dict_c,
            variable_cost_df=variable_costs_df,
            plant_df=steel_plant_df_c,
            capex_dict=capex_dict,
            tech_choices=tc_dict_ref,
            year=year,
            util_min=close_plant_util_cutoff,
            util_max=open_plant_util_cutoff
        )
        gap_analysis_df = trade_output['production_demand_df']
        util_dict_c = trade_output['util_dict']

    regions = list(gap_analysis_df.index.get_level_values(1).unique())
    regions.remove('World')

    # REGION LOOP
    for region in tqdm(regions, total=len(regions), desc=f'Open Close Plants for {year}'):
        sd_df_c = gap_analysis_df.loc[year, region].copy()
        plants_required = sd_df_c['plants_required']
        plants_to_close = sd_df_c['plants_to_close']

        # OPEN PLANT
        if plants_required > 0:
            for _ in range(plants_required):
                new_plant_meta = new_plant_metadata(plant_id_container, gap_analysis_df, lev_cost_df, country_df, steel_plant_df_c, ng_mapper, year=year, region=region)
                steel_plant_df_c = create_new_plant(steel_plant_df_c, new_plant_meta)
                xcost_tech = get_xcost_from_region(lev_cost_df, year=year, region=region, value_type='min')
                idx_open = steel_plant_df_c.index[steel_plant_df_c['plant_id'] == new_plant_meta['plant_id']].tolist()[0]
                steel_plant_df_c.loc[idx_open, 'plant_capacity'] = new_plant_meta['plant_capacity']
                steel_plant_df_c.loc[idx_open, 'technology_in_2020'] = xcost_tech
                tc_dict_ref_c = tc_dict_editor(tc_dict_ref_c, year, new_plant_meta['plant_name'], xcost_tech)

        # CLOSE PLANT
        if plants_to_close > 0:
            closed_list = []
            for _ in range(abs(plants_required)):
                plant_list = return_plants_from_region(steel_plant_df, region)
                # define cos function
                # rank descending
                # filter for age < 11 years
                # pick highest
                plant_to_close = return_oldest_plant(inv_dict, year, plant_list)
                idx_close = steel_plant_df_c.index[steel_plant_df_c['plant_name'] == plant_to_close].tolist()[0]
                steel_plant_df_c.loc[idx_close, 'status'] = f'decomissioned ({year})'
                if plant_to_close in closed_list:
                    pass
                else:
                    tc_dict_ref_c = tc_dict_editor(tc_dict_ref_c, year, plant_to_close, 'Close plant')

    return {'tech_choice_dict': tc_dict_ref_c, 'plant_df': steel_plant_df_c, 'util_dict': util_dict_c}


def format_wsa_production_data(df, as_dict: bool = False):
    logger.info('Formatting WSA production data for 2020')
    df_c = df.copy()
    df_c = df_c.melt(id_vars=['WSA_Region','RMI_Region','Country','Metric','Unit'], var_name='year')
    df_c = df_c[df_c['year'] == 2020]
    df_c.columns = [col.lower() for col in df_c.columns]
    df_c = df_c.groupby(['rmi_region', 'year']).sum().reset_index()
    if as_dict:
        return dict(zip(df_c['rmi_region'], df_c['value']))
    return df_c

def return_utilization(prod_dict: dict, cap_dict: dict, value_cap: float = None):
    util_dict = {}
    for region in prod_dict:
        val = round(prod_dict[region] / cap_dict[region], 2)
        if value_cap:
            val = min(val, value_cap)
        util_dict[region] = val
    return util_dict

def create_wsa_2020_utilization_dict():
    logger.info('Creating the utilization dictionary for 2020.')
    wsa_production = read_pickle_folder(PKL_DATA_IMPORTS, "wsa_production", "df")
    steel_plants_processed = read_pickle_folder(PKL_DATA_FORMATTED, "steel_plants_processed", "df")
    wsa_2020_production_dict = format_wsa_production_data(wsa_production, as_dict=True)
    capacity_dict = create_plant_capacity_dict(steel_plants_processed, as_mt=True)
    return return_utilization(wsa_2020_production_dict, capacity_dict, value_cap=1)

def open_close_flow(
    plant_container: PlantIdContainer, trade_container: TradeBalance, 
    plant_df: pd.DataFrame, levelized_cost: pd.DataFrame, steel_demand_df: pd.DataFrame,
    country_df: pd.DataFrame, variable_costs_df: pd.DataFrame, 
    capex_dict: dict, tech_choice_dict: dict, investment_dict: dict, 
    util_dict: dict, year: int, trade_scenario: bool, steel_demand_scenario: str) -> str:
    
    logger.info(f'Running open close decisions for {year}')
    ng_mapper = ng_flag_mapper(plant_df, country_df)
    
    return open_close_plants(
        demand_df=steel_demand_df,
        steel_plant_df=plant_df,
        lev_cost_df=levelized_cost,
        country_df=country_df,
        variable_costs_df=variable_costs_df,
        capex_dict=capex_dict,
        util_dict=util_dict,
        inv_dict=investment_dict,
        tc_dict_ref=tech_choice_dict,
        ng_mapper=ng_mapper,
        plant_id_container=plant_container,
        trade_container=trade_container,
        year=year,
        trade_scenario=trade_scenario,
        steel_demand_scenario=steel_demand_scenario
    )
