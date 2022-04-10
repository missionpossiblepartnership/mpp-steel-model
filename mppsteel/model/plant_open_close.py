"""Module that determines functionality for opening and closing plants"""

import math

import pandas as pd
from tqdm import tqdm

import random
from copy import deepcopy
from mppsteel.config.reference_lists import SWITCH_DICT
from mppsteel.model.solver_constraints import tech_availability_check

from mppsteel.utility.location_utility import get_countries_from_group
from mppsteel.utility.plant_container_class import PlantIdContainer
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter

from mppsteel.config.model_config import (
    MAIN_REGIONAL_SCHEMA,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
)

from mppsteel.model.solver_classes import (
    CapacityContainerClass, UtilizationContainerClass, MarketContainerClass,
    PlantChoices, MaterialUsage, apply_constraints_for_min_cost_tech
)
from mppsteel.model.trade import trade_flow

from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)

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
    country_list = plant_df[plant_df[MAIN_REGIONAL_SCHEMA] == region]['country_code'].unique()
    return random.choice(country_list)


def create_new_plant(plant_df: pd.DataFrame, plant_row_dict: dict):
    base_dict = {col: '' for col in plant_df.columns}
    base_dict = replace_dict_items(base_dict, plant_row_dict)
    plant_df_c = plant_df.copy()
    return plant_df_c.append(base_dict, ignore_index=True)


def get_min_cost_tech_for_region(
    lcost_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    tech_availability: pd.DataFrame,
    material_container: MaterialUsage,
    tech_moratorium: bool,
    year: int,
    region: str,
    plant_capacity: float,
    plant_name: str,
    with_constraints: bool = False
):  
    lcost_df_c = lcost_df.loc[year, region].groupby('technology').mean().copy()

    if with_constraints:
        potential_technologies = apply_constraints_for_min_cost_tech(
            business_cases, 
            tech_availability, 
            material_container, 
            SWITCH_DICT.keys(), 
            plant_capacity,
            tech_moratorium, 
            year, 
            plant_name
        )
        lcost_df_c = lcost_df_c[lcost_df_c.index.isin(potential_technologies)]

    return lcost_df_c['levelised_cost'].idxmin()

def get_min_cost_region(lcost_df: pd.DataFrame, year: int):
    lcost_df_c = lcost_df.loc[year].groupby('region').mean().copy()
    return lcost_df_c['levelised_cost'].idxmin()


def current_plant_year(investment_dict: pd.DataFrame, plant: str, current_year: int, cycle_length: int = 20):
    main_cycle_years = [yr for yr in investment_dict[plant] if isinstance(yr, int)]
    first_inv_year = main_cycle_years[0]
    if len(main_cycle_years) == 2:
        second_inv_year = main_cycle_years[1]
        cycle_length = second_inv_year - first_inv_year
    
    trans_years = [yr for yr in investment_dict[plant] if isinstance(yr, range)]
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
    levelized_cost_df: pd.DataFrame, plant_df: pd.DataFrame, ng_mapper: dict,
    year: int, region: str = None, low_cost_region: bool = False):

    if region and low_cost_region:
        raise AttributeError('You entered a value for `region` and set `low_cost_region` to true. Select ONE or the other, NOT both.')
    new_id = plant_container.generate_plant_id(add_to_container=True)
    if low_cost_region:
        region = get_min_cost_region(levelized_cost_df, year=year)
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
        MAIN_REGIONAL_SCHEMA: region
    }

def return_oldest_plant(investment_dict: dict, current_year: int, plant_list: list = None):
    if not plant_list:
        plant_list = investment_dict.keys()
    plant_age_dict = {plant: current_plant_year(investment_dict, plant, current_year) for plant in plant_list}
    max_value = max(plant_age_dict.values())
    return random.choice(get_dict_keys_by_value(plant_age_dict, max_value))


def return_plants_from_region(plant_df: pd.DataFrame, region: str):
    return list(plant_df[plant_df[MAIN_REGIONAL_SCHEMA] == region]['plant_name'].values)


def add_new_plant_choices(tech_choices_container, year: int, plant: str, tech: str):
    tech_choices_container.update_choices(year, plant, tech)


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
    demand_df: pd.DataFrame,
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    market_container: MarketContainerClass,
    year: int, steel_demand_scenario: str,
    regional_capacity: bool = False,
    all_regions: bool = False, 
    capacity_util_max: float = 0.95,
    capacity_util_min: float = 0.6):
    
    logger.info(f'Defining the production demand gap for {year}')
    # SUBSETTING & VALIDATION
    avg_plant_global_capacity = capacity_container.return_avg_capacity_value(year, 'avg')

    region_list = list(capacity_container.regional_capacities_agg[year].keys()) if all_regions else ['World',]
    results_container = []

    for region in region_list:
        initial_utilization = utilization_container.get_utilization_values(year, region)
        demand = steel_demand_getter(
            demand_df, year=year, scenario=steel_demand_scenario, metric='crude', region=region)
        if region == 'World':
            capacity_agg = capacity_container.get_world_capacity_sum(year)
        else:
            capacity_agg = capacity_container.return_regional_capacity(year, region, 'agg')
            avg_plant_region_capacity = capacity_container.return_regional_capacity(year, region, 'avg')

        avg_plant_capacity_value = avg_plant_global_capacity
        if regional_capacity:
            avg_plant_capacity_value = avg_plant_region_capacity

        new_capacity_required = 0
        excess_capacity = 0
        new_plants_required = 0
        plants_to_close = 0
        new_total_capacity = 0
        current_capacity = deepcopy(capacity_agg)

        initial_min_utilization_reqiured = demand / capacity_agg
        new_min_utilization_required = 0
        
        if capacity_util_min <= initial_min_utilization_reqiured <= capacity_util_max:
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
        
        utilization_container.update_region(year, region, new_min_utilization_required)
        regional_capacities = capacity_container.return_regional_capacity(year)
        utilization_container.calculate_world_utilization(year, regional_capacities)

        initial_utilized_capacity = capacity_agg * initial_utilization
        new_utilized_capacity = new_total_capacity * new_min_utilization_required
        initial_balance_value = initial_utilized_capacity - demand
        new_balance_value = new_utilized_capacity - demand

        # RETURN RESULTS
        region_result = {
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
            return region_result

        results_container.append(region_result)

    results_container_df = pd.DataFrame(results_container).set_index(['year', 'region']).round(3)
    market_container.store_results(year, results_container_df)

    return results_container_df

def open_close_plants(
    demand_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    lev_cost_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    tech_availability: pd.DataFrame,
    variable_costs_df: pd.DataFrame,
    capex_dict: dict,
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    material_container: MaterialUsage,
    tech_choices_container: PlantChoices,
    plant_id_container: PlantIdContainer,
    market_container: MarketContainerClass,
    investment_dict: dict,
    ng_mapper: dict,
    year: int,
    trade_scenario: bool = False,
    steel_demand_scenario: bool = False,
    tech_moratorium: bool = False,
    open_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    close_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
):

    logger.info(f'Iterating through the open close loops for {year}')

    production_demand_gap_analysis = production_demand_gap(
        demand_df=demand_df,
        capacity_container=capacity_container,
        utilization_container=utilization_container,
        market_container=market_container,
        year=year,
        steel_demand_scenario=steel_demand_scenario,
        all_regions=True,
        capacity_util_max=open_plant_util_cutoff,
        capacity_util_min=close_plant_util_cutoff
    )
    steel_plant_df_c = steel_plant_df.copy()
    tech_choices_ref = tech_choices_container.return_choices()

    if trade_scenario:
        logger.info(f"Starting the trade flow for {year}")
        production_demand_gap_analysis = trade_flow(
            market_container=market_container,
            production_demand_df=production_demand_gap_analysis,
            utilization_container=utilization_container,
            capacity_container=capacity_container,
            variable_cost_df=variable_costs_df,
            plant_df=steel_plant_df_c,
            capex_dict=capex_dict,
            tech_choices_ref=tech_choices_ref,
            year=year,
            util_min=close_plant_util_cutoff,
            util_max=open_plant_util_cutoff
        )

    regions = list(production_demand_gap_analysis.index.get_level_values(1).unique())

    levelised_cost_for_regions = lev_cost_df.set_index(['year', 'region']).sort_index(ascending=True).copy()
    levelised_cost_for_tech = lev_cost_df.set_index(['year', 'region', 'technology']).sort_index(ascending=True).copy()

    # REGION LOOP
    for region in tqdm(regions, total=len(regions), desc=f'Open Close Plants for {year}'):
        sd_df_c = production_demand_gap_analysis.loc[year, region].copy()
        plants_required = sd_df_c['plants_required']
        plants_to_close = sd_df_c['plants_to_close']

        # OPEN PLANT
        if plants_required > 0:
            for _ in range(plants_required):
                new_plant_meta = new_plant_metadata(
                    plant_id_container, production_demand_gap_analysis, 
                    levelised_cost_for_regions, steel_plant_df_c, ng_mapper, year=year, region=region
                )
                new_plant_capacity = new_plant_meta['plant_capacity']
                new_plant_name = new_plant_meta['plant_name']
                steel_plant_df_c = create_new_plant(steel_plant_df_c, new_plant_meta)
                xcost_tech = get_min_cost_tech_for_region(
                    levelised_cost_for_tech,
                    business_cases,
                    tech_availability,
                    material_container,
                    tech_moratorium,
                    year,
                    region,
                    new_plant_capacity,
                    new_plant_name,
                    with_constraints=True,
                )
                idx_open = steel_plant_df_c.index[steel_plant_df_c['plant_id'] == new_plant_meta['plant_id']].tolist()[0]
                steel_plant_df_c.loc[idx_open, 'plant_capacity'] = new_plant_meta['plant_capacity']
                steel_plant_df_c.loc[idx_open, 'technology_in_2020'] = xcost_tech
                add_new_plant_choices(tech_choices_container, year, new_plant_name, xcost_tech)

        # CLOSE PLANT
        if plants_to_close > 0:
            closed_list = []
            for _ in range(abs(plants_required)):
                plant_list = return_plants_from_region(steel_plant_df, region)
                # define cos function
                # rank descending
                # filter for age < 11 years
                # pick highest
                plant_to_close = return_oldest_plant(investment_dict, year, plant_list)
                idx_close = steel_plant_df_c.index[steel_plant_df_c['plant_name'] == plant_to_close].tolist()[0]
                steel_plant_df_c.loc[idx_close, 'status'] = f'decomissioned ({year})'
                if plant_to_close in closed_list:
                    pass
                else:
                    add_new_plant_choices(tech_choices_container, year, plant_to_close, 'Close plant')

    return steel_plant_df_c


def open_close_flow(
    plant_container: PlantIdContainer,
    market_container: MarketContainerClass,
    plant_df: pd.DataFrame,
    levelized_cost: pd.DataFrame,
    steel_demand_df: pd.DataFrame,
    country_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    tech_availability: pd.DataFrame,
    variable_costs_df: pd.DataFrame,
    capex_dict: dict,
    tech_choices_container: PlantChoices,
    investment_dict: dict,
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    material_container: MaterialUsage,
    year: int,
    trade_scenario: bool,
    steel_demand_scenario: str,
    tech_moratorium: bool,
) -> str:
    
    logger.info(f'Running open close decisions for {year}')
    ng_mapper = ng_flag_mapper(plant_df, country_df)
    
    return open_close_plants(
        demand_df=steel_demand_df,
        steel_plant_df=plant_df,
        lev_cost_df=levelized_cost,
        business_cases=business_cases,
        tech_availability=tech_availability,
        variable_costs_df=variable_costs_df,
        capex_dict=capex_dict,
        capacity_container=capacity_container,
        utilization_container=utilization_container,
        material_container=material_container,
        investment_dict=investment_dict,
        tech_choices_container=tech_choices_container,
        ng_mapper=ng_mapper,
        plant_id_container=plant_container,
        market_container=market_container,
        year=year,
        trade_scenario=trade_scenario,
        steel_demand_scenario=steel_demand_scenario,
        tech_moratorium=tech_moratorium
    )
