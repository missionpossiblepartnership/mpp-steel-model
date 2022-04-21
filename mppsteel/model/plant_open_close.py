"""Module that determines functionality for opening and closing plants"""

import math

import pandas as pd

import random
from copy import deepcopy
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST
from mppsteel.model.investment_cycles import PlantInvestmentCycle

from mppsteel.utility.location_utility import get_countries_from_group
from mppsteel.utility.plant_container_class import PlantIdContainer
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter

from mppsteel.config.model_config import (
    MAIN_REGIONAL_SCHEMA,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    MEGATON_TO_KILOTON_FACTOR,
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


def create_new_plant(plant_row_dict: dict, plant_columns: list):
    base_dict = {col: '' for col in plant_columns}
    return replace_dict_items(base_dict, plant_row_dict)


def get_min_cost_tech_for_region(
    lcost_df: pd.DataFrame,
    business_case_ref: dict,
    tech_availability: pd.DataFrame,
    material_container: MaterialUsage,
    tech_moratorium: bool,
    year: int,
    region: str,
    plant_capacity: float,
    plant_name: str,
    enforce_constraints: bool = False
):  
    lcost_df_c = lcost_df.loc[year, region].groupby('technology').mean().copy()

    if enforce_constraints:
        potential_technologies = apply_constraints_for_min_cost_tech(
            business_case_ref, 
            tech_availability, 
            material_container, 
            TECH_REFERENCE_LIST, 
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
        if first_trans_years:
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
    plant_container: PlantIdContainer, production_demand_dict: dict,
    levelized_cost_df: pd.DataFrame, plant_df: pd.DataFrame, ng_mapper: dict,
    year: int, region: str = None, low_cost_region: bool = False):

    if region and low_cost_region:
        raise AttributeError('You entered a value for `region` and set `low_cost_region` to true. Select ONE or the other, NOT both.')
    new_id = plant_container.generate_plant_id(add_to_container=True)
    if low_cost_region:
        region = get_min_cost_region(levelized_cost_df, year=year)
    capacity_value = production_demand_dict[region]['avg_plant_capacity']
    country_specific_mapper = {'China': 'CHN', 'India': 'IND'}
    if region in country_specific_mapper:
        assigned_country = country_specific_mapper[region]
    else:
        assigned_country = pick_random_country_from_region_subset(plant_df, region)
    return {
        'plant_id': new_id,
        'plant_name': f'{new_id} - {assigned_country}',
        'status': 'new model plant',
        'active_check': True,
        'start_of_operation': year,
        'country_code': assigned_country,
        'cheap_natural_gas': ng_mapper[assigned_country],
        'plant_capacity': capacity_value,
        'primary': 'Y',
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
    if change_type == 'open':
        return ocp_df[(ocp_df['status'] == 'new model plant') & (ocp_df['start_of_operation'] == year)]
    elif change_type == 'close':
        return ocp_df[(ocp_df['status'] == 'decomissioned')]



def ng_flag_mapper(plant_df: pd.DataFrame, country_ref: pd.DataFrame):
    df = plant_df[['country_code', 'cheap_natural_gas']].drop_duplicates().reset_index(drop=True).copy()
    initial_mapper = dict(zip(df['country_code'], df['cheap_natural_gas']))
    final_values = country_ref['country_code'].apply(
        lambda x: initial_mapper.get(x, 0)
    )
    ng_mapper = dict(zip(country_ref['country_code'], final_values))
    ng_mapper['TWN'] = 0
    return ng_mapper


def production_demand_gap(
    demand_df: pd.DataFrame,
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    year: int, steel_demand_scenario: str,
    capacity_util_max: float = 0.95,
    capacity_util_min: float = 0.6
):

    logger.info(f'Defining the production demand gap for {year}')
    # SUBSETTING & VALIDATION
    avg_plant_global_capacity = capacity_container.return_avg_capacity_value()

    results_container = {}

    for region in capacity_container.regional_capacities_agg[year]:
        initial_utilization = utilization_container.get_utilization_values(year, region) if year == 2020 else utilization_container.get_utilization_values(year - 1, region)
        demand = steel_demand_getter(
            demand_df, year=year, scenario=steel_demand_scenario, metric='crude', region=region)
        current_capacity = capacity_container.return_regional_capacity(year, region)

        avg_plant_capacity_value = avg_plant_global_capacity

        avg_plant_capacity_at_max_production = avg_plant_capacity_value * capacity_util_max

        new_capacity_required = 0
        excess_capacity = 0
        new_plants_required = 0
        plants_to_close = 0
        new_total_capacity = 0

        initial_min_utilization_reqiured = demand / current_capacity
        new_min_utilization_required = 0

        if capacity_util_min <= initial_min_utilization_reqiured <= capacity_util_max:
            # INCREASE CAPACITY: Capacity can be adjusted to meet demand
            new_total_capacity = current_capacity
            new_min_utilization_required = initial_min_utilization_reqiured

        elif initial_min_utilization_reqiured < capacity_util_min:
            # CLOSE PLANT: Excess capacity even in lowest utilization option
            excess_capacity = (current_capacity * capacity_util_min) - demand
            plants_to_close = math.ceil(excess_capacity / avg_plant_capacity_at_max_production)
            new_total_capacity = current_capacity - (plants_to_close * avg_plant_capacity_value)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = max(new_min_utilization_required, capacity_util_min)

        elif initial_min_utilization_reqiured > capacity_util_max:
            # OPEN PLANT: Capacity adjustment not enough to meet demand
            new_capacity_required = demand - (current_capacity * capacity_util_max)
            new_plants_required = math.ceil(new_capacity_required / avg_plant_capacity_at_max_production)
            new_total_capacity = current_capacity + (new_plants_required * avg_plant_capacity_value)
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = min(new_min_utilization_required, capacity_util_max)

        utilization_container.update_region(year, region, new_min_utilization_required)

        initial_utilized_capacity = current_capacity * initial_utilization
        new_utilized_capacity = new_total_capacity * new_min_utilization_required
        initial_balance_value = initial_utilized_capacity - demand
        new_balance_value = new_utilized_capacity - demand

        # RETURN RESULTS
        region_result = {
            'year': year,
            'region': region,
            'capacity': current_capacity,
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

        results_container[region] = region_result

    return results_container


def market_balance_test(production_supply_df: pd.DataFrame, year: int, rounding: int = 0):
    demand_sum = round( production_supply_df['demand'].sum(), rounding)
    capacity_sum = round( production_supply_df['new_total_capacity'].sum(), rounding)
    production_sum = round( production_supply_df['new_utilized_capacity'].sum(), rounding)
    plants_required = production_supply_df['plants_required'].sum()
    plants_to_close = production_supply_df['plants_to_close'].sum()
    logger.info(f'Trade Results for {year}: Demand: {demand_sum :0.2f}  | Capacity: {capacity_sum :0.2f} | Production: {production_sum :0.2f}  | New Plants: {plants_required} | Closed Plants {plants_to_close}')
    assert capacity_sum > demand_sum
    assert capacity_sum > production_sum
    assert production_sum >= demand_sum

def open_close_plants(
    demand_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    lev_cost_df: pd.DataFrame,
    business_case_ref: pd.DataFrame,
    tech_availability: pd.DataFrame,
    variable_costs_df: pd.DataFrame,
    capex_dict: dict,
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    material_container: MaterialUsage,
    tech_choices_container: PlantChoices,
    plant_id_container: PlantIdContainer,
    market_container: MarketContainerClass,
    investment_container: PlantInvestmentCycle,
    ng_mapper: dict,
    year: int,
    trade_scenario: bool = False,
    steel_demand_scenario: bool = False,
    tech_moratorium: bool = False,
    enforce_constraints: bool = False,
    open_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    close_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
):

    logger.info(f'Iterating through the open close loops for {year}')
    investment_dict = investment_container.return_investment_dict()
    production_demand_gap_analysis = production_demand_gap(
        demand_df=demand_df,
        capacity_container=capacity_container,
        utilization_container=utilization_container,
        year=year,
        steel_demand_scenario=steel_demand_scenario,
        capacity_util_max=open_plant_util_cutoff,
        capacity_util_min=close_plant_util_cutoff
    )

    prior_active_plants = steel_plant_df.copy()
    steel_plant_cols = prior_active_plants.columns

    if trade_scenario:
        logger.info(f"Starting the trade flow for {year}")
        production_demand_gap_analysis = trade_flow(
            market_container=market_container,
            production_demand_dict=production_demand_gap_analysis,
            utilization_container=utilization_container,
            capacity_container=capacity_container,
            variable_cost_df=variable_costs_df,
            plant_df=prior_active_plants,
            capex_dict=capex_dict,
            tech_choices_ref= tech_choices_container.return_choices(),
            year=year,
            util_min=close_plant_util_cutoff,
            util_max=open_plant_util_cutoff
        )

    production_demand_gap_analysis_df = pd.DataFrame(
        production_demand_gap_analysis.values()).set_index(['year', 'region']).round(3)

    market_balance_test(production_demand_gap_analysis_df, year)
    market_container.store_results(year, production_demand_gap_analysis_df)

    regions = list(production_demand_gap_analysis.keys())
    random.shuffle(regions)
    levelised_cost_for_regions = lev_cost_df.set_index(['year', 'region']).sort_index(ascending=True).copy()
    levelised_cost_for_tech = lev_cost_df.set_index(['year', 'region', 'technology']).sort_index(ascending=True).copy()

    # REGION LOOP
    for region in regions:
        plants_required = production_demand_gap_analysis[region]['plants_required']
        plants_to_close = production_demand_gap_analysis[region]['plants_to_close']

        # OPEN PLANT
        if plants_required > 0:
            metadata_container = []
            for _ in range(plants_required):
                new_plant_meta = new_plant_metadata(
                    plant_id_container,
                    production_demand_gap_analysis,
                    levelised_cost_for_regions,
                    prior_active_plants,
                    ng_mapper,
                    year=year,
                    region=region
                )
                new_plant_capacity = new_plant_meta['plant_capacity']
                new_plant_name = new_plant_meta['plant_name']
                dict_entry = create_new_plant(new_plant_meta, steel_plant_cols)
                xcost_tech = get_min_cost_tech_for_region(
                    levelised_cost_for_tech,
                    business_case_ref,
                    tech_availability,
                    material_container,
                    tech_moratorium,
                    year,
                    region,
                    new_plant_capacity,
                    new_plant_name,
                    enforce_constraints=enforce_constraints,
                )
                dict_entry['plant_capacity'] = new_plant_capacity * MEGATON_TO_KILOTON_FACTOR
                dict_entry['initial_technology'] = xcost_tech
                metadata_container.append(dict_entry)
                add_new_plant_choices(tech_choices_container, year, new_plant_name, xcost_tech)
            prior_active_plants = pd.concat([prior_active_plants, pd.DataFrame(metadata_container)]).reset_index(drop=True)

        # CLOSE PLANT
        if plants_to_close > 0:
            for _ in range(abs(plants_to_close)):
                # define cos function
                # rank descending
                # filter for age < 11 years
                # pick highest
                plant_to_close = return_oldest_plant(investment_dict, year, return_plants_from_region(prior_active_plants, region))
                idx_close = prior_active_plants.index[prior_active_plants['plant_name'] == plant_to_close].tolist()[0]
                prior_active_plants.loc[idx_close, 'status'] = 'decomissioned'
                prior_active_plants.loc[idx_close, 'end_of_operation'] = year
                prior_active_plants.loc[idx_close, 'active_check'] = False
                add_new_plant_choices(tech_choices_container, year, plant_to_close, 'Close plant')
    
    # dataframe_modification_test(prior_active_plants, production_demand_gap_analysis_df, year)
    new_active_plants = prior_active_plants[prior_active_plants['active_check'] == True]
    capacity_container.map_capacities(new_active_plants, year)
    regional_capacities = capacity_container.return_regional_capacity(year)
    global_demand = steel_demand_getter(
        demand_df, year=year, scenario=steel_demand_scenario, metric='crude', region='World')
    utilization_container.calculate_world_utilization(year, regional_capacities, global_demand)
    new_open_plants = return_modified_plants(new_active_plants, year, 'open')
    investment_container.add_new_plants(new_open_plants['plant_name'], new_open_plants['start_of_operation'])
    return prior_active_plants


def dataframe_modification_test(plant_df: pd.DataFrame, pdga_df: pd.DataFrame, year: int, rounding: int = 1):
    plant_df_capacity_sum = round( plant_df.set_index(['active_check']).loc[True]['plant_capacity'].sum() / MEGATON_TO_KILOTON_FACTOR, rounding)
    new_pdga_df_capacity_sum = round( pdga_df['new_total_capacity'].sum(), rounding)
    old_pdga_df_capacity_sum = round( pdga_df['capacity'].sum(), rounding)
    logger.info(f'Capacity equality check in {year} -> Pre-trade Capacity: {old_pdga_df_capacity_sum :0.2f} | Post-trade Capacity: {new_pdga_df_capacity_sum :0.2f} | Plant DF Capacity: {plant_df_capacity_sum :0.2f}')
    plants_to_close = pdga_df['plants_to_close'].sum()
    plants_required = pdga_df['plants_required'].sum()
    if (plants_to_close == 0) & (plants_required == 0):
        assert plant_df_capacity_sum == old_pdga_df_capacity_sum
    elif plants_to_close > 0:
        pass
    else:
        assert plant_df_capacity_sum == new_pdga_df_capacity_sum


def open_close_flow(
    plant_container: PlantIdContainer,
    market_container: MarketContainerClass,
    plant_df: pd.DataFrame,
    levelized_cost: pd.DataFrame,
    steel_demand_df: pd.DataFrame,
    country_df: pd.DataFrame,
    business_case_ref: dict,
    tech_availability: pd.DataFrame,
    variable_costs_df: pd.DataFrame,
    capex_dict: dict,
    tech_choices_container: PlantChoices,
    investment_container: PlantInvestmentCycle,
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    material_container: MaterialUsage,
    year: int,
    trade_scenario: bool,
    steel_demand_scenario: str,
    tech_moratorium: bool,
    enforce_constraints: bool,
) -> str:

    logger.info(f'Running open close decisions for {year}')
    ng_mapper = ng_flag_mapper(plant_df, country_df)

    return open_close_plants(
        demand_df=steel_demand_df,
        steel_plant_df=plant_df,
        lev_cost_df=levelized_cost,
        business_case_ref=business_case_ref,
        tech_availability=tech_availability,
        variable_costs_df=variable_costs_df,
        capex_dict=capex_dict,
        capacity_container=capacity_container,
        utilization_container=utilization_container,
        material_container=material_container,
        investment_container=investment_container,
        tech_choices_container=tech_choices_container,
        ng_mapper=ng_mapper,
        plant_id_container=plant_container,
        market_container=market_container,
        year=year,
        trade_scenario=trade_scenario,
        steel_demand_scenario=steel_demand_scenario,
        tech_moratorium=tech_moratorium,
        enforce_constraints=enforce_constraints
    )
