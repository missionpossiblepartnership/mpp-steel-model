"""Classes to manage Capacity & Utilization"""

import pandas as pd
import numpy as np

from mppsteel.config.model_config import (
    PKL_DATA_IMPORTS, 
    PKL_DATA_FORMATTED, 
    MAIN_REGIONAL_SCHEMA,
    GIGATON_TO_MEGATON_FACTOR
)
from mppsteel.config.reference_lists import RESOURCE_CONTAINER_REF
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder
)
from mppsteel.model.solver_constraints import (
    create_carbon_constraint,
    create_biomass_constraint,
    create_scrap_constraints,
    return_projected_usage,
    tech_availability_check,
)
from mppsteel.utility.log_utility import get_logger
logger = get_logger(__name__)

class PlantChoices:
    def __init__(self):
        self.choices = {}
        self.records = []

    def initiate_container(self, year_range: range):
        for year in year_range:
            self.choices[str(year)] = {}
            
    def update_choices(self, year: int, plant: str, tech: str):
        self.choices[str(year)][plant] = tech
            
    def update_records(self, df_entry: pd.DataFrame):
        self.records.append(df_entry)

    def get_choice(self, year: int, plant: str):
        return self.choices[str(year)][plant]

    def return_choices(self):
        return self.choices

    def output_records_to_df(self):
        return pd.DataFrame(self.records).reset_index(drop=True)


class MaterialUsage:
    def __init__(self):
        self.constraint = {}
        self.usage = {}
        self.balance = {}
        self.results = []

    def initiate_years(self, year_range: range, resource_list: list):
        for year in year_range:
            self.usage[year] = {resource: 0 for resource in resource_list}
            self.balance[year] = {resource: 0 for resource in resource_list}

    def load_constraint(self, model: pd.DataFrame, model_type: str):
        if model_type == 'biomass':
            self.constraint[model_type] = create_biomass_constraint(model)
        elif model_type == 'scrap':
            self.constraint[model_type] = create_scrap_constraints(model)
        elif model_type in {'ccs', 'co2'}:
            self.constraint[model_type] = create_carbon_constraint(model, model_type)

    def set_year_balance(self, year: int, model_type: str):
        self.balance[year][model_type] = self.constraint[model_type][year]

    def get_current_balance(self, year: int, model_type: str):
        return self.balance[year][model_type]

    def get_current_usage(self, year: int, model_type: str):
        return self.usage[year][model_type]

    def record_results(self, dict_entry: dict):
        self.results.append(dict_entry)

    def output_results_to_df(self):
        return pd.DataFrame(self.results)

    def constraint_transaction(self, year: int, model_type: str, amount: float, override_constraint: bool = False):
        current_balance = self.balance[year][model_type]
        current_usage = self.usage[year][model_type]
        if (current_balance < amount) and not override_constraint:
            return False
        self.balance[year][model_type] = current_balance - amount
        self.usage[year][model_type] = current_usage + amount
        return True

class CapacityContainerClass():
    def __init__(self):
        self.plant_capacities = {}
        self.regional_capacities_agg = {}
        self.regional_capacities_avg = {}
    
    def instantiate_container(self, year_range: range):
        self.plant_capacities = {year: 0 for year in year_range}
        self.regional_capacities_agg = {year: 0 for year in year_range}
        self.regional_capacities_avg = {year: 0 for year in year_range}

    def map_capacities(self, original_plant_df: pd.DataFrame, plant_df: pd.DataFrame, year: int):
        # Map capacities of plants that are still active for aggregates, else use averages
        plant_capacity_dict, regional_capacity_dict = create_annual_capacity_dict(plant_df, year, as_gt=True)
        self.plant_capacities[year] = plant_capacity_dict
        self.regional_capacities_agg[year] = regional_capacity_dict
        self.regional_capacities_avg[year] = create_regional_capacity_dict(original_plant_df, as_avg=True, as_gt=True)

    def return_regional_capacity(self, year: int = None, region: str = None, capacity_type: str = 'agg'):
        capacity_dict = self.regional_capacities_agg if capacity_type == 'agg' else self.regional_capacities_avg
        if region and not year:
            # return a year value time series for a region
            return {year_val: capacity_dict[year_val][region] for year_val in capacity_dict}
        
        if year and not region:
            # return all regions for single year
            return capacity_dict[year]
        
        if year and region:
            # return single value
            return capacity_dict[year][region]
        
        # return all years and regions
        return capacity_dict

    def update_region(self, year: int, region: str, value: float):
        self.utilization_container[year][region] = value

    def return_avg_capacity_value(self, year: int, capacity_type: str = 'agg'):
        if capacity_type == 'agg':
            return np.mean(list(self.regional_capacities_agg[year].values()))
        elif capacity_type == 'avg':
            return np.mean(list(self.regional_capacities_avg[year].values()))

    def get_world_capacity_sum(self, year: int):
        return sum(list(self.regional_capacities_agg[year].values()))
        
    def return_plant_capacity(self, year: int = None, plant: str = None):
        if plant and not year:
            # return a year valye time series for a region
            return {year_val: (self.plant_capacities[year_val][plant] if plant in self.plant_capacities[year_val] else 0) for year_val in self.plant_capacities}
        
        if year and not plant:
            # return all plants for single year
            return self.plant_capacities[year]
        
        if year and plant:
            # return single value
            return self.plant_capacities[year][plant] if plant in self.plant_capacities[year] else 0
        
        # return all years and regions
        return self.plant_capacities

class UtilizationContainerClass:
    def __init__(self):
        self.utilization_container = {}
        
    def initiate_container(self, year_range: range, region_list: list):
        for year in year_range:
            self.utilization_container[year] = {region: 0 for region in region_list}
    
    def assign_year_utilization(self, year: int, entry: dict):
        self.utilization_container[year] = entry
        
    def update_region(self, year: int, region: str, value: float):
        self.utilization_container[year][region] = value
    
    def get_average_utilization(self, year: int):
        return np.mean(self.utilization_container[year].values())

    def calculate_world_utilization(self, year: int, capacity_dict: dict):
        region_container = []
        for region in capacity_dict:
            region_container.append(capacity_dict[region] * self.utilization_container[year][region])
        
        world_utilization = sum(region_container) / sum(capacity_dict.values())
        self.utilization_container[year]['World'] = round(world_utilization, 3)
    
    def get_utilization_values(self, year: int = None, region: str = None):
        if region and not year:
            # return a year valye time series for a region
            return {year_val: self.utilization_container[year_val][region] for year_val in self.regional_capacities}
        
        if year and not region:
            # return all regions for single year
            return self.utilization_container[year]
        
        if year and region:
            # return single value
            return self.utilization_container[year][region]
        
        # return all years and regions
        return self.utilization_container


class MarketContainerClass:
    def __init__(self):
        self.trade_container = {}
        self.market_results = {}
        
    def __repr__(self):
        return "Trade Container"
    
    def __str__(self):
        return "Trade Container Class"
    
    def initiate_years(self, year_range: range):
        self.trade_container = {year: {} for year in year_range}
        self.market_results = {year: {} for year in year_range}
        
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

    def store_results(self, year: int, results_df: pd.DataFrame):
        self.market_results[year] = results_df

    def output_trade_calculations_to_df(self):
        return pd.concat(self.market_results.values(), axis=1)


def apply_constraints(
    business_case_ref: dict,
    plant_capacities: dict,
    material_usage_dict_container: MaterialUsage,
    combined_available_list: list,
    year: int,
    plant_name: str,
    base_tech: str
):
    # Constraints checks
    new_availability_list = []
    for switch_technology in combined_available_list:
        material_check_container = {}
        for resource in RESOURCE_CONTAINER_REF:
            projected_usage = return_projected_usage(
                plant_name,
                switch_technology,
                plant_capacities,
                business_case_ref,
                RESOURCE_CONTAINER_REF[resource]
            )

            material_check = material_usage_dict_container.constraint_transaction(
                year,
                resource,
                projected_usage,
                override_constraint=False
            )
            material_check_container[resource] = True if material_check else False
        if all(material_check_container.values()):
            new_availability_list.append(switch_technology)
        failure_resources = [resource for resource in material_check_container if not material_check_container[resource]]

        result = 'PASS' if all(material_check_container.values()) else 'FAIL'

        entry = {
            'plant': plant_name,
            'start_technology': base_tech,
            'switch_technology': switch_technology,
            'year': year,
            'assign_case': 'pre-existing plant',
            'result': result,
            'failure_resources': failure_resources,
            'pass_boolean_check': material_check_container
        }

        material_usage_dict_container.record_results(entry)
    return new_availability_list

def apply_constraints_for_min_cost_tech(
    business_case_ref: dict,
    tech_availability: pd.DataFrame,
    material_usage_dict_container: MaterialUsage,
    combined_available_list: list,
    plant_capacity: float,
    tech_moratorium: bool,
    year: int,
    plant_name: str,
):
    new_availability_list = []
    # Constraints checks
    combined_available_list = [
        tech for tech in combined_available_list if tech_availability_check(
            tech_availability, tech, year, tech_moratorium=tech_moratorium
        )
    ]
    for technology in combined_available_list:
        material_check_container = {}
        for resource in RESOURCE_CONTAINER_REF:
            projected_usage = sum([business_case_ref[(technology, material)] * plant_capacity for material in RESOURCE_CONTAINER_REF[resource]] )
            material_check = material_usage_dict_container.constraint_transaction(
                year,
                resource,
                projected_usage,
                override_constraint=False
            )
            material_check_container[resource] = True if material_check else False
        if all(material_check_container.values()):
            new_availability_list.append(technology)

        failure_resources = [resource for resource in material_check_container if material_check_container[resource]]
        
        result = 'PASS' if all(material_check_container.values()) else 'FAIL'

        entry = {
            'plant': plant_name,
            'start_technology': 'unknown',
            'switch_technology': technology,
            'year': year,
            'assign_case': 'new plant',
            'result': result,
            'failure_resources': failure_resources,
            'pass_boolean_check': material_check_container
        }

        material_usage_dict_container.record_results(entry)
    return new_availability_list

def create_regional_capacity_dict(plant_df: pd.DataFrame, rounding: int = 3, as_avg: bool = False, as_gt: bool = False):
    logger.info('Deriving average plant capacity statistics')
    plant_df_c = plant_df.copy()
    df = plant_df_c[[MAIN_REGIONAL_SCHEMA, 'plant_capacity']].groupby([MAIN_REGIONAL_SCHEMA])
    if as_avg:
        df = df.mean().round(rounding).reset_index()
    else:
        df = df.sum().round(rounding).reset_index()
    dict_obj = dict(zip(df[MAIN_REGIONAL_SCHEMA], df['plant_capacity']))
    if as_gt:
        return {region: value / GIGATON_TO_MEGATON_FACTOR for region, value in dict_obj.items()} # Mt to Gt
    return dict_obj


def create_annual_capacity_dict(plant_df: pd.DataFrame, year: int, rounding: int = 3, as_gt: bool = False):
    plant_capacity_dict = {}
    regional_capacity_dict = {}
    for row in plant_df.itertuples():
        if row.status in ['operating', 'proposed', 'construction']:
            plant_capacity_dict[row.plant_name] = row.plant_capacity
        elif 'operating ' in row.status:
            if row.start_of_operation <= year:
                plant_capacity_dict[row.plant_name] = row.plant_capacity
    regions = plant_df[MAIN_REGIONAL_SCHEMA].unique()
    regional_capacity_dict_list = {region: [plant_capacity_dict[plant_name] for plant_name in set(plant_capacity_dict.keys()).intersection(set(plant_df[plant_df[MAIN_REGIONAL_SCHEMA] == region]['plant_name'].unique()))] for region in regions}
    regional_capacity_dict = {region: sum(value_list) for region, value_list in regional_capacity_dict_list.items()}
    if as_gt: # Mt to Gt
        plant_capacity_dict = {plant_name: value / GIGATON_TO_MEGATON_FACTOR for plant_name, value in plant_capacity_dict.items()}
        regional_capacity_dict = {region: value / GIGATON_TO_MEGATON_FACTOR for region, value in regional_capacity_dict.items()}
    return plant_capacity_dict, regional_capacity_dict



def format_wsa_production_data(df, as_dict: bool = False):
    logger.info('Formatting WSA production data for 2020')
    df_c = df.copy()
    df_c = df_c.melt(id_vars=['WSA_Region','RMI_Region','Country','Metric','Unit'], var_name='year')
    df_c = df_c[df_c['year'] == 2020]
    df_c.columns = [col.lower() for col in df_c.columns]
    df_c = df_c.groupby([MAIN_REGIONAL_SCHEMA, 'year']).sum().reset_index()
    if as_dict:
        return dict(zip(df_c[MAIN_REGIONAL_SCHEMA], df_c['value']))
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
    capacity_dict = create_regional_capacity_dict(steel_plants_processed, as_gt=True)
    return return_utilization(wsa_2020_production_dict, capacity_dict, value_cap=1)
