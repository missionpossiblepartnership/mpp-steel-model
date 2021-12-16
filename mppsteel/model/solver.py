"""Main solving script for deciding investment decisions."""

import pandas as pd
import numpy as np
from tqdm import tqdm

from mppsteel.utility.utils import (
    read_pickle_folder, create_list_permutations,
    serialise_file, get_logger
)

from mppsteel.model_config import (
    PKL_FOLDER, IMPORT_DATA_PATH, SWITCH_DICT, DISCOUNT_RATE,
    TECHNOLOGY_STATES, FURNACE_GROUP_DICT,
    TECH_MATERIAL_CHECK_DICT,
    RESOURCE_CONTAINER_REF,
    TCO_RANK_1_SCALER, TCO_RANK_2_SCALER,
    ABATEMENT_RANK_2, ABATEMENT_RANK_3,
    GREEN_PREMIUM_MIN_PCT, GREEN_PREMIUM_MAX_PCT,
    MODEL_YEAR_END, SWITCH_RANK_PROPORTIONS
)

from mppsteel.data_loading.data_interface import (
    commodity_data_getter, static_energy_prices_getter,
)

from mppsteel.model.prices_and_emissions_tables import (
    dynamic_energy_price_getter
)

from mppsteel.minimodels.timeseries_generator import (
    timeseries_generator,
)

from mppsteel.model.tco_and_emissions import (
    calculate_present_values, compare_capex,
    generate_capex_financial_summary,
)

from mppsteel.utility.timeseries_extender import (
    full_model_flow
)

# Create logger
logger = get_logger("Solver")

def return_furnace_group(furnace_dict: dict, tech:str):
    for key in furnace_dict.keys():
        if tech in furnace_dict[key]:
            return furnace_dict[key]


def read_and_format_tech_availability():
    tech_availability = read_pickle_folder(PKL_FOLDER, 'tech_availability', 'df')
    tech_availability.columns = [col.lower().replace(' ', '_') for col in tech_availability.columns]
    return tech_availability[['technology', 'main_technology_type', 'technology_phase', 'year_available_from', 'year_available_until']].set_index('technology')

def tech_availability_check(tech_df: pd.DataFrame, technology: str, year: int, tech_moratorium: bool = False) -> bool:
    row = tech_df.loc[technology].copy()
    year_available = row.loc['year_available_from']
    year_unavailable = row.loc['year_available_until']
    if tech_moratorium:
        if row.loc['technology_phase'] == 'Initial':
            year_unavailable = 2030
    if year_available < year < year_unavailable:
        # print(f'{technology} will be available in {year}')
        return True
    if year < year_available:
        # print(f'{technology} will not be ready yet in {year}')
        return False
    if year > year_unavailable:
        # print(f'{technology} will become unavailable in {year}')
        return False

def ccs_co2_getter(df: pd.DataFrame, metric: str, year: str) -> float:
    if year > 2050:
        year = 2050
    df_c = df.copy()
    metric_names = df_c["Metric"].unique()
    # logger.info(f'Creating CCS CO2 getter with the following metrics: {metric_names}')
    df_c.set_index(["Metric", "Year"], inplace=True)
    # logger.info(f'Getting {metric} value for: {year}')
    value = df_c.loc[metric, year]["Value"]
    return value

def biomass_getter(biomass_df: pd.DataFrame, year: int):
    if year > 2050:
        year = 2050
    return biomass_df.set_index('year').loc[year]['value']

def plant_closure_check(utilization_rate: float, cutoff: float, current_tech: str):
    if utilization_rate < cutoff:
        return 'Close Plant'
    return current_tech

def format_steel_plant_df(df: pd.DataFrame):
    df_c = df.copy() 
    steel_plant_cols_to_remove = [
        'Fill in data BF-BOF',
        'Fill in data EAF', 'Fill in data DRI',
        'Estimated BF-BOF capacity (kt steel/y)',
        'Estimated EAF capacity (kt steel/y)',
        'Estimated DRI capacity (kt sponge iron/y)',
        'Estimated DRI-EAF capacity (kt steel/y)',
        'Source', 'Excel Tab'
    ]
    df_c.drop(steel_plant_cols_to_remove, axis=1, inplace = True)
    new_steel_plant_cols = [
        'plant_name', 'parent', 'country', 'region', 'coordinates', 'status', 'start_of_operation',
        'BFBOF_capacity', 'EAF_capacity', 'DRI_capacity', 'DRIEAF_capacity', 'abundant_res',
        'ccs_available', 'cheap_natural_gas', 'industrial_cluster', 'technology_in_2020']
    df_c = df_c.rename(mapper=dict(zip(df_c.columns, new_steel_plant_cols)), axis=1)
    return df_c

def extract_steel_plant_capacity(df: pd.DataFrame):

    def convert_to_float(val):
        try:
            return float(val)
        except:
            if isinstance(val, float):
                return val
        return 0

    df_c = df.copy()
    capacity_cols = ['BFBOF_capacity', 'EAF_capacity', 'DRI_capacity', 'DRIEAF_capacity']
    for row in df_c.itertuples():
        tech = row.technology_in_2020
        for col in capacity_cols:
            if col == 'EAF_capacity':
                if tech == 'EAF':
                    value = convert_to_float(row.EAF_capacity)
                    df_c.loc[row.Index, 'primary_capacity_2020'] = 0
            elif col == 'BFBOF_capacity':
                if tech in ['Avg BF-BOF', 'BAT BF-BOF']:
                    value = convert_to_float(row.BFBOF_capacity)
                    df_c.loc[row.Index, 'primary_capacity_2020'] = value
            elif col == 'DRIEAF_capacity':
                if tech in ['DRI-EAF', 'DRI-EAF+CCUS']:
                    value = convert_to_float(row.DRIEAF_capacity)
                    df_c.loc[row.Index, 'primary_capacity_2020'] = value
            elif col == 'DRI_capacity':
                if tech == 'DRI':
                    value = convert_to_float(row.DRI_capacity)
                    df_c.loc[row.Index, 'primary_capacity_2020'] = value
            else:
                df_c.loc[row.Index, 'primary_capacity_2020'] = 0
    df_c['secondary_capacity_2020'] = df_c['EAF_capacity'].apply(lambda x: convert_to_float(x)) - df_c['DRIEAF_capacity'].apply(lambda x: convert_to_float(x)) 
    return df_c

def create_plant_capacities_dict():
    # Edit this one!
    steel_plant_df = generate_formatted_steel_plants()
    technologies = steel_plant_df['technology_in_2020']
    plant_names = steel_plant_df['plant_name']
    primary_capacities = steel_plant_df['primary_capacity_2020']
    secondary_capacities = steel_plant_df['secondary_capacity_2020']
    plant_capacities = {}
    ticker = 0
    while ticker < len(plant_names):
        plant_name = plant_names.iloc[ticker]
        row = {
            '2020_tech': technologies.iloc[ticker],
            'primary_capacity': primary_capacities.iloc[ticker],
            'secondary_capacity': secondary_capacities.iloc[ticker]}
        plant_capacities[plant_name] = row
        ticker += 1
    return plant_capacities


def calculate_primary_and_secondary(tech_capacities: dict, plant: str, tech: str):
    if tech == 'EAF':
        return tech_capacities[plant]['secondary_capacity'] + tech_capacities[plant]['primary_capacity']
    return tech_capacities[plant]['primary_capacity']

def material_usage_summary(business_case_df: pd.DataFrame, material: str, technology: str = ''):
    if technology:
        try:
            return business_case_df.groupby(['material_category', 'technology']).sum().loc[material, technology].values[0]
        except:
            return 0
    return business_case_df.groupby(['material_category', 'technology']).sum().loc[material]

def total_plant_capacity(plant_cap_dict: dict):
    all_capacities = [calculate_primary_and_secondary(plant_cap_dict, plant, plant_cap_dict[plant]['2020_tech']) for plant in plant_cap_dict.keys()]
    all_capacities = [x for x in all_capacities if str(x) != 'nan']
    return sum(all_capacities)


def material_usage(
    plant_capacities: dict, steel_plant_df: pd.DataFrame, business_cases: pd.DataFrame,
    materials_list: list, plant_name: str, year: float, tech: str, material: str
    ):

    plant_capacity = calculate_primary_and_secondary(plant_capacities, plant_name, tech) / 1000
    steel_demand = steel_demand_value_selector(steel_plant_df, 'Crude', year, 'bau')
    capacity_sum = total_plant_capacity(plant_capacities)
    projected_production = (plant_capacity / capacity_sum) * steel_demand
    material_list = []
    for material in materials_list:
        usage_value = material_usage_summary(business_cases, material, tech)
        material_list.append(usage_value)
    material_usage = projected_production * sum(material_list)
    return material_usage

def plant_tech_resource_checker(
    plant_name: str,
    base_tech: str,
    year: int,
    steel_demand_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    biomass_df: pd.DataFrame,
    ccs_co2_df: pd.DataFrame,
    materials_list: list,
    tech_material_dict: dict,
    resource_container_ref: dict,
    plant_capacities: dict,
    material_usage_dict: dict = {},
    output_type: str = 'excluded'
):

    tech_list = SWITCH_DICT[base_tech].copy()
    if 'Close plant' in tech_list:
        tech_list.remove('Close plant')

    tech_approved_list = []

    for tech in tech_list:

        material_check_container = []

        for material_check in tech_material_dict[tech]:
            # Setting parameters
            if material_check == 'Bioenergy':
                material_ref = 'Bioenergy'
                material_capacity = biomass_getter(biomass_df, year)
                materials_to_check = ['Biomass', 'Biomethane']

            if material_check == 'Used CO2':
                material_ref = 'Used CO2'
                material_capacity = ccs_co2_getter(ccs_co2_df, 'Steel CO2 use market', year)
                materials_to_check = ['Used CO2']

            if material_check == 'Captured CO2':
                material_ref = 'Captured CO2'
                material_capacity = ccs_co2_getter(ccs_co2_df, 'Total Steel CCS capacity', year)
                materials_to_check = ['Captured CO2']

            if material_check in ['Scrap', 'Scrap EAF']:
                material_ref = 'Scrap'
                material_capacity = steel_demand_value_selector(steel_demand_df, 'Scrap', year, 'bau')
                materials_to_check = ['Scrap']

            # Checking for zero
            if material_capacity == 0:
                logger.info(f'{year} -> Material {material_check} is not available, capacity = 0')
                material_check_container.append(False)
                pass

            # Core logic
            material_container = material_usage_dict[resource_container_ref[material_ref]]
            if material_check in ['Bioenergy', 'Used CO2', 'Captured CO2', 'Scrap']:
                current_usage = sum(material_container)
                if current_usage == 0:
                    logger.info('First usage for {material_check}')
                resource_remaining = material_capacity - current_usage
                plant_usage = material_usage(plant_capacities, steel_demand_df, business_cases, materials_list, plant_name, year, tech, materials_to_check)
                if plant_usage > resource_remaining:
                    print(f'{year} -> {plant_name} cannot adopt {tech} because usage of {material_check} exceeds capacity | uses {plant_usage} of remaining {resource_remaining}')
                    material_check_container.append(False)
                else:
                    print(f'{year} -> {plant_name} can adopt {tech} because usage of {material_check} does not exceed capacity | uses {plant_usage} of remaining {resource_remaining}')
                    material_check_container.append(True)
                    material_container.append(plant_usage)

            if material_check in ['Scrap EAF']:
                if material_capacity > 1.5:
                    print(f'Sufficient enough scrap for {tech} -> {material_capacity}')
                    material_check_container.append(True)
                else:
                    print(f'Not enough scrap for {tech}')
                    material_check_container.append(False)

        if all(material_check_container):
            logger.info(f'PASSED: {tech} has passed availability checks for {plant_name}')
            tech_approved_list.append(tech)
        else:
            if tech == base_tech:
                logger.info(f'PASSED: {tech} is the same as based tech, but would have failed otherwise')
                tech_approved_list.append(tech)
                # material_container.append(plant_usage)
            else:
                logger.info(f'FAILED: {tech} has NOT passed availability checks for {plant_name}')

    unavailable_techs = list(set(tech_list).difference(set(tech_approved_list)))
    # Final check and return

    if output_type == 'excluded':
        return unavailable_techs
    if output_type == 'included':
        return tech_approved_list


def generate_variable_costs(
    business_cases_df: pd.DataFrame,
    plant_iteration: list = None,
    year_end: int = None,
    feedstock_dict: dict = None,
    static_energy_df: pd.DataFrame = None,
    electricity_df: pd.DataFrame = None,
    hydrogen_df: pd.DataFrame = None
) -> pd.DataFrame:

    df_list = []

    plant_iteration_dict = {
        'abundant_res': plant_iteration[0],
        'ccs_available': plant_iteration[1],
        'cheap_natural_gas': plant_iteration[2],
    }

    # Create resources reference list
    static_energy_list = static_energy_df['Metric'].unique().tolist()
    feedstock_list = list(feedstock_dict.keys())

    # Create a year range
    year_range = range(2020, tuple({year_end+1 or 2021})[0])

    for year in year_range:
        df_c = business_cases_df.copy()

        df_c['Static'] = ''
        df_c['Feedstock'] = ''
        df_c['Electricity'] = ''
        df_c['Hydrogen'] = ''
        df_c['Natural gas'] = ''

        static_year = year
        if year > 2026:
            static_year = 2026
        dynamic_year = year
        if year > 2050:
            dynamic_year = 2050

        low_elec_price = dynamic_energy_price_getter(electricity_df, 'favorable', dynamic_year)
        high_elec_price = dynamic_energy_price_getter(electricity_df, 'average', dynamic_year)
        low_hyd_price = dynamic_energy_price_getter(hydrogen_df, 'favorable', dynamic_year)
        high_hyd_price = dynamic_energy_price_getter(hydrogen_df, 'average', dynamic_year)
        natural_gas_high = static_energy_prices_getter(static_energy_df, 'Natural gas - high', static_year)
        natural_gas_low = static_energy_prices_getter(static_energy_df, 'Natural gas - low', static_year)

        for row in df_c.itertuples():
            resource = row.material_category
            resource_consumed = row.value

            if resource in static_energy_list:
                price_unit_value = static_energy_prices_getter(static_energy_df, resource, static_year)
                df_c.loc[row.Index, 'Static'] = resource_consumed * price_unit_value

            if resource in feedstock_list:
                price_unit_value = feedstock_dict[resource]
                df_c.loc[row.Index, 'Feedstock'] = resource_consumed * price_unit_value

            if resource == 'Natural gas':
                if plant_iteration_dict['cheap_natural_gas'] == 1:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * natural_gas_low
                elif plant_iteration_dict['cheap_natural_gas'] == 0:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * natural_gas_high

            if resource == 'Electricity':
                if plant_iteration_dict['abundant_res'] == 1 or plant_iteration_dict['ccs_available'] == 1:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * low_elec_price
                elif plant_iteration_dict['abundant_res'] == 0 and plant_iteration_dict['ccs_available'] == 0:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * high_elec_price

            if resource == 'Hydrogen':
                if plant_iteration_dict['abundant_res'] == 1 or plant_iteration_dict['ccs_available'] == 1:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * low_hyd_price
                elif plant_iteration_dict['abundant_res'] == 0 and plant_iteration_dict['ccs_available'] == 0:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * high_hyd_price

        df_c['year'] = year
        df_list.append(df_c)
        
    combined_df = pd.concat(df_list)
    return combined_df

def plant_variable_costs():
    df_list = []

    options = [[0,0,0],[1,0,0],[1,1,0],[1,1,1],[0,1,1],[0,0,1],[0,1,0],[1,0,1]]
    plant_iterations = {''.join([str(num) for num in option]): option for option in options}

    electricity_minimodel_timeseries = read_pickle_folder(PKL_FOLDER, 'electricity_minimodel_timeseries', 'df')
    hydrogen_minimodel_timeseries = read_pickle_folder(PKL_FOLDER, 'hydrogen_minimodel_timeseries', 'df')

    static_energy_prices = read_pickle_folder(PKL_FOLDER, 'static_energy_prices', 'df')[['Metric', 'Year', 'Value']]
    feedstock_dict = generate_feedstock_dict()

    business_cases = load_business_cases()

    for plant_iteration in tqdm(plant_iterations, total=len(plant_iterations), desc='Plant variables'):
        df = generate_variable_costs(
            business_cases_df=business_cases,
            plant_iteration=plant_iteration,
            year_end=2050,
            feedstock_dict=feedstock_dict,
            static_energy_df=static_energy_prices,
            electricity_df=electricity_minimodel_timeseries,
            hydrogen_df=hydrogen_minimodel_timeseries
        )
        df['plant_iteration'] = plant_iteration
        df_list.append(df)

    return pd.concat(df_list).reset_index(drop=True)

def create_new_material_usage_dict(resource_container_ref: dict):
    return {material_key: [] for material_key in resource_container_ref.values()}


def format_variable_costs(variable_cost_df: pd.DataFrame):

    df_c = variable_cost_df.copy()
    df_c.drop(labels=['value'], axis=1, inplace=True)
    df_c = df_c.melt(id_vars=['plant_iteration', 'technology', 'year', 'material_category', 'unit'],var_name=['cost_type'], value_name='cost')
    df_c['cost'] = df_c['cost'].replace('', 0)
    return df_c.groupby(by=['plant_iteration', 'year', 'technology']).sum().sort_values(by=['plant_iteration', 'year', 'technology'])


def carbon_tax_estimate(emission_dict_ref: dict, carbon_tax_df: pd.DataFrame, year: int):
    year_ref = year
    if year > 2050:
        year_ref = 2050
    carbon_tax = carbon_tax_df.set_index('year').loc[year_ref]['value']
    return (emission_dict_ref['s2'].loc[year_ref] + emission_dict_ref['s3'].loc[year_ref]) * carbon_tax

def get_steel_making_costs(steel_plant_df: pd.DataFrame, variable_cost_df, plant_name: str, technology: str):
    conversion = 0.877
    if technology == 'Not operating':
        return 0
    else:
        primary = steel_plant_df[steel_plant_df['plant_name'] == plant_name]['primary_capacity_2020'].values[0]
        secondary = steel_plant_df[steel_plant_df['plant_name'] == plant_name]['secondary_capacity_2020'].values[0]
        variable_tech_cost = variable_cost_df.loc[technology].values[0]
        if technology == 'EAF':
            variable_cost_value = (variable_tech_cost * primary) + (variable_tech_cost * secondary)
        else:
            variable_cost_value = variable_tech_cost * primary

        return (primary + secondary) * (variable_cost_value / (primary+secondary)) / (primary + secondary) / conversion

def get_opex_costs(
    plant: tuple,
    year: int,
    variable_costs_df: pd.DataFrame,
    opex_df: pd.DataFrame,
    emissions_dict_ref: dict,
    carbon_tax_timeseries: pd.DataFrame,
    green_premium_timeseries: pd.DataFrame,
    steel_plant_capacity: pd.DataFrame,
    include_carbon_tax: bool = False,
    include_green_premium: bool = False,
):
    combined_row = str(plant.abundant_res) + str(plant.ccs_available) + str(plant.cheap_natural_gas)
    variable_costs = variable_costs_df.loc[combined_row, year]
    opex_costs = opex_df.swaplevel().loc[year]

    # single results
    carbon_tax_result = 0
    if include_carbon_tax:
        carbon_tax_result = carbon_tax_estimate(emissions_dict_ref, carbon_tax_timeseries, year)
    green_premium_value = 0
    if include_green_premium:
        steel_making_cost = get_steel_making_costs(steel_plant_capacity, variable_costs, plant.plant_name, plant.technology_in_2020)
        green_premium = green_premium_timeseries[green_premium_timeseries['year'] == year]['value'].values[0]
        green_premium_value = (steel_making_cost * green_premium)

    variable_costs.rename(mapper={'cost': 'value'},axis=1, inplace=True)
    carbon_tax_result.rename(mapper={'emissions': 'value'},axis=1, inplace=True)
    total_opex = variable_costs + opex_costs + carbon_tax_result - green_premium_value
    total_opex.rename(mapper={'value': 'opex'},axis=1, inplace=True)
    total_opex.loc['Close plant', 'opex'] = 0
    total_opex.drop(['Charcoal mini furnace', 'Close plant'],inplace=True)
    return total_opex

def get_opex_costs(
    plant: tuple,
    year: int,
    variable_costs_df: pd.DataFrame,
    opex_df: pd.DataFrame,
    emissions_dict_ref: dict,
    carbon_tax_timeseries: pd.DataFrame,
    green_premium_timeseries: pd.DataFrame,
    steel_plant_capacity: pd.DataFrame,
    include_carbon_tax: bool = False,
    include_green_premium: bool = False,
):
    combined_row = str(plant.abundant_res) + str(plant.ccs_available) + str(plant.cheap_natural_gas)
    variable_costs = variable_costs_df.loc[combined_row, year]
    opex_costs = opex_df.swaplevel().loc[year]

    # single results
    carbon_tax_result = 0
    if include_carbon_tax:
        carbon_tax_result = carbon_tax_estimate(emissions_dict_ref, carbon_tax_timeseries, year)
    green_premium_value = 0
    if include_green_premium:
        steel_making_cost = get_steel_making_costs(steel_plant_capacity, variable_costs, plant.plant_name, plant.technology_in_2020)
        green_premium = green_premium_timeseries[green_premium_timeseries['year'] == year]['value'].values[0]
        green_premium_value = (steel_making_cost * green_premium)

    variable_costs.rename(mapper={'cost': 'value'},axis=1, inplace=True)
    carbon_tax_result.rename(mapper={'emissions': 'value'},axis=1, inplace=True)
    total_opex = variable_costs + opex_costs + carbon_tax_result - green_premium_value
    total_opex.rename(mapper={'value': 'opex'},axis=1, inplace=True)
    total_opex.loc['Close plant', 'opex'] = 0
    total_opex.drop(['Charcoal mini furnace', 'Close plant'],inplace=True)
    return total_opex

def calculate_capex(start_year):
    df_list = []
    for base_tech in SWITCH_DICT.keys():
        for switch_tech in SWITCH_DICT[base_tech]:
            if switch_tech == 'Close plant':
                pass
            else:
                df = compare_capex(base_tech, switch_tech, start_year)
                df_list.append(df)
    full_df = pd.concat(df_list)
    return full_df.set_index(['year', 'start_technology'])

def get_capex_year(capex_df: pd.DataFrame, start_tech: str, end_tech: str, switch_year: int):
    df_c = None
    if switch_year > 2050:
        df_c = capex_df.loc[2050, start_tech].copy()
    elif 2020 <= switch_year <= 2050:
        df_c = capex_df.loc[switch_year, start_tech].copy()

    raw_switch_value = df_c.loc[df_c['new_technology'] == end_tech]['value'].values[0]

    financial_summary = generate_capex_financial_summary(
        principal=raw_switch_value,
        interest_rate=DISCOUNT_RATE,
        years=20,
        downpayment=0,
        compounding_type='annual',
        rounding=2
    )

    return financial_summary

def get_discounted_opex_values(
    plant: tuple, year_start: int, carbon_tax_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    variable_cost_summary: pd.DataFrame,
    green_premium_timeseries: pd.DataFrame,
    other_opex_df: pd.DataFrame,
    year_interval: int = 20, int_rate: float = 0.07):
    year_range = range(year_start, year_start+year_interval+1)
    df_list = []
    emissions_dict = create_emissions_dict()
    for year in year_range:
        year_ref = year
        if year > 2050:
            year_ref = 2050
        df = get_opex_costs(
            plant,
            year_ref,
            variable_cost_summary,
            other_opex_df,
            emissions_dict,
            carbon_tax_df,
            green_premium_timeseries,
            steel_plant_df,
            include_carbon_tax=True,
            include_green_premium=True
        )
        df['year'] = year_ref
        df_list.append(df)
    df_combined = pd.concat(df_list)
    technologies = df.index
    new_df = pd.DataFrame(index=technologies, columns=['value'])
    for technology in technologies:
        values = df_combined.loc[technology]['opex'].values
        discounted_values = calculate_present_values(values, int_rate)
        new_df.loc[technology, 'value'] = sum(discounted_values)
    return new_df

def tco_calc(
    plant, start_year: int, plant_tech: str, carbon_tax_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame, variable_cost_summary: pd.DataFrame, 
    green_premium_timeseries: pd.DataFrame, other_opex_df: pd.DataFrame, 
    investment_cycle: int = 20
    ):
    opex_values = get_discounted_opex_values(
        plant, start_year, carbon_tax_df, steel_plant_df, variable_cost_summary,
        green_premium_timeseries, other_opex_df, int_rate=DISCOUNT_RATE
        )
    capex_values = calculate_capex(start_year).swaplevel().loc[plant_tech].groupby('end_technology').sum()
    return capex_values + opex_values / investment_cycle

def capex_values_for_levilised_steelmaking(capex_df: pd.DataFrame, int_rate: float, year: int, payments: int):
    df_temp = capex_df.swaplevel().loc[year]
    value_list = []
    for tech_row in df_temp.itertuples():
        capex_value = - generate_capex_financial_summary(tech_row.value, int_rate, payments)['total_interest']
        value_list.append(capex_value)
    df_temp.drop(['value'], axis=1, inplace=True)
    df_temp['value'] = value_list
    return df_temp


def levelised_steelmaking_cost(year: pd.DataFrame, include_greenfield: bool = False):
    # Levelised of cost of steelmaking = OtherOpex + VariableOpex + RenovationCapex w/ 7% over 20 years (+ GreenfieldCapex w/ 7% over 40 years)
    df_list = []
    all_plant_variable_costs = plant_variable_costs()
    all_plant_variable_costs_summary = format_variable_costs(all_plant_variable_costs)
    opex_values_dict = read_pickle_folder(PKL_FOLDER, 'capex_dict', 'df')
    for iteration in all_plant_variable_costs_summary.index.get_level_values(0).unique():
        variable_costs = all_plant_variable_costs_summary.loc[iteration, year]
        other_opex = opex_values_dict['other_opex'].swaplevel().loc[year]
        brownfield_capex = capex_values_for_levilised_steelmaking(opex_values_dict['brownfield'], DISCOUNT_RATE, year, 20)
        variable_costs.rename(mapper={'cost': 'value'}, axis=1, inplace=True)
        variable_costs.rename(mapper={'technology': 'Technology'}, axis=0, inplace=True)
        combined_df = variable_costs + other_opex + brownfield_capex
        if include_greenfield:
            greenfield_capex = capex_values_for_levilised_steelmaking(opex_values_dict['greenfield'], DISCOUNT_RATE, year, 40)
            combined_df = variable_costs + other_opex + greenfield_capex + brownfield_capex
        combined_df['iteration'] = iteration
        df_list.append(combined_df)
    df = pd.concat(df_list)
    return df.reset_index().rename(mapper={'index': 'technology'},axis=1).set_index(['iteration', 'technology'])

def normalise_data(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))


def scale_data(df: pd.DataFrame, cols = list):
    df_c = df.copy()[cols]
    df_c[cols] = 1 - normalise_data(df_c[cols].values)
    return df_c

def tco_ranker_logic(x: float, min_value: float):
    if min_value is None: # check for this
        # print('NoneType value')
        return 1
    if x > min_value * TCO_RANK_2_SCALER:
        return 3
    elif x > min_value * TCO_RANK_1_SCALER:
        return 2
    else:
        return 1

def abatement_ranker_logic(x: float):
    if x < ABATEMENT_RANK_3:
        return 3
    elif x < ABATEMENT_RANK_2:
        return 2
    else:
        return 1

def tco_min_ranker(df: pd.DataFrame, value_col: list, rank_only: bool = False):
    df_c = df.copy().sort_values(value_col, ascending=False).copy()
    min_value = df_c.min().values[0]
    if rank_only:
        df_c['rank_score'] = df_c[value_col[0]].apply(lambda x: tco_ranker_logic(x, min_value))
        df_c.drop(value_col, axis=1, inplace=True)
        return df_c
    return df_c

def abatement_min_ranker(df: pd.DataFrame, start_tech: str, year: int, cols: list, rank_only: bool = False):
    df_c = df.copy()
    df_subset = df_c.loc[year, start_tech][cols].sort_values(cols, ascending=False).copy()
    if rank_only:
        df_subset['rank_score'] = df_subset[cols[0]].apply(lambda x: abatement_ranker_logic(x)) # get fix for list subset
        df_subset.drop(cols, axis=1, inplace=True)
        return df_subset
    return df_subset


def overall_scores(
    tco_df: pd.DataFrame,
    emissions_df: pd.DataFrame,
    proportions_dict: dict,
    steel_demand_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    biomass_df: pd.DataFrame,
    ccs_co2_df: pd.DataFrame,
    plant_capacities: dict,
    materials_list: list,
    year: int,
    plant_name: str,
    base_tech: str = '',
    tech_moratorium: bool = False,
    transitional_switch_only: bool = False,
    material_usage_dict_container: dict = {},
    return_container: bool = True,
):
    tco_df_c = tco_df.copy().sort_index()
    emissions_df_c = emissions_df.copy().sort_index()
    new_df = tco_df_c.copy()
    new_df['combined_score'] = ( proportions_dict['tco'] * tco_df_c['rank_score'] ) + ( proportions_dict['emissions'] * emissions_df_c['rank_score'] )

    tech_availability = read_and_format_tech_availability()
    # Availability checks
    unavailable_techs = [tech for tech in tco_df.index if not tech_availability_check(tech_availability, tech, year, tech_moratorium=tech_moratorium)]

    if base_tech in unavailable_techs:
        print(f'Current tech {base_tech} is unavailable in {year}, but including anyway')
        unavailable_techs.remove(base_tech)

    # Constraints checks
    constraints_check = plant_tech_resource_checker(
        plant_name, base_tech, year, steel_demand_df,
        business_cases, materials_list, biomass_df,
        ccs_co2_df, TECH_MATERIAL_CHECK_DICT,
        RESOURCE_CONTAINER_REF, material_usage_dict_container,
        plant_capacities, 'excluded'
        )
    
    # Non_switches
    excluded_switches = [key for key in SWITCH_DICT.keys() if key not in SWITCH_DICT[base_tech]]

    # Drop excluded techs
    combined_unavailable_list = list(set(unavailable_techs + constraints_check + excluded_switches))
    new_df.drop(combined_unavailable_list, inplace=True)

    # Transitional switches
    if transitional_switch_only:
        # Cannot downgrade tech
        # Must be current or transitional tech
        transitional_switch_possibilities = TECHNOLOGY_STATES['current'] + TECHNOLOGY_STATES['transitional']
        matches = set(transitional_switch_possibilities).intersection(set(new_df.index))
        # Must be within the furnace group
        furnace_matches = matches.intersection(set(return_furnace_group(FURNACE_GROUP_DICT, base_tech)))
        new_df = new_df.loc[furnace_matches]

    new_df.drop(['rank_score'], axis=1, inplace=True)
    if return_container:
        return new_df.sort_values('combined_score', ascending=False), material_usage_dict_container
    return new_df.sort_values('combined_score', ascending=False)



def choose_technology(
    plant_df: pd.DataFrame, investment_year_ref: pd.DataFrame, year_end: int, rank_only: bool = False, off_cycle_investment: bool = False, tech_moratorium: bool = False, error_plant: str = ''):
    

    logger.info('Creating Steel plant df')
    def plant_name_check(plant_name: str, name_to_check: str, extra: str = ''):
        if plant_name == name_to_check:
            print(f'{plant_name} : {extra}')
            print(plant_df[plant_df['plant_name'] == plant_name]['technology_in_2020'].values[0])

    plant_df_c = plant_df.copy()

    steel_demand_df = extend_steel_demand(MODEL_YEAR_END)
    carbon_tax_df = read_pickle_folder(PKL_FOLDER, 'carbon_tax', 'df')
    all_plant_variable_costs = plant_variable_costs()
    all_plant_variable_costs_summary = format_variable_costs(all_plant_variable_costs)
    biomass_availability = read_pickle_folder(PKL_FOLDER, 'biomass_availability', 'df')
    ccs_co2 = read_pickle_folder(PKL_FOLDER, 'ccs_co2', 'df')
    green_premium_timeseries = timeseries_generator(2020,year_end,GREEN_PREMIUM_MIN_PCT,GREEN_PREMIUM_MAX_PCT,'pct')
    emissions_switching_df_summary = read_pickle_folder(PKL_FOLDER, 'emissions_switching_df_summary', 'df')
    materials = load_materials()
    opex_values_dict = read_pickle_folder(PKL_FOLDER, 'capex_dict', 'df')
    business_cases = load_business_cases()
    plant_capacities_dict = create_plant_capacities_dict()

    all_plant_names = plant_df_c['plant_name'].copy()

    year_range = range(2020, year_end+1)
    current_plant_choices = {}
    for year in tqdm(year_range, total=len(year_range), desc='Tech Choice: Non-Switchers'):
        logger.info(f'Running investment decisions for {year}')
        current_plant_choices[str(year)] = {}

        switchers = extract_tech_plant_switchers(investment_year_ref, year)
        non_switchers = list(set(all_plant_names).difference(switchers))

        switchers_df = plant_df_c.set_index(['plant_name']).drop(non_switchers).reset_index()
        switchers_df.rename({'index': 'plant_name'},axis=1,inplace=True)
        non_switchers_df = plant_df_c.set_index(['plant_name']).drop(switchers).reset_index()
        non_switchers_df.rename({'index': 'plant_name'},axis=1,inplace=True)

        if year == 2020:
            technologies = non_switchers_df['technology_in_2020'].values

        else:
            technologies = current_plant_choices[str(year-1)].values()

        yearly_usage = material_usage_per_plant(non_switchers, technologies, business_cases, plant_capacities_dict, steel_demand_df, materials, year)
        material_usage_dict = load_resource_usage_dict(yearly_usage)
        logger.info(f'-- Running investment decisions for Non Switching Plants')
        for plant_name in non_switchers:
            # plant_name_check(plant_name, error_plant, 'Non Switch Year')
            if year == 2020:
                # plant_name_check(plant_name, error_plant, 'Non Switch Year: 2020')
                tech_in_2020 = non_switchers_df[non_switchers_df['plant_name'] == plant_name]['technology_in_2020'].values[0]
                current_plant_choices[str(year)][plant_name] = tech_in_2020
            else:
                # plant_name_check(plant_name, error_plant, 'Non Switch Year: Non-2020')
                current_plant_choices[str(year)][plant_name] = current_plant_choices[str(year-1)][plant_name]

        logger.info(f'-- Running investment decisions for Switching Plants')
        for plant in tqdm(switchers_df.itertuples(), total=switchers_df.shape[0], desc='Tech Choice: Switchers'):
            plant_name = plant.plant_name

            # plant_name_check(plant_name, error_plant, 'Switch Year')

            if year == 2020:
                # plant_name_check(plant_name, error_plant, 'Switch Year 2020')
                tech_in_2020 = switchers_df[switchers_df['plant_name'] == plant_name]['technology_in_2020'].values[0]
                current_tech = tech_in_2020

            else:
                # plant_name_check(plant_name, error_plant, 'Switch Year Non-2020')
                current_tech = current_plant_choices[str(year-1)][plant_name]

            if current_tech == 'Not operating' or 'Close plant':
                # plant_name_check(plant_name, error_plant, 'Closed')
                current_plant_choices[str(year)][plant_name] = 'Close plant'
                pass

            else:

                switch_type = investment_year_ref.reset_index().set_index(['year', 'plant_name']).loc[year, plant_name].values[0]

                tco = tco_calc(
                    plant, year, current_tech, carbon_tax_df, plant_df_c, 
                    all_plant_variable_costs_summary, green_premium_timeseries, 
                    opex_values_dict['other_opex'])

                tco_switching_df_summary_final_rank = tco_min_ranker(tco, ['value'], rank_only)
                emissions_switching_df_summary_final_rank = abatement_min_ranker(emissions_switching_df_summary, current_tech, year, ['abated_s1_emissions'], rank_only)

                if switch_type == 'main cycle':
                    # plant_name_check(plant_name, error_plant, 'Main cycle')
                    best_score_tech = ''
                    scores, material_usage_dict = overall_scores(
                        tco_switching_df_summary_final_rank,
                        emissions_switching_df_summary_final_rank,
                        SWITCH_RANK_PROPORTIONS,
                        steel_demand_df,
                        business_cases,
                        biomass_availability,
                        ccs_co2,
                        plant_capacities_dict,
                        materials,
                        year,
                        plant_name,
                        current_tech,
                        tech_moratorium=tech_moratorium,
                        material_usage_dict_container=material_usage_dict,
                        return_container=True
                    )
                    try:
                        # Improve this!!!
                        best_score_tech = scores.index[0]
                        if best_score_tech == current_tech:
                            print(f'No change in main investment cycle in {year} for {plant_name} | {year} -> {current_tech} to {best_score_tech}')

                        else:
                            print(f'Regular change in main investment cycle in {year} for {plant_name} | {year} -> {current_tech} to {best_score_tech}')
                        current_plant_choices[str(year)][plant_name] = best_score_tech
                    except:
                        print(f'Error in ranking in {year} for {plant_name} with {current_tech}')
                        current_plant_choices[str(year)][plant_name] = current_tech

                if switch_type == 'trans switch':
                    # plant_name_check(plant_name, error_plant, 'Trans switch')
                    best_score_tech = ''
                    scores, material_usage_dict = overall_scores(
                        tco_switching_df_summary_final_rank,
                        emissions_switching_df_summary_final_rank,
                        SWITCH_RANK_PROPORTIONS,
                        steel_demand_df,
                        business_cases,
                        biomass_availability,
                        ccs_co2,
                        plant_capacities_dict,
                        materials,
                        year,
                        plant_name,
                        current_tech,
                        tech_moratorium=tech_moratorium,
                        transitional_switch_only=True,
                        material_usage_dict_container=material_usage_dict,
                        return_container=True
                    )
                    try:
                        # Change this!!
                        best_score_tech = scores.index[0]
                        if best_score_tech != current_tech:
                            print(f'Transistional switch flipped for {plant_name} in {year} -> {current_tech} to {best_score_tech}')
                        else:
                            print(f'{plant_name} kept its current tech {current_tech} in transitional year {year}')

                        current_plant_choices[str(year)][plant_name] = best_score_tech

                    except:
                        print(f'Error in ranking in {year} for {plant_name} with {current_tech}')
                        current_plant_choices[str(year)][plant_name] = current_tech

    return current_plant_choices

def material_usage_per_plant(
    plant_list: list, technology_list: list,
    business_cases: pd.DataFrame,
    plant_capacities: dict, steel_demand_df: pd.DataFrame,
    materials_list: list, year: float):
    df_list = []
    zipped_data = zip(plant_list, technology_list)
    steel_demand = steel_demand_value_selector(steel_demand_df, 'Crude', year, 'bau')
    capacity_sum = total_plant_capacity(plant_capacities)
    for plant_name, tech in zipped_data:
        plant_capacity = calculate_primary_and_secondary(plant_capacities, plant_name, tech) / 1000
        projected_production = (plant_capacity / capacity_sum) * steel_demand
        df = pd.DataFrame(index=materials_list, columns=['value'])
        for material in materials_list:
            usage_value = material_usage_summary(business_cases, material, tech)
            df.loc[material, 'value'] = projected_production * usage_value
        df_list.append(df)
    return pd.concat(df_list).reset_index().groupby(['index']).sum()


def extract_tech_plant_switchers(inv_cycle_ref: pd.DataFrame, year: int, combined_output: bool = True):
    try:
        main_switchers = inv_cycle_ref.loc[year, 'main cycle']['plant_name'].to_list()
    except KeyError:
        pass
    try:
        trans_switchers = inv_cycle_ref.loc[year, 'trans switch']['plant_name'].to_list()
    except KeyError:
        pass
    if combined_output:
        return main_switchers + trans_switchers
    return main_switchers, trans_switchers

def load_resource_usage_dict(yearly_usage_df: pd.DataFrame):
    resource_usage_dict = create_new_material_usage_dict(RESOURCE_CONTAINER_REF)
    resource_usage_dict['biomass'] = list({yearly_usage_df.loc['Biomass']['value'] or 0})
    resource_usage_dict['scrap'] = list({yearly_usage_df.loc['Scrap']['value'] or 0})
    resource_usage_dict['used_co2'] = list({yearly_usage_df.loc['Used CO2']['value'] or 0})
    resource_usage_dict['captured_co2'] = list({yearly_usage_df.loc['Captured CO2']['value'] or 0})
    return resource_usage_dict


def steel_demand_value_selector(df: pd.DataFrame, steel_type: str, year: int, output_type: str = ''):
    df_c = df.copy()
    def steel_demand_getter(df, steel_type, scenario, year):
        val = df[ (df['Year'] == year) & (df['Steel Type'] == steel_type) & (df['Scenario'] == scenario) ]['Value'].values[0]
        return val
    bau = steel_demand_getter(df_c, steel_type, 'BAU', year)
    circ = steel_demand_getter(df_c, steel_type, 'Circular', year)
    if output_type == 'bau':
        return bau
    if output_type == 'circular':
        return circ
    if output_type == 'combined':
        return bau + circ / 2

def extend_steel_demand(year_end: int):
    logger.info(f'-- Extedning the Steel Demand DataFrame to {year_end}')
    scenarios = ['Circular', 'BAU']
    steel_types = ['Crude', 'Scrap']
    steel_demand_perms = create_list_permutations(steel_types, scenarios)
    global_demand = read_pickle_folder(PKL_FOLDER, 'steel_demand', 'df')
    df_list = []
    for permutation in steel_demand_perms:
        steel_type = permutation[0]
        scenario = permutation[1]
        if steel_type == 'Crude' and scenario == 'BAU':
            series_type = 'geometric'
            growth_type = 'fixed'
            value_change = 2850
        if steel_type == 'Crude' and scenario == 'Circular':
            series_type = 'linear'
            growth_type = 'fixed'
            value_change = 1500
        if steel_type == 'Scrap' and scenario == 'BAU':
            series_type = 'geometric'
            growth_type = 'pct'
            value_change = 15
        if steel_type == 'Scrap' and scenario == 'Circular':
            series_type = 'geometric'
            growth_type = 'pct'
            value_change = 20
        df = full_model_flow(
            df=global_demand[(global_demand['Steel Type'] == steel_type) & (global_demand['Scenario'] == scenario)],
            year_value_col_dict={'year': 'Year', 'value': 'Value'},
            static_value_override_dict={'Source': 'RMI + Model Extension beyond 2050', 'Excel Tab': 'Extended from Excel'},
            new_end_year = year_end,
            series_type = series_type,
            growth_type = growth_type,
            value_change = value_change,
        )
        df_list.append(df)
    return pd.concat(df_list).reset_index(drop=True)

def format_bc(df: pd.DataFrame):
    df_c = df.copy()
    df_c = df_c[df_c['material_category'] != 0]
    df_c['material_category'] = df_c['material_category'].apply(lambda x: x.strip())
    return df_c

def create_emissions_dict():
    calculated_s1_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s1_emissions', 'df')
    calculated_s2_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s2_emissions', 'df')
    calculated_s3_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s3_emissions', 'df')
    return {'s1': calculated_s1_emissions, 's2': calculated_s2_emissions, 's3': calculated_s3_emissions}

def load_materials():
    return load_business_cases()['material_category'].unique()

def generate_formatted_steel_plants():
    # Notice this comes from the raw steel plant file - fix in script
    steel_plants_raw = read_pickle_folder(PKL_FOLDER, 'steel_plants', 'df')
    steel_plants_raw_c = format_steel_plant_df(steel_plants_raw)
    steel_plants_aug = extract_steel_plant_capacity(steel_plants_raw_c)
    return steel_plants_aug[steel_plants_aug['technology_in_2020'] != 'Not operating'].reset_index(drop=True)

def generate_feedstock_dict():
    commodities_df = read_pickle_folder(PKL_FOLDER, 'commodities_df', 'df')
    feedstock_prices = read_pickle_folder(PKL_FOLDER, 'feedstock_prices', 'df')
    commodities_dict = commodity_data_getter(commodities_df)
    commodity_dictname_mapper = {'plastic': 'Plastic waste', 'ethanol': 'Ethanol', 'charcoal': 'Charcoal'}
    for key in commodity_dictname_mapper.keys():
        commodities_dict[commodity_dictname_mapper[key]] = commodities_dict.pop(key)
    return {**commodities_dict, **dict(zip(feedstock_prices['Metric'], feedstock_prices['Value']))}

def load_business_cases():
    standardised_business_cases = read_pickle_folder(PKL_FOLDER, 'standardised_business_cases', 'df')
    return format_bc(standardised_business_cases)

def solver_flow(year_end: int, serialize_only: bool = False):

    steel_plants_aug = generate_formatted_steel_plants()

    plant_investment_cycles = read_pickle_folder(PKL_FOLDER, 'plant_investment_cycles', 'df')

    tech_choice_dict = choose_technology(
        plant_df=steel_plants_aug, investment_year_ref=plant_investment_cycles, year_end=year_end,
        rank_only=True, off_cycle_investment=True, tech_moratorium=True, error_plant='SSAB Americas Alabama steel plant')

    if serialize_only:
        logger.info(f'-- Serializing dataframes')
        serialise_file(tech_choice_dict, IMPORT_DATA_PATH, "tech_choice_dict")
    return tech_choice_dict
