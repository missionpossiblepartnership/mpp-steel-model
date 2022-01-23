"""Production Results generator for technology investments"""

import pandas as pd
from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.model_config import (
    AVERAGE_LEVEL_OF_CAPACITY,
    MODEL_YEAR_START, MODEL_YEAR_END,
    PKL_DATA_INTERMEDIATE, PKL_DATA_FINAL
)

from mppsteel.utility.reference_lists import LOW_CARBON_TECHS

from mppsteel.model.solver import (
    load_materials, load_business_cases, create_plant_capacities_dict,
)

from mppsteel.model.solver_constraints import (
    calculate_primary_and_secondary
)

from mppsteel.data_loading.reg_steel_demand_formatter import (
    steel_demand_getter
)

from mppsteel.data_loading.data_interface import (
    load_business_cases
)

from mppsteel.model.tco_calculation_functions import get_s2_emissions

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger, serialize_file, timer_func,
    add_results_metadata, enumerate_columns
)

# Create logger
logger = get_logger("Production Results")

def create_emissions_dict():
    calculated_s1_emissions = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'calculated_s1_emissions', 'df')
    calculated_s3_emissions = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'calculated_s3_emissions', 'df')
    return {'s1': calculated_s1_emissions, 's3': calculated_s3_emissions}

def generate_production_stats(
    tech_capacity_df: pd.DataFrame, steel_df: pd.DataFrame, steel_demand_scenario: str, year_end: int):
    """[summary]

    Args:
        tech_capacity_df (pd.DataFrame): [description]
        steel_df (pd.DataFrame): [description]
        year_end (int): [description]

    Returns:
        [type]: [description]
    """    
    logger.info(f'- Generating Production Results from capacity')
    df_list = []
    year_range = range(MODEL_YEAR_START, year_end+1)
    for year in tqdm(year_range, total=len(year_range), desc='Production Stats'):
        df = tech_capacity_df[tech_capacity_df['year'] == year].copy()
        capacity_sum = df['capacity'].sum()
        steel_demand = steel_demand_getter(steel_df, year, steel_demand_scenario, 'crude', 'World')
        df['production'] = (df['capacity'] / capacity_sum) * steel_demand
        df['low_carbon_tech'] = df['technology'].apply(lambda tech: 'Y' if tech in LOW_CARBON_TECHS else 'N')
        df_list.append(df)
    return pd.concat(df_list).reset_index(drop=True)


def tech_capacity_splits():
    """[summary]

    Returns:
        [type]: [description]
    """    
    logger.info(f'- Generating Capacity split DataFrame')
    tech_capacities_dict = create_plant_capacities_dict()
    tech_choices_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'tech_choice_dict', 'df')
    steel_plant_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'steel_plants_processed', 'df')
    steel_plant_dict = dict(zip(steel_plant_df['plant_name'].values, steel_plant_df['country_code'].values))
    max_year = max([int(year) for year in tech_choices_dict.keys()])
    steel_plants = tech_capacities_dict.keys()
    year_range = range(MODEL_YEAR_START, max_year+1)
    df_list = []

    def value_mapper(row, enum_dict):
        row[enum_dict['capacity']] = calculate_primary_and_secondary(tech_capacities_dict, row[enum_dict['steel_plant']], row[enum_dict['technology']]) / 1000
        return row

    for year in tqdm(year_range, total=len(year_range), desc='Tech Capacity Splits'):
        df = pd.DataFrame({'year': year, 'steel_plant': steel_plants, 'technology': '', 'capacity': 0})
        df['technology'] = df['steel_plant'].apply(lambda plant: get_tech_choice(tech_choices_dict, year, plant))
        tqdma.pandas(desc="Technology Capacity  Splits")
        enumerated_cols = enumerate_columns(df.columns)
        df = df.progress_apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
        df_list.append(df)

    df_combined = pd.concat(df_list)
    df_combined['country_code'] = df['steel_plant'].apply(lambda plant: steel_plant_dict[plant])

    return df_combined, max_year

def production_stats_generator(production_df: pd.DataFrame, as_summary: bool = False):
    """[summary]
    Args:
        production_df (pd.DataFrame): [description]
        as_summary (bool, optional): [description]. Defaults to False.
    Returns:
        [type]: [description]
    """    
    logger.info(f'- Generating Material Usage stats')
    df_c = production_df.copy()
    material_dict_mapper = load_materials_mapper()
    standardised_business_cases = load_business_cases()

    # Create columns
    for colname in material_dict_mapper.values():
        df_c[colname] = 0
    df_c['power'] = 0

    # Create values
    for row in tqdm(df_c.itertuples(), total=df_c.shape[0], desc='Production Stats Generator'):     
        for item in material_dict_mapper.items():
            material_category = item[0]
            new_colname = item[1]
            if material_category == 'BF slag':
                df_c.loc[row.Index, new_colname] = row.production * business_case_getter(standardised_business_cases, row.technology, material_category) / 1000
            elif material_category == 'Met coal':
                df_c.loc[row.Index, new_colname] = row.production * business_case_getter(standardised_business_cases, row.technology, material_category) * 28
            elif material_category == 'Hydrogen':
                df_c.loc[row.Index, new_colname] = row.production * business_case_getter(standardised_business_cases, row.technology, material_category) / 3.6
            else:
                df_c.loc[row.Index, new_colname] = row.production * business_case_getter(standardised_business_cases, row.technology, material_category)
        # Create power column
        electricity_value = business_case_getter(standardised_business_cases, row.technology, 'Electricity')
        df_c.loc[row.Index, 'power'] = row.production * electricity_value / 3.6

    df_c['bioenergy'] = df_c['biomass'] + df_c['biomethane']
    if as_summary:
        return df_c.groupby(['year', 'technology']).sum()
    return df_c

def generate_production_emission_stats(
    production_df: pd.DataFrame, 
    power_model: pd.DataFrame,
    hydrogen_model: pd.DataFrame,
    business_cases: pd.DataFrame,
    as_summary: bool = False,
    electricity_cost_scenario: str = 'average',
    grid_scenario: str = 'low',
    hydrogen_cost_scenario: str =  'average',
    ):
    """[summary]

    Args:
        production_df (pd.DataFrame): [description]
        as_summary (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """
    logger.info(f'- Producing Production Emission Stats')
    emissions_dict = create_emissions_dict()
    emissions_name_ref = ['s1', 's2', 's3']

    def emissions_getter(
        emission_dict: dict, emission_type: str, tech: str, year: int):
        if year > 2050:
            year = 2050
        return emission_dict[emission_type].loc[year, tech].values[0]

    df_c = production_df.copy()

    def value_mapper(row, enum_dict):
        if row[enum_dict['technology']] == 'Close plant':
            for colname in emissions_name_ref:
                row[enum_dict[df_colname]] = 0
        else:
            for colname in emissions_name_ref:
                if colname in ['s1', 's3']:
                    row[enum_dict[df_colname]] = row[enum_dict['production']] * emissions_getter(
                        emissions_dict, colname, row[enum_dict['technology']], row[enum_dict['year']])
                elif colname == 's2':
                    row[enum_dict[df_colname]] = row[enum_dict['production']] * get_s2_emissions(
                        power_model, hydrogen_model, business_cases,
                        row[enum_dict['year']], row[enum_dict['country_code']], row[enum_dict['technology']],
                        electricity_cost_scenario, grid_scenario, hydrogen_cost_scenario)
        return row
    # Create columns
    for colname in emissions_name_ref:
        df_colname = f'{colname}_emissions'
        df_c[df_colname] = 0

    # Create values
    tqdma.pandas(desc="Generate Production Emission Stats")
    enumerated_cols = enumerate_columns(df_c.columns)
    df_c = df_c.progress_apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)

    if as_summary:
        return df_c.groupby(['year', 'technology']).sum()
    return df_c

def global_metaresults_calculator(
    steel_market_df: pd.DataFrame,
    tech_capacity_df: pd.DataFrame,
    production_results_df: pd.DataFrame,
    steel_demand_scenario: str,
    year_end,
    ):
    """[summary]

    Args:
        steel_market_df (pd.DataFrame): [description]
        tech_capacity_df (pd.DataFrame): [description]
        production_results_df (pd.DataFrame): [description]
        year_end ([type]): [description]

    Returns:
        [type]: [description]
    """
    # Initial Steel capacity values
    logger.info(f'- Generating Global Metaresults')

    steel_capacity_years = {'2020': 2362.5}
    steel_capacity_years['2022'] = steel_capacity_years['2020'] * 1.03
    steel_capacity_years['2021'] = (steel_capacity_years['2020'] + steel_capacity_years['2022']) / 2

    def initial_capacity_assignor(year: int):
        if 2020 <= year <= 2022:
            return steel_capacity_years[str(year)]
        else:
            return 0

    def potential_extra_capacity(capacity_value: float, steel_demand_value: float):
        excess_demand_check = steel_demand_value - (AVERAGE_LEVEL_OF_CAPACITY * capacity_value)
        if excess_demand_check < 0:
            return 0
        return round(excess_demand_check, 3)

    def tech_capacity_summary(tech_capacity_df: pd.DataFrame, year: int):
        return tech_capacity_df[tech_capacity_df['year'] == year][['capacity']].sum().values[0]

    # Base DataFrame
    year_range = list(range(MODEL_YEAR_START, year_end+1))
    df = pd.DataFrame({'year': year_range, 'steel_demand': 0, 'steel_capacity': 0, 'potential_extra_capacity': 0})

    # Assign initial values
    df['steel_demand'] = df['year'].apply(lambda year: steel_demand_getter(steel_market_df, year, steel_demand_scenario, 'crude', 'World'))
    df['steel_capacity'] = df['year'].apply(lambda x: initial_capacity_assignor(x))

    # Assign iterative values
    for row in tqdm(df.itertuples(), total=df.shape[0], desc='Steel Capacity Calculator'):
        if row.year < 2023:
            steel_capacity = df.loc[(df['year'] == row.year), 'steel_capacity'].values[0]
            df.loc[row.Index, 'potential_extra_capacity'] = potential_extra_capacity(steel_capacity, row.steel_demand)
        else:
            prior_capacity_value = df.loc[row.Index-1, 'steel_capacity']
            prior_extra_capacity_value = df.loc[row.Index-1, 'potential_extra_capacity']
            current_year_tech_capacity = tech_capacity_summary(tech_capacity_df, row.year)
            prior_year_tech_capacity = tech_capacity_summary(tech_capacity_df, row.year-1)
            steel_capacity = prior_capacity_value + ((current_year_tech_capacity - prior_year_tech_capacity) * 1000) + prior_extra_capacity_value
            df.loc[row.Index, 'steel_capacity'] = steel_capacity
            df.loc[row.Index, 'potential_extra_capacity'] = potential_extra_capacity(steel_capacity, row.steel_demand)

    df['capacity_utilization_factor'] = (df['steel_demand'] / df['steel_capacity']).round(3)
    df['scrap_availability'] = df['year'].apply(lambda year: steel_demand_getter(steel_market_df, year, steel_demand_scenario, 'crude', 'World'))
    df['scrap_consumption'] = [production_results_df.loc[year]['scrap'].sum() for year in year_range]
    df['scrap_avail_above_cons'] = df['scrap_availability'] - df['scrap_consumption']
    return df

def business_case_getter(df: pd.DataFrame, tech: str, material: str):
    """[summary]

    Args:
        df (pd.DataFrame): [description]
        tech (str): [description]
        material (str): [description]

    Returns:
        [type]: [description]
    """
    if material in df[(df['technology'] == tech)]['material_category'].unique():
        return df[(df['technology'] == tech) & (df['material_category'] == material)]['value'].values
    return 0

def get_tech_choice(tc_dict: dict, year: int, plant_name: str):
    """[summary]

    Args:
        tc_dict (dict): [description]
        year (int): [description]
        plant_name (str): [description]

    Returns:
        [type]: [description]
    """    
    return tc_dict[str(year)][plant_name]

def load_materials_mapper():
    """[summary]

    Returns:
        [type]: [description]
    """    
    materials = load_materials()
    material_col_names = [material.lower().replace(' ', '_') for material in materials]
    return dict(zip(materials, material_col_names))

@timer_func
def production_results_flow(scenario_dict: dict, serialize_only: bool = False):
    """[summary]

    Args:
        serialize_only (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """    
    logger.info(f'- Starting Production Results Model Flow')
    steel_demand_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'regional_steel_demand_formatted', 'df')
    business_cases = load_business_cases()
    power_model = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'power_model_formatted', 'df')
    hydrogen_model = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'hydrogen_model_formatted', 'df')
    tech_capacity_df, max_solver_year = tech_capacity_splits()
    steel_demand_scenario = scenario_dict['steel_demand_scenario']
    electricity_cost_scenario=scenario_dict['electricity_cost_scenario']
    grid_scenario=scenario_dict['grid_scenario']
    hydrogen_cost_scenario=scenario_dict['hydrogen_cost_scenario']

    production_results = generate_production_stats(tech_capacity_df, steel_demand_df, steel_demand_scenario, max_solver_year)
    production_results_all = production_stats_generator(production_results)
    production_emissions = generate_production_emission_stats(
        production_results, power_model, hydrogen_model, business_cases,
        electricity_cost_scenario=electricity_cost_scenario,
        grid_scenario=grid_scenario,
        hydrogen_cost_scenario=hydrogen_cost_scenario)
    global_metaresults = global_metaresults_calculator(
        steel_demand_df, tech_capacity_df, production_results_all, steel_demand_scenario, max_solver_year)

    results_dict = {
        'production_results_all': production_results_all,
        'production_emissions': production_emissions,
        'global_metaresults': global_metaresults
        }

    for key in results_dict.keys():
        if key in ['production_results_all', 'production_emissions']:
            results_dict[key] = add_results_metadata(results_dict[key], scenario_dict)

    if serialize_only:
        logger.info(f'-- Serializing dataframes')
        serialize_file(results_dict['production_results_all'], PKL_DATA_FINAL, "production_stats_all")
        serialize_file(results_dict['production_emissions'], PKL_DATA_FINAL, "production_emissions")
        serialize_file(results_dict['global_metaresults'], PKL_DATA_FINAL, "global_metaresults")
    return results_dict
