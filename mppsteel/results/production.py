"""Production Results generator for technology investments"""

import pandas as pd
from tqdm import tqdm

from mppsteel.model_config import (
    IMPORT_DATA_PATH, LOW_CARBON_TECHS,
    AVERAGE_LEVEL_OF_CAPACITY, 
    MODEL_YEAR_START, MODEL_YEAR_END, PKL_FOLDER
)

from mppsteel.model.solver import (
    calculate_primary_and_secondary, load_materials,
    steel_demand_value_selector, extend_steel_demand,
    load_business_cases, create_plant_capacities_dict,
)

from mppsteel.model.tco import (
    create_emissions_dict
)

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger, serialize_file
)

# Create logger
logger = get_logger("Production Results")

def generate_production_stats(
    tech_capacity_df: pd.DataFrame, steel_df: pd.DataFrame, year_end: int):
    logger.info(f'- Generating Production Results from capacity')
    df_list = []
    year_range = range(MODEL_YEAR_START, year_end+1)
    for year in tqdm(year_range, total=len(year_range), desc='Production Stats'):
        df = tech_capacity_df[tech_capacity_df['year'] == year].copy()
        capacity_sum = df['capacity'].sum()
        steel_demand = steel_demand_value_selector(steel_df, 'Crude', year, 'bau')
        df['production'] = (df['capacity'] / capacity_sum) * steel_demand
        df['low_carbon_tech'] = df['technology'].apply(lambda tech: 'Y' if tech in LOW_CARBON_TECHS else 'N')
        df_list.append(df)
    return pd.concat(df_list).reset_index(drop=True)


def tech_capacity_splits():

    logger.info(f'- Generating Capacity split DataFrame')
    tech_capacities_dict = create_plant_capacities_dict()
    tech_choices_dict = read_pickle_folder(PKL_FOLDER, 'tech_choice_dict', 'df')
    max_year = max([int(year) for year in tech_choices_dict.keys()])
    steel_plants = tech_capacities_dict.keys()
    year_range = range(MODEL_YEAR_START, max_year+1)
    df_list = []

    for year in tqdm(year_range, total=len(year_range), desc='Tech Capacity Splits'):
        df = pd.DataFrame({'year': year, 'steel_plant': steel_plants, 'technology': '', 'capacity': 0})
        df['technology'] = df['steel_plant'].apply(lambda plant: get_tech_choice(tech_choices_dict, year, plant))
        for row in df.itertuples():
            df.loc[row.Index, 'capacity'] = calculate_primary_and_secondary(tech_capacities_dict, row.steel_plant, row.technology) / 1000
        df = df[df['technology'] != 'Not operating']
        df_list.append(df)

    return pd.concat(df_list), max_year

def production_stats_generator(production_df: pd.DataFrame, as_summary: bool = False):
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

def generate_production_emission_stats(production_df: pd.DataFrame, as_summary: bool = False):
    logger.info(f'- Generating Material Usage stats')
    emissions_dict = create_emissions_dict()

    def emissions_getter(emission_dict: dict, emission_type: str, tech: str, year: int):
        if year > 2050:
            year = 2050
        return emission_dict[emission_type].loc[year, tech].values[0]

    df_c = production_df.copy()

    # Create columns
    for colname in emissions_dict.keys():
        df_c[f'{colname}_emissions'] = 0

        # Create values
    for row in tqdm(df_c.itertuples(), total=df_c.shape[0], desc='Production Emissions'):
        if row.technology == 'Close plant':
            for colname in emissions_dict.keys():
                df_c.loc[row.Index, f'{colname}_emissions'] = 0
        else:
            for colname in emissions_dict.keys():
                df_c.loc[row.Index, f'{colname}_emissions'] = row.production * emissions_getter(emissions_dict, colname, row.technology, row.year)

    if as_summary:
        return df_c.groupby(['year', 'technology']).sum()
    return df_c

def global_metaresults_calculator(
    steel_market_df: pd.DataFrame,
    tech_capacity_df: pd.DataFrame,
    production_results_df: pd.DataFrame,
    year_end,
    ):
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
    df['steel_demand'] = df['year'].apply(lambda year: steel_demand_value_selector(steel_market_df, 'Crude', year, 'bau'))
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
    df['scrap_availability'] = df['year'].apply(lambda year: steel_demand_value_selector(steel_market_df, 'Scrap', year, 'bau'))
    df['scrap_consumption'] = [production_results_df.loc[year]['scrap'].sum() for year in year_range]
    df['scrap_avail_above_cons'] = df['scrap_availability'] - df['scrap_consumption']
    return df

def business_case_getter(df: pd.DataFrame, tech: str, material: str):
    if material in df[(df['technology'] == tech)]['material_category'].unique():
        return df[(df['technology'] == tech) & (df['material_category'] == material)]['value'].values[0]
    return 0

def get_tech_choice(tc_dict: dict, year: int, plant_name: str):
    return tc_dict[str(year)][plant_name]

def load_materials_mapper():
    materials = load_materials()
    material_col_names = [material.lower().replace(' ', '_') for material in materials]
    return dict(zip(materials, material_col_names))

def production_results_flow(serialize_only: bool = False):
    logger.info(f'- Starting Production Results Model Flow')
    new_steel_demand = extend_steel_demand(MODEL_YEAR_END)
    tech_capacity_df, max_solver_year = tech_capacity_splits()
    production_results = generate_production_stats(tech_capacity_df, new_steel_demand, max_solver_year)

    production_results_all = production_stats_generator(production_results)
    production_emissions = generate_production_emission_stats(production_results)
    global_metaresults = global_metaresults_calculator(
        new_steel_demand, tech_capacity_df, production_results_all, max_solver_year)

    results_dict = {
        'production_results_all': production_results_all,
        'production_emissions': production_emissions,
        'global_metaresults': global_metaresults
        }

    if serialize_only:
        logger.info(f'-- Serializing dataframes')
        serialize_file(production_results_all, IMPORT_DATA_PATH, "production_stats_all")
        serialize_file(production_emissions, IMPORT_DATA_PATH, "production_emissions")
        serialize_file(global_metaresults, IMPORT_DATA_PATH, "global_metaresults")
    return results_dict
