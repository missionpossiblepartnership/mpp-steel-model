"""Production Results generator for technology investments"""

import pandas as pd
from tqdm import tqdm

from mppsteel.model_config import (
    IMPORT_DATA_PATH, LOW_CARBON_TECHS,
    AVERAGE_LEVEL_OF_CAPACITY, MODEL_YEAR_END,
    PKL_FOLDER
)

from mppsteel.model.solver import (
    calculate_primary_and_secondary,
    steel_demand_value_selector, extend_steel_demand,
    generate_formatted_steel_plants, load_business_cases,
    create_plant_capacities_dict, create_emissions_dict,
    load_materials
)

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger, serialise_file
)

from mppsteel.data_loading.country_reference import (
    match_country
)

# Create logger
logger = get_logger("Production Results")


def generate_production_stats(
    tech_capacity_df: pd.DataFrame,
    steel_df: pd.DataFrame,
    year_end: int, as_summary: bool = False):

    df_list = []
    for year in range(2020, year_end+1):
        df = tech_capacity_df[tech_capacity_df['year'] == year].copy()
        capacity_sum = df['capacity'].sum()
        steel_demand = steel_demand_value_selector(steel_df, 'Crude', year, 'bau')
        df['production'] = (df['capacity'] / capacity_sum) * steel_demand
        df_list.append(df)

    return pd.concat(df_list).reset_index(drop=True)

def low_carbon_generator(production_df: pd.DataFrame, as_summary: bool = False):
    def low_carbon_check(tech, prod_value):
        if tech in LOW_CARBON_TECHS:
            return prod_value
        return 0

    df_c = production_df.copy()
    df_c['low_carbon_ore_based'] = 0
    for row in df_c.itertuples():
        df_c.loc[row.Index, 'low_carbon_ore_based'] = round(low_carbon_check(row.technology, row.production), 3)
    if as_summary:
        return df_c.groupby(['year', 'technology']).sum()
    return df_c

def tech_capacity_splits( year_end: int):
    # CHANGE THIS FUNCTION WITH OUTPUTS
    tech_capacities = create_plant_capacities_dict()
    tech_choices_dict = generate_tech_choices_dict()
    steel_plants = tech_capacities.keys()
    year_range = range(2020, year_end+1)
    df_list = []

    for year in tqdm(year_range, total=len(year_range)):
        df = pd.DataFrame({'year': year, 'steel_plant': steel_plants, 'technology': '', 'capacity': 0})
        df['technology'] = df['steel_plant'].apply(lambda plant: tech_year_selector(
            tech_choices_dict, tech_capacities, non_operating_plants, plant, year, year_end))
        for row in df.itertuples():
            df.loc[row.Index, 'capacity'] = calculate_primary_and_secondary(tech_capacities, row.steel_plant, row.technology) / 1000
        df = df[df['technology'] != 'Not operating']
        df_list.append(df)

    return pd.concat(df_list)

def production_stats_generator(production_df: pd.DataFrame, as_summary: bool = False):
    df_c = production_df.copy()

    material_dict_mapper = load_materials_mapper()
    standardised_business_cases = load_business_cases()

    # Create columns
    for colname in material_dict_mapper.values():
        df_c[colname] = 0
    df_c['power'] = 0

    # Create values
    for row in tqdm(df_c.itertuples(), total=df_c.shape[0]):     
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
    for row in tqdm(df_c.itertuples(), total=df_c.shape[0]):
        for colname in emissions_dict.keys():
            df_c.loc[row.Index, f'{colname}_emissions'] = row.production * emissions_getter(emissions_dict, colname, row.technology, row.year)

    if as_summary:
        return df_c.groupby(['year', 'technology']).sum()
    return df_c

def steel_capacity_calculator(steel_df: pd.DataFrame, year_end: int = MODEL_YEAR_END):
    # Initial Steel capacity values
    
    tech_capacity_df = generate_production_emission_stats(production_stats)
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
    year_range = list(range(2020, year_end+1))
    df = pd.DataFrame({'year': year_range, 'steel_demand': 0, 'steel_capacity': 0, 'potential_extra_capacity': 0})

    # Assign initial values
    df['steel_demand'] = df['year'].apply(lambda year: steel_demand_value_selector(steel_df, 'Crude', year, 'bau'))
    df['steel_capacity'] = df['year'].apply(lambda x: initial_capacity_assignor(x))

    # Assign iterative values
    for row in df.itertuples():
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
    df['scrap_availability'] = df['year'].apply(lambda year: steel_demand_value_selector(steel_df, 'Scrap', year, 'bau'))
    df['scrap_consumption'] = [scrap_df.loc[year]['scrap_use'].sum() for year in year_range]
    df['scrap_avail_above_cons'] = df['scrap_availability'] - df['scrap_consumption']
    return df

def investment_results():
    steel_plant_choices = generate_tech_choices_dict()
    steel_plant_df = generate_formatted_steel_plants()
    capex_df = create_capex_dict()
    columns = ['plant', 'year', 'start_tech', 'end_tech', 'switch_type', 'capital_cost']
    df = pd.DataFrame(columns=columns)
    for row in tqdm(steel_plant_df.itertuples(), total=steel_plant_df.shape[0]):
        try:
            plant_values = steel_plant_choices[f'{row.plant_name}']
            ticker = 0

            for investment_year in plant_values:
                values = plant_values[investment_year]

                end_tech = values[0]
                switch_type = values[1]

                tech = row.technology_in_2020
                if ticker > 0:
                    tech = latest_tech

                capex_year = investment_year
                if investment_year > 2050:
                    capex_year = 2050
                if investment_year == 2019:
                    investment_year = 2020
                    capex_year = 2020

                capex_ref = capex_df.loc[capex_year, tech]
                try:
                    capex_ref = capex_ref[capex_ref['new_technology'] == end_tech]['value'].values[0]
                except:
                    capex_ref = 0
                new_row = {
                    'plant' : row.plant_name,
                    'year': investment_year,
                    'start_tech': tech,
                    'end_tech': end_tech,
                    'switch_type': switch_type,
                    'capital_cost': capex_ref,
                }
                latest_tech = end_tech
                ticker += 1
                df=df.append(new_row, ignore_index=True)
        except:
            pass
    return df.set_index(['year'])

def business_case_getter(df: pd.DataFrame, tech: str, material: str):
    if material in df[(df['technology'] == tech)]['material_category'].unique():
        return df[(df['technology'] == tech) & (df['material_category'] == material)]['value'].values[0]
    return 0

def tech_year_selector(tech_dict: dict, tech_capacities: dict, non_operating_plants: list, plant: str, year: int, year_end: int = MODEL_YEAR_END):
    if plant in non_operating_plants:
        return 'Not operating'

    plant_dict = tech_dict['plant_choices'][plant]

    # Creating investment years
    investment_years = list(plant_dict.keys())
    first_inv_year = investment_years[0]
    second_inv_year = investment_years[1]
    try:
        third_inv_year = investment_years[2]
    except:
        # print('No third investment year')
        third_inv_year = year_end

    # Main year logic
    if  2020 <= year < first_inv_year:
        # print('between 2020 and first inv year')
        return tech_capacities[plant]['2020_tech']
    elif first_inv_year <= year < second_inv_year:
        # print('between first and second inv years')
        return plant_dict[first_inv_year]
    elif second_inv_year <= year < third_inv_year:
        # print('between second and third inv years')
        return plant_dict[second_inv_year]
    else:
        # print('returning last tech')
        return plant_dict[investment_years[-1]]

def create_capex_dict():
    capex = read_pickle_folder(IMPORT_DATA_PATH, 'capex_switching_df', 'df')
    capex_c = capex.copy()
    capex_c.reset_index(inplace=True)
    capex_c.columns = [col.lower().replace(' ', '_') for col in capex_c.columns]
    return capex_c.set_index(['year', 'start_technology']).sort_index()

def add_regions_to_steel_plants():
    steel_plants_clean = read_pickle_folder(IMPORT_DATA_PATH, 'steel_plants_processed', 'df')
    steel_plants_clean['region'] = steel_plants_clean['country_code'].apply(lambda x: match_country(x))
    return steel_plants_clean

def load_materials_mapper():
    materials = load_materials()
    material_col_names = [material.lower().replace(' ', '_') for material in materials]
    return dict(zip(materials, material_col_names))


def generate_tech_choices_dict():
    tech_choice_dict = read_pickle_folder(PKL_FOLDER, 'tech_choice_dict', 'df')
    return tech_choice_dict['plant_choices']

def model_flow(serialize_only: bool = False):
    investments_results = investment_results()

    new_steel_demand = extend_steel_demand(MODEL_YEAR_END)
    tech_capacity_df = tech_capacity_splits(MODEL_YEAR_END)

    steel_capacity_df_calculated = steel_capacity_calculator(new_steel_demand, tech_capacity_df)
    production_stats = generate_production_stats(steel_capacity_df_calculated, new_steel_demand, MODEL_YEAR_END)
    production_stats_low_carbon = low_carbon_generator(production_stats)
    production_stats_all = production_stats_generator(production_stats_low_carbon)

    if serialize_only:
        logger.info(f'-- Serializing dataframes')
        serialise_file(production_stats_all, IMPORT_DATA_PATH, "production_stats_all")
        serialise_file(investments_results, IMPORT_DATA_PATH, "investments_results")
    return production_stats_all
