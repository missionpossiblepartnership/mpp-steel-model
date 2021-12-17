"""Investment Results generator for technology investments"""

import pandas as pd
from tqdm import tqdm

from mppsteel.model_config import (
    MODEL_YEAR_START,
    PKL_FOLDER
)

from mppsteel.model.solver import (
    generate_formatted_steel_plants,
)

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger, serialize_file
)

# Create logger
logger = get_logger("Investment Results")

def create_capex_dict():
    capex = read_pickle_folder(PKL_FOLDER, 'capex_switching_df', 'df')
    capex_c = capex.copy()
    capex_c.reset_index(inplace=True)
    capex_c.columns = [col.lower().replace(' ', '_') for col in capex_c.columns]
    return capex_c.set_index(['year', 'start_technology']).sort_index()

def get_capex_ref(capex_df: pd.DataFrame, year: int, start_tech: str, new_tech: str):
    capex_year = year
    if year > 2050:
        capex_year = 2050
    if new_tech == 'Close plant':
        return 0
    capex_ref = capex_df.loc[capex_year, start_tech]
    return capex_ref.loc[capex_ref['new_technology'] == new_tech]['value'].values[0]

def investment_switch_getter(inv_df: pd.DataFrame, year: int, plant_name: str):
    inv_df_ref = inv_df.reset_index().set_index(['year', 'plant_name']).sort_values(['year'])
    return inv_df_ref.loc[year, plant_name].values[0]

def get_tech_choice(tc_dict: dict, year: int, plant_name: str):
    return tc_dict[str(year)][plant_name]

def investment_row_calculator(inv_df: pd.DataFrame, capex_df: pd.DataFrame, tech_choices: dict, plant_name: str, year: int):
    switch_type = investment_switch_getter(inv_df, year, plant_name)

    if year == 2020:
        start_tech = get_tech_choice(tech_choices, 2020, plant_name)
    else:
        start_tech = get_tech_choice(tech_choices, year-1, plant_name)

    new_tech = get_tech_choice(tech_choices, year, plant_name)
    capex_ref = get_capex_ref(capex_df, year, start_tech, new_tech)
    new_row = {
        'plant' : plant_name,
        'year': year,
        'start_tech': start_tech,
        'end_tech': new_tech,
        'switch_type': switch_type,
        'capital_cost': capex_ref,
    }
    return new_row

def investment_results(serialize_only: bool = False):
    logger.info(f'Generating Investment Results')
    tech_choice_dict = read_pickle_folder(PKL_FOLDER, 'tech_choice_dict', 'df')
    plant_investment_cycles = read_pickle_folder(PKL_FOLDER, 'plant_investment_cycles', 'df')
    steel_plant_df = generate_formatted_steel_plants()
    plant_names = steel_plant_df['plant_name'].values
    capex_df = create_capex_dict()
    max_year = max([int(year) for year in tech_choice_dict.keys()])
    year_range = range(MODEL_YEAR_START, max_year+1)
    data_container = []
    for plant_name in tqdm(plant_names, desc='Steel Plant Investments'):
        for year in year_range:
            data_container.append(
                investment_row_calculator(
                    plant_investment_cycles, capex_df, tech_choice_dict, plant_name, year
                    ))
    investment_results_df = pd.DataFrame(data_container).set_index(['year']).sort_values('year')
    if serialize_only:
        logger.info(f'-- Serializing dataframes')
        serialize_file(investment_results_df, PKL_FOLDER, "investment_results_df")
    return investment_results_df
