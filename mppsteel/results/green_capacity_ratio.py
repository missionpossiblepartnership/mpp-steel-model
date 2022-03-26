"""Calculates Green Technology Capacity Ratios"""

from copy import deepcopy

import pandas as pd

from mppsteel.config.model_config import (
    PKL_DATA_INTERMEDIATE,
    PKL_DATA_FINAL,
)
from mppsteel.config.reference_lists import TECHNOLOGY_STATES
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.location_utility import get_region_from_country_code

# Create logger
logger = get_logger("Green Capacity Ratio")

def green_capacity_ratio_predata(
    plant_df: pd.DataFrame, tech_choices: dict, country_reference_dict: dict, inc_trans: bool = False):

    def tech_status_mapper(tech_choice: dict, inc_trans: bool):
        check_list = deepcopy(TECHNOLOGY_STATES['end_state'])
        if inc_trans:
            check_list = deepcopy(TECHNOLOGY_STATES['end_state'] + TECHNOLOGY_STATES['transitional'])
        if tech_choice in check_list:
            return True
        elif tech_choice in TECHNOLOGY_STATES['current']:
            return False
    
    def fix_start_year(start_year):
        if pd.isna(start_year):
            return 2020
        elif "(anticipated)" in str(start_year):
            return int(start_year[:4])
        return int(start_year)
    
    def active_status(row, tech_choices: dict, current_year: int):
        plant_name = row.plant_name
        start_year = row.start_year
        current_tech_choice = tech_choices[str(current_year)][plant_name]
        if current_tech_choice == 'Close plant':
            return False
        return start_year <= current_year

    df_container = []
    years = [int(key) for key in tech_choices.keys()]
    capacities_dict = dict(zip(plant_df['plant_name'], plant_df['plant_capacity']))
    start_year_dict = dict(zip(plant_df['plant_name'], plant_df['start_of_operation']))
    country_code_dict = dict(zip(plant_df['plant_name'], plant_df['country_code']))
    for year in years:
        plants = list(tech_choices[str(year)].keys())
        capacities = [capacities_dict[plant] for plant in plants]
        df = pd.DataFrame({'year': year, 'plant_name': plants, 'capacity': capacities, 'technology': '', 'region': ''})
        df['start_year'] = df['plant_name'].apply(lambda plant_name: fix_start_year(start_year_dict[plant_name]))
        df['technology'] = df['plant_name'].apply(lambda plant_name: tech_choices[str(year)][plant_name])
        df['green_tech'] = df['technology'].apply(lambda technology: tech_status_mapper(technology, inc_trans))
        df['active_status'] = df.apply(lambda row: active_status(row, tech_choices, year), axis=1)
        df['region'] = df["plant_name"].apply(lambda plant_name: get_region_from_country_code(country_code_dict[plant_name], "rmi_region", country_reference_dict))
        df_container.append(df)
    df_final = pd.concat(df_container).reset_index(drop=True)
    df_final['capacity'] /= 1000
    return df_final
    
def create_gcr_df(green_capacity_ratio_df: pd.DataFrame, rounding: int = 3):
    gcr = green_capacity_ratio_df[['year', 'capacity', 'green_tech', 'active_status']].set_index(['active_status','green_tech']).copy()
    gcr_green = gcr.loc[True, True].reset_index().groupby(['year']).sum()[['capacity']].copy()
    gcr_green.rename({'capacity': 'green_capacity'}, axis=1, inplace=True)
    gcr_nongreen = gcr.loc[True, False].reset_index().groupby(['year']).sum()[['capacity']].copy()
    gcr_nongreen.rename({'capacity': 'nongreen_capacity'}, axis=1, inplace=True)
    gcr_combined = gcr_green.join(gcr_nongreen)
    gcr_combined['nongreen_capacity'] = gcr_combined['nongreen_capacity'].fillna(0)
    gcr_combined['green_capacity_ratio'] = gcr_combined['green_capacity'] / gcr_combined['nongreen_capacity']
    return gcr_combined.round(rounding)

@timer_func
def generate_gcr_df(scenario_dict: dict, serialize: bool = False) -> pd.DataFrame:
    logger.info("- Starting Green Capacity Ratio")

    plant_result_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, "plant_result_df", "df")
    tech_choice_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "tech_choice_dict", "dict")
    country_reference_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "country_reference_dict", "dict")
    green_capacity_ratio_df = green_capacity_ratio_predata(plant_result_df, tech_choice_dict, country_reference_dict, True)
    green_capacity_ratio_result = create_gcr_df(green_capacity_ratio_df)
    green_capacity_ratio_result = add_results_metadata(
        green_capacity_ratio_result, scenario_dict, include_regions=False, single_line=True
    )
    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(green_capacity_ratio_result, PKL_DATA_FINAL, "green_capacity_ratio")
    return green_capacity_ratio_result
