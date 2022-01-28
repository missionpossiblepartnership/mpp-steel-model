"""Script to run a full reference dataframe for tco switches and abatement switches"""
import itertools
import pandas as pd
import numpy as np
import numpy_financial as npf

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.data_loading.data_interface import load_business_cases
from mppsteel.model.tco_calculation_functions import tco_calc, calculate_green_premium
from mppsteel.utility.utils import timer_func, read_pickle_folder, get_logger, serialize_file, add_results_metadata, move_cols_to_front, enumerate_columns
from mppsteel.utility.reference_lists import SWITCH_DICT
from mppsteel.model_config import DISCOUNT_RATE, MODEL_YEAR_END, MODEL_YEAR_START, PKL_DATA_INTERMEDIATE, PKL_DATA_IMPORTS, INVESTMENT_CYCLE_LENGTH


logger = get_logger("TCO & Abatement switches")

def tco_regions_ref_generator(electricity_cost_scenario, grid_scenario, hydrogen_cost_scenario, biomass_cost_scenario, eur_usd_rate, technology: str = None):
    carbon_tax_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'carbon_tax_timeseries', 'df')
    all_plant_variable_costs_summary = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'all_plant_variable_costs_summary', 'df')
    power_model = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'power_model_formatted', 'df')
    hydrogen_model = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'hydrogen_model_formatted', 'df')
    opex_values_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'capex_dict', 'df')
    business_cases = load_business_cases()
    calculated_s1_emissivity = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'calculated_s1_emissivity', 'df')
    capex_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_switching_df", "df")
    country_ref_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "country_reference_dict", "df")
    steel_plants = read_pickle_folder(PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df")
    steel_plant_country_codes = list(steel_plants['country_code'].unique())
    technologies = SWITCH_DICT.keys()
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END+1)
    for year in tqdm(year_range, total=len(year_range), desc='All Regions TCO: Year Loop'):
        for country_code in tqdm(steel_plant_country_codes, total=len(steel_plant_country_codes), desc='All Regions TCO: Region Loop'):
            for tech in technologies:
                tco_df = tco_calc(
                    country_code, year, tech, carbon_tax_df,
                    business_cases, all_plant_variable_costs_summary,
                    power_model, hydrogen_model,
                    opex_values_dict['other_opex'], calculated_s1_emissivity,
                    country_ref_dict, capex_df, INVESTMENT_CYCLE_LENGTH,
                    electricity_cost_scenario, grid_scenario,
                    hydrogen_cost_scenario)
                df_list.append(tco_df)
    combined_df = pd.concat(df_list).reset_index(drop=True)
    return combined_df

def create_full_steel_plant_ref(eur_usd_rate: float):
    logger.info('Adding Green Premium Values and year and technology index to steel plant data')
    green_premium_timeseries = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'green_premium_timeseries', 'df')
    all_plant_variable_costs_summary = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'all_plant_variable_costs_summary', 'df')
    steel_plants = read_pickle_folder(PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df")
    steel_plant_ref = steel_plants[['plant_name', 'country_code', 'technology_in_2020']].copy()
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END+1)
    df_list = []
    for year in tqdm(year_range, total=len(year_range), desc='Steel Plant TCO Switches: Year Loop'):
        sp_c = steel_plant_ref.copy()
        sp_c['year'] = year
        df_list.append(sp_c)
    steel_plant_ref = pd.concat(df_list).reset_index(drop=True)
    steel_plant_ref['discounted_green_premium'] = ''
    def value_mapper(row, enum_dict):
        start_year = row[enum_dict['year']]
        gp_arr = np.array([])
        year_range = range(start_year, start_year+INVESTMENT_CYCLE_LENGTH+1)
        for year in year_range:
            year_loop_val = year
            if year > 2050:
                year_loop_val = 2050
            green_premium_value = calculate_green_premium(
                all_plant_variable_costs_summary, steel_plants,
                green_premium_timeseries, row[enum_dict['country_code']],
                row[enum_dict['plant_name']], # remove ref to region
                row[enum_dict['technology_in_2020']],
                year_loop_val, eur_usd_rate)
            gp_arr = np.append(gp_arr, green_premium_value)
        discounted_gp_arr = npf.npv(DISCOUNT_RATE, gp_arr)
        row[enum_dict['discounted_green_premium']] = discounted_gp_arr
        return row
    logger.info('Calculating green premium values')
    tqdma.pandas(desc="Apply Green Premium Values")
    enumerated_cols = enumerate_columns(steel_plant_ref.columns)
    steel_plant_ref = steel_plant_ref.progress_apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    pair_list = []
    for tech in SWITCH_DICT.keys():
        pair_list.append(list(itertools.product([tech], SWITCH_DICT[tech])))
    all_tech_switch_combinations = [item for sublist in pair_list for item in sublist]
    df_list = []
    logger.info('Generating Year range reference')
    for combination in tqdm(all_tech_switch_combinations, total=len(all_tech_switch_combinations), desc='Steel Plant TCO Switches: Technology Loop'):
        sp_c = steel_plant_ref.copy()
        sp_c['base_tech'] = combination[0]
        sp_c['switch_tech'] = combination[1]
        df_list.append(sp_c)
    steel_plant_full_ref = pd.concat(df_list).reset_index(drop=True)
    steel_plant_full_ref.set_index(['year', 'country_code', 'base_tech', 'switch_tech'], inplace=True)
    return steel_plant_full_ref


def map_region_tco_to_plants(steel_plant_ref: pd.DataFrame, opex_capex_ref: pd.DataFrame):
    logger.info('Mapping Regional emissions dict to plants')
    # Format TCO values
    opex_capex_ref_c = opex_capex_ref.reset_index(drop=True).copy()
    opex_capex_ref_c.rename({'start_technology': 'base_tech', 'end_technology': 'switch_tech'}, axis='columns', inplace=True)
    opex_capex_ref_c.set_index(['year', 'country_code', 'base_tech', 'switch_tech'], inplace=True)
    logger.info('Joining tco values on steel plant df')
    combined_df = steel_plant_ref.join(opex_capex_ref_c, how='left').reset_index()
    combined_df['tco'] = combined_df.apply(lambda x: (x['capex_value'] + x['discounted_opex'] - x['discounted_green_premium']) / INVESTMENT_CYCLE_LENGTH, axis=1)
    new_col_order = ['year', 'plant_id', 'plant_name', 'country_code', 'base_tech', 'switch_tech', 'capex_value', 'discounted_opex', 'discounted_green_premium', 'tco']
    return combined_df[new_col_order]


@timer_func
def tco_presolver_reference(scenario_dict, serialize_only: bool = False):
    electricity_cost_scenario=scenario_dict['electricity_cost_scenario']
    grid_scenario=scenario_dict['grid_scenario']
    hydrogen_cost_scenario=scenario_dict['hydrogen_cost_scenario']
    biomass_cost_scenario=scenario_dict['biomass_cost_scenario']
    eur_usd_rate=scenario_dict['eur_usd']
    opex_capex_reference_data = tco_regions_ref_generator(electricity_cost_scenario, grid_scenario, hydrogen_cost_scenario, biomass_cost_scenario, eur_usd_rate)
    steel_plant_ref = create_full_steel_plant_ref(eur_usd_rate)
    tco_reference_data = map_region_tco_to_plants(steel_plant_ref, opex_capex_reference_data)
    tco_reference_data = add_results_metadata(tco_reference_data, scenario_dict, single_line=True)
    if serialize_only:
        logger.info(f'-- Serializing dataframe')
        serialize_file(tco_reference_data, PKL_DATA_INTERMEDIATE, "tco_reference_data")
    return tco_reference_data

def emissivity_abatement(combined_emissivity: pd.DataFrame, scope: str):
    logger.info('Getting all Emissivity Abatement combinations for all technology switches')
    emissivity_mapper = {
        's1': 's1_emissivity',
        's2': 's2_emissivity',
        's3': 's3_emissivity',
        'combined': 'combined_emissivity',
    }
    combined_emissivity = combined_emissivity.reset_index().set_index(['year', 'country_code', 'technology']).copy()
    country_codes = combined_emissivity.index.get_level_values(1).unique()
    technologies = combined_emissivity.index.get_level_values(2).unique()
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END+1)
    for year in tqdm(year_range, total=len(year_range), desc='Emission Abatement: Year Loop'):
        for country_code in country_codes:
            for base_tech in technologies:
                for switch_tech in SWITCH_DICT[base_tech]:
                    base_tech_value = combined_emissivity.loc[year, country_code, base_tech][emissivity_mapper[scope]]
                    switch_tech_value = combined_emissivity.loc[year, country_code, switch_tech][emissivity_mapper[scope]]
                    value_difference =  base_tech_value - switch_tech_value
                    entry = {'year': year, 'country_code': country_code, 'base_tech': base_tech, 'switch_tech': switch_tech, f'abated_{scope}_emissivity': value_difference}
                    df_list.append(entry)
    combined_df = pd.DataFrame(df_list)
    return combined_df

@timer_func
def abatement_presolver_reference(scenario_dict, serialize_only: bool = False):
    logger.info('Running Abatement Reference Sheet')
    calculated_emissivity_combined = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'calculated_emissivity_combined', 'df')
    emissivity_abatement_switches = emissivity_abatement(calculated_emissivity_combined, scope='combined')
    emissivity_abatement_switches = add_results_metadata(emissivity_abatement_switches, scenario_dict, single_line=True)
    if serialize_only:
        logger.info(f'-- Serializing dataframe')
        serialize_file(emissivity_abatement_switches, PKL_DATA_INTERMEDIATE, "emissivity_abatement_switches")
    return emissivity_abatement_switches
