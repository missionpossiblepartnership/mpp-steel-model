"""Script that standardises business cases into per steel units and into summary tables."""

# For copying float objects
from copy import deepcopy
from typing import Union

# For Data Manipulation
import pandas as pd
import pandera as pa

from tqdm.auto import tqdm as tqdma

# For logger
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file
)

from mppsteel.utility.log_utility import get_logger
from mppsteel.validation.data_import_tests import STEEL_BUSINESS_CASES_SCHEMA

from mppsteel.model_config import (
    PKL_DATA_IMPORTS,
    PKL_DATA_INTERMEDIATE
)

from mppsteel.utility.reference_lists import (
    TECH_REFERENCE_LIST,
    FURNACE_GROUP_DICT,
    TECHNOLOGY_PROCESSES,
    PER_T_STEEL_DICT_UNITS,
    bosc_factor_group,
    eaf_factor_group,
    electricity_and_steam_self_gen_group,
    electricity_self_gen_group,
    HARD_CODED_FACTORS,
)

# Create logger
logger = get_logger("Business Case Standarisation")


# For Data Manipulation
import pprint
from copy import deepcopy

from tqdm import tqdm

def create_tech_processes_list() -> dict:
    basic_bof_processes = ['Coke Production', 'Sintering', 'Pelletisation', 'Blast Furnace', 'Oxygen Generation', 'Basic Oxygen Steelmaking + Casting', 'Limestone', 'Self-Generation Of Electricity']
    basic_bof_processes_ccs = basic_bof_processes.copy()
    basic_bof_processes_ccs.append('CCS')
    basic_bof_processes_ccu = basic_bof_processes.copy()
    basic_bof_processes_ccu.extend(['CCU -CO-based', 'CCU -CO2-based'])
    dri_eaf_basic_processes = ['Coke Production', 'Sintering', 'Pelletisation', 'Shaft Furnace', 'EAF (Steel-making) + Casting']
    dri_eaf_basic_processes_ccs = dri_eaf_basic_processes.copy()
    dri_eaf_basic_processes_ccs.append('CCS')
    eaf_basic_processes = ['Coke Production', 'Sintering', 'Pelletisation', 'Blast Furnace', 'EAF (Steel-making) + Casting']
    eaf_electro_processes = ['Coke Production', 'Sintering', 'Pelletisation', 'Electrolyzer', 'EAF (Steel-making) + Casting']
    dri_melt_bof_processes = ['Coke Production', 'Sintering', 'Pelletisation', 'Shaft Furnace', 'Remelt', 'Oxygen Generation', 'Limestone', 'Basic Oxygen Steelmaking + Casting']
    dri_melt_bof_processes_ccs = dri_melt_bof_processes.copy()
    dri_melt_bof_processes_ccs.append('CCS')
    dri_melt_bof_ch2 = ['Coke Production', 'Sintering', 'Pelletisation', 'Shaft Furnace', 'Remelt', 'Oxygen Generation', 'Limestone', 'Basic Oxygen Steelmaking + Casting']
    basic_smelting_processes = ['Coke Production', 'Sintering', 'Pelletisation', 'Smelting Furnace', 'Oxygen Generation', 'Limestone', 'Basic Oxygen Steelmaking + Casting', 'Self-Generation Of Electricity']
    basic_smelting_processes_ccs = basic_smelting_processes.copy()
    basic_smelting_processes_ccs.append('CCS')
    
    return {
        'Avg BF-BOF': basic_bof_processes,
        'BAT BF-BOF': basic_bof_processes,
        'BAT BF-BOF_bio PCI': basic_bof_processes,
        'BAT BF-BOF_H2 PCI': basic_bof_processes,
        'BAT BF-BOF+CCUS': basic_bof_processes_ccs,
        'DRI-EAF': dri_eaf_basic_processes,
        'DRI-EAF_50% green H2': dri_eaf_basic_processes,
        'DRI-EAF_50% bio-CH4': dri_eaf_basic_processes,
        'DRI-EAF+CCUS': dri_eaf_basic_processes_ccs,
        'DRI-EAF_100% green H2': dri_eaf_basic_processes,
        'Smelting Reduction': basic_smelting_processes,
        'Smelting Reduction+CCUS': basic_smelting_processes_ccs,
        'EAF': eaf_basic_processes,
        'Electrolyzer-EAF': eaf_electro_processes,
        'BAT BF-BOF+CCU': basic_bof_processes_ccu,
        'DRI-Melt-BOF': dri_melt_bof_processes,
        'DRI-Melt-BOF+CCUS': dri_melt_bof_processes_ccs,
        'DRI-Melt-BOF_100% zero-C H2': dri_melt_bof_ch2,
        'Electrowinning-EAF': eaf_electro_processes,
        'BAT BF-BOF+BECCUS': basic_bof_processes_ccs,
        'Non-Furnace': [],
        'Charcoal mini furnace': []
    }


def create_hardcoded_exceptions(hard_coded_dict: dict, furnace_group_dict: dict) -> list:
    hard_coded_factor_list = []
    for furnace_group in hard_coded_dict:
        hard_coded_factor_list.extend(furnace_group_dict[furnace_group])
    return hard_coded_factor_list


def furnace_group_from_tech(furnace_group_dict: dict) -> dict:
    tech_container = {}
    for group in furnace_group_dict.keys():
        tech_list = furnace_group_dict[group]
        for tech in tech_list:
            tech_container[tech] = group
    return tech_container

@pa.check_input(STEEL_BUSINESS_CASES_SCHEMA)
def business_case_formatter_splitter(df: pd.DataFrame) -> Union[pd.DataFrame, pd.DataFrame]:
    df_c = df.copy()
    df_c = df_c.melt(id_vars=['Section', 'Process', 'Process Detail', 'Step', 'Material Category', 'Unit'], var_name='Technology')
    df_c.columns = [col.lower().replace(' ', '_') for col in df_c.columns]
    df_c['value'].fillna(0,inplace=True)
    df_c_parameters = df_c.loc[df_c['section'] == 'Parameters']
    df_c_process = df_c.loc[df_c['section'] != 'Parameters']
    df_c_parameters.drop(labels=['section'],axis=1,inplace=True)
    df_c_process.drop(labels=['section'],axis=1,inplace=True)
    return df_c_parameters, df_c_process


def tech_process_getter(
    df, technology: str, process: str, 
    step: str = None, material: str = None, 
    process_detail: str = None, full_ref: bool = False) -> pd.DataFrame:
    df_c = df.copy()
    full_ref_cols = ['technology', 'step', 'material_category', 'value']
    if full_ref:
        choice_cols = full_ref_cols
    # ALL
    if step and material and process_detail:
        return df_c[
            (df_c['technology'] == technology) & (df_c['process'] == process) & (df_c['step'] == step) & (df_c['material_category'] == material) & (df_c['process_detail'] == process_detail)
        ]['value'].values[0]
    # 2 ONLY
    if material and step and not process_detail:
        return df_c[
            (df_c['technology'] == technology) & (df_c['process'] == process) & (df_c['material_category'] == material) & (df_c['step'] == step)
        ]['value']['value'].values[0]
    if material and process_detail and not step:
        return df_c[
            (df_c['technology'] == technology) & (df_c['process'] == process) & (df_c['material_category'] == material) & (df_c['process_detail'] == process_detail)
        ]['value'].values[0]
    if step and process_detail and not material:
        return df_c[
            (df_c['technology'] == technology) & (df_c['process'] == process) & (df_c['process_detail'] == process_detail) & (df_c['step'] == step)
        ]['value'].values[0]
    # 1 ONLY
    if material and not step and not process_detail:
        return df_c[
            (df_c['technology'] == technology) & (df_c['process'] == process) & (df_c['material_category'] == material)
        ]['value'].values[0]
    if step and not material and not process_detail:
        return df_c[
            (df_c['technology'] == technology) & (df_c['process'] == process) & (df_c['step'] == step)
        ]['value'].values[0]
    if process_detail and not material and not step:
        return df_c[
            (df_c['technology'] == technology) & (df_c['process'] == process) & (df_c['process_detail'] == process_detail)
        ]['value'].values[0]
    # NONE
    if not full_ref:
        return df_c[(df_c['technology'] == technology) & (df_c['process'] == process)]
    full_ref_df = df_c[(df_c['technology'] == technology) & (df_c['process'] == process)]
    return full_ref_df

def tech_parameter_getter(df, technology, parameter) -> float:
    df_c = df.copy()
    return df_c[(df_c['technology'] == technology) & (df_c['step'] == parameter)]['value'].values[0]

def replace_units(df: pd.DataFrame, units_dict: dict) -> pd.DataFrame:
    df_c = df.copy()
    def value_mapper(row, enum_dict):
        if row[enum_dict['material_category']] in units_dict.keys():
            row[enum_dict['unit']] = units_dict[row[enum_dict['material_category']]]
        else:
            row[enum_dict['unit']] = ''
        return row
    tqdma.pandas(desc="Replace Units")
    enumerated_cols = enumerate_iterable(df_c.columns)
    df_c = df_c.progress_apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    return df_c

def create_mini_process_dfs(
    df: pd.DataFrame, technology_name: str, 
    process_mapper: dict, factor_value_dict: dict) -> dict:
    df_c = df.copy()
    df_dict = {}
    for process in process_mapper[technology_name]:
        df_f = tech_process_getter(df_c, technology_name, process=process)
        df_f['value'] = df_f['value'] * factor_value_dict[process]
        df_dict[process] = df_f
    return df_dict

def format_combined_df(df: pd.DataFrame, units_dict: dict) -> pd.DataFrame:
    df_c = df.copy()
    df_c = replace_units(df_c, units_dict)
    return df_c
    
def sum_product_ef(df: pd.DataFrame, ef_dict: dict, materials_to_exclude: list = None) -> float:
    df_c = df.copy()
    df_c['material_emissions'] = ''
    def value_mapper(row, enum_dict):
        if materials_to_exclude:
            if (row[enum_dict['material_category']] in ef_dict.keys()) & (row[enum_dict['material_category']] not in materials_to_exclude):
                row[enum_dict['material_emissions']] = row[enum_dict['value']] * ef_dict[row[enum_dict['material_category']]]
            else:
                row[enum_dict['material_emissions']] = 0
        else:
            if row[enum_dict['material_category']] in ef_dict.keys():
                row[enum_dict['material_emissions']] = row[enum_dict['value']] * ef_dict[row[enum_dict['material_category']]]
            else:
                row[enum_dict['material_emissions']] = 0
        return row
    tqdma.pandas(desc="Sum Product Emissions Factors")
    enumerated_cols = enumerate_iterable(df_c.columns)
    df_c = df_c.progress_apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    return df_c['material_emissions'].sum()

def get_all_steam_values(df: pd.DataFrame, technology: str, process_list: list, factor_dict: dict) -> list:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    steam_value_list = []
    df_c = df.loc[df['technology'] == technology].copy()
    for process in process_list:
        if 'Steam' in df_c[df_c['process'] == process]['material_category'].unique(): # changed
            factor = factor_dict[process] # changed
            steam_value = tech_process_getter(bc_processes, technology, process=process, material='Steam')
            steam_value_list.append(steam_value * factor)
    return steam_value_list


def get_all_electricity_values(df: pd.DataFrame, technology: str, process_list: list, factor_mapper: dict = [], as_dict: bool = False) -> Union[dict, list]: 
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    electricity_value_list = []
    electricity_value_dict = {}
    df_c = df.loc[df['technology'] == technology].copy()
    for process in process_list:
        if 'Electricity' in df_c[df_c['process'] == process]['material_category'].unique():
            factor = factor_mapper[process]
            if process == 'Oxygen Generation':
                factor = factor_mapper['Basic Oxygen Steelmaking + Casting']
            
            if process == 'Basic Oxygen Steelmaking + Casting':
                electricity_value_oxygen_furnace = tech_process_getter(df, technology, process=process, process_detail='Energy-oxygen furnace', material='Electricity')
                val_to_append = electricity_value_oxygen_furnace * factor
                electricity_value_list.append(val_to_append)
                if as_dict:
                    electricity_value_dict[f'{process} - Oxygen'] = val_to_append
                electricity_value_casting = tech_process_getter(df, technology, process=process, process_detail='Energy-casting', material='Electricity')
                val_to_append = electricity_value_casting * factor
                electricity_value_list.append(val_to_append)
                if as_dict:
                    electricity_value_dict[f'{process} - Casting'] = val_to_append
            else:
                electricity_value_general = tech_process_getter(bc_processes, technology, process=process, material='Electricity')
                val_to_append = electricity_value_general * factor
                electricity_value_list.append(val_to_append)
                if as_dict:
                    electricity_value_dict[process] = val_to_append
    if as_dict:
        return electricity_value_dict
    return electricity_value_list



def create_production_factors(technology: str, furnace_group_dict: dict, hard_coded_factors: dict) -> dict:
    
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    processes = bc_processes['process'].copy().unique()

    # Instantiate factors
    COKE_PRODUCTION_FACTOR = None
    SINTERING_FACTOR = None
    PELLETISATION_FACTOR = None
    BLAST_FURNACE_FACTOR = None
    OXYGEN_GENERATION_FACTOR = None
    BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR = None
    SHAFT_FURNACE_FACTOR = None
    EAF_STEELMAKING_CASTING_FACTOR = None
    LIMESTONE_FACTOR = None
    ELECTRICITY_GENERATION_FACTOR = None
    SMELTING_FURNACE_FACTOR = None
    ELECTROLYZER_FACTOR = None
    CCS_FACTOR = None
    CCU_CO_FACTOR = None
    CCU_CO2_FACTOR = None
    REMELT_FACTOR = None

    # SET BASE FACTORS
    if technology in bosc_factor_group:
        BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR = 1.02
    if technology in eaf_factor_group:
        EAF_STEELMAKING_CASTING_FACTOR = 1.02

    # Factor Calculation: BOSC
    if technology in bosc_factor_group:
        hot_metal_required = tech_process_getter(bc_processes, technology, 'Basic Oxygen Steelmaking + Casting', step='Hot metal required')
        bosc_hot_metal_required = hot_metal_required * BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR
        bof_lime = tech_process_getter(bc_processes, technology, process='Limestone', step='BOF lime') * BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR
        oxygen_electricity = tech_process_getter(bc_processes, technology, process='Oxygen Generation', material='Electricity')
        

        # Smelting
        if technology in furnace_group_dict['smelting_reduction']:
            SMELTING_FURNACE_FACTOR = deepcopy(bosc_hot_metal_required)
            smelting_furnace_sinter = tech_process_getter(bc_processes, technology, 'Smelting Furnace', step='Sinter') * SMELTING_FURNACE_FACTOR
            SINTERING_FACTOR = deepcopy(smelting_furnace_sinter)
            smelting_furnace_pellets = tech_process_getter(bc_processes, technology, 'Smelting Furnace', step='Pellets') * SMELTING_FURNACE_FACTOR
            PELLETISATION_FACTOR = deepcopy(smelting_furnace_pellets)
            OXYGEN_GENERATION_FACTOR = BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR * oxygen_electricity

        # Blast Furnace
        if technology in furnace_group_dict['blast_furnace']:
            BLAST_FURNACE_FACTOR = deepcopy(bosc_hot_metal_required)
            blast_furnace_sinter = tech_process_getter(bc_processes, technology, 'Blast Furnace', step='Sinter') * BLAST_FURNACE_FACTOR
            SINTERING_FACTOR = deepcopy(blast_furnace_sinter)
            blast_furnace_pellets = tech_process_getter(bc_processes, technology, 'Blast Furnace', step='Pellets') * BLAST_FURNACE_FACTOR
            PELLETISATION_FACTOR = deepcopy(blast_furnace_pellets)
            coke_lcv = tech_parameter_getter(bc_parameters, technology, 'Coke LCV')
            blast_furnace_coke = tech_process_getter(bc_processes, technology, 'Blast Furnace', step='Coke') * coke_lcv * BLAST_FURNACE_FACTOR 
            COKE_PRODUCTION_FACTOR = blast_furnace_coke / coke_lcv
            OXYGEN_GENERATION_FACTOR = BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR * oxygen_electricity

        # DRI-BOF
        if technology in furnace_group_dict['dri-bof']:
            REMELT_FACTOR = deepcopy(bosc_hot_metal_required)
            dri_captive_remelt = tech_process_getter(bc_processes, technology, 'Remelt', step='DRI - captive') * REMELT_FACTOR
            SHAFT_FURNACE_FACTOR = deepcopy(dri_captive_remelt)
            shaft_furnace_pellets = tech_process_getter(bc_processes, technology, 'Shaft Furnace', material='Iron ore') * SHAFT_FURNACE_FACTOR # Iron ore, but should be pellets
            PELLETISATION_FACTOR = deepcopy(shaft_furnace_pellets)
            OXYGEN_GENERATION_FACTOR = REMELT_FACTOR * oxygen_electricity

    # Factor Calculation: EAF
    # EAF Basic
    if technology in furnace_group_dict['eaf-basic']:
        COKE_PRODUCTION_FACTOR = 1
        SINTERING_FACTOR = 1
        PELLETISATION_FACTOR = 1
        BLAST_FURNACE_FACTOR = 1

    # EAF Advanced
    if technology in furnace_group_dict['eaf-advanced']:
        COKE_PRODUCTION_FACTOR = 0
        SINTERING_FACTOR = 0
        ELECTROLYZER_FACTOR = tech_process_getter(bc_processes, technology, 'EAF (Steel-making) + Casting', step='Iron in steel')
        electrolyzer_pellets = 0 * ELECTROLYZER_FACTOR  # * Mystery cell?? - No Pellets in Business Cases One Table
        PELLETISATION_FACTOR = deepcopy(electrolyzer_pellets)
        electrolyzer_coke = 0 * ELECTROLYZER_FACTOR # * Mystery cell?? - No Coke in Business Cases One Table
        electrolyzer_thermal_coal = 0 * ELECTROLYZER_FACTOR / 1000 # * Mystery cell?? - No Thermal Coal in Business Cases One Table

    # DRI-EAF
    if technology in furnace_group_dict['dri-eaf']:
        COKE_PRODUCTION_FACTOR = 0
        SINTERING_FACTOR = 0
        dri_captive_eaf_casting = tech_process_getter(bc_processes, technology, 'EAF (Steel-making) + Casting', step='DRI - captive') * EAF_STEELMAKING_CASTING_FACTOR
        SHAFT_FURNACE_FACTOR = deepcopy(dri_captive_eaf_casting)
        shaft_furnace_pellets = tech_process_getter(bc_processes, technology, 'Shaft Furnace', material='Iron ore') * SHAFT_FURNACE_FACTOR # Iron ore, but should be pellets
        PELLETISATION_FACTOR = deepcopy(shaft_furnace_pellets)
        shaft_furnace_coke = tech_process_getter(bc_processes, technology, 'Shaft Furnace', material='Coke') * SHAFT_FURNACE_FACTOR # * Mystery cell??
        shaft_furnace_coal = tech_process_getter(bc_processes, technology, 'Shaft Furnace', material='Coal') * SHAFT_FURNACE_FACTOR * tech_parameter_getter(bc_parameters, technology, 'DRI metallic Fe concentration') / 1000

    # Create process factor 
    factor_list = [
        COKE_PRODUCTION_FACTOR, SINTERING_FACTOR, PELLETISATION_FACTOR, BLAST_FURNACE_FACTOR,
        OXYGEN_GENERATION_FACTOR, BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR, SHAFT_FURNACE_FACTOR,
        EAF_STEELMAKING_CASTING_FACTOR, LIMESTONE_FACTOR, ELECTRICITY_GENERATION_FACTOR, CCS_FACTOR,
        SMELTING_FURNACE_FACTOR, ELECTROLYZER_FACTOR, CCU_CO_FACTOR, CCU_CO2_FACTOR, REMELT_FACTOR
    ]

    # Overwrite dictionary values
    process_factor_mapper = dict(zip(processes, factor_list))
    furnace_group = furnace_group_from_tech(furnace_group_dict)[technology]
    # Overwrite processes
    if technology in create_hardcoded_exceptions(hard_coded_factors, furnace_group_dict):
        if furnace_group in hard_coded_factors.keys():
            for process in hard_coded_factors[furnace_group]:
                process_factor_mapper[process] = hard_coded_factors[furnace_group][process]
    # Replace None values with 0
    for process in process_factor_mapper.keys():
        process_factor_mapper[process] = list({process_factor_mapper[process] or 0})[0]
        
    return process_factor_mapper



def limestone_df_editor(df_dict: dict, technology: str, furnace_group_dict: dict, factor_dict: dict) -> dict:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    df_dict_c = df_dict.copy()
    
    if technology in electricity_self_gen_group:
        
        if technology in furnace_group_dict['smelting_reduction']:
            limestone_df = df_dict_c['Limestone'].copy()
            bof_lime = tech_process_getter(bc_processes, technology, 'Limestone', step='BOF lime') * factor_dict['Basic Oxygen Steelmaking + Casting']
            limestone_df.loc[limestone_df['step'] == 'BOF lime', 'value'] = bof_lime

            new_row = {
                'process':'Limestone', 
                'process_detail':'', 
                'step': 'Process emissions', 
                'material_category': 'Process emissions', 
                'unit': 't CO2 / t LS', 
                'technology': technology, 
                'value': bof_lime * 0.75 / 1000
            }
            limestone_df.append(new_row, ignore_index=True)
            df_dict_c['Limestone'] = limestone_df

        if technology in furnace_group_dict['blast_furnace']:
            limestone_df = df_dict_c['Limestone'].copy()
            bof_lime = tech_process_getter(bc_processes, technology, 'Limestone', step='BOF lime') * factor_dict['Basic Oxygen Steelmaking + Casting']
            limestone_df.loc[limestone_df['step'] == 'BOF lime', 'value'] = bof_lime
            blast_furnace_lime = tech_process_getter(bc_processes, technology, 'Limestone', step='Blast furnace lime') * factor_dict['Blast Furnace']
            limestone_df.loc[limestone_df['step'] == 'Blast furnace lime', 'value'] = blast_furnace_lime

            new_row = {
                'process':'Limestone', 
                'process_detail':'', 
                'step': 'Process emissions', 
                'material_category': 'Process emissions', 
                'unit': 't CO2 / t LS', 
                'technology': technology, 
                'value': (bof_lime + blast_furnace_lime) * 0.75 / 1000
            }
            limestone_df = limestone_df.append(new_row, ignore_index=True)
            df_dict_c['Limestone'] = limestone_df
    
    return df_dict_c

def fix_ccs_factors(r_dict, factor_dict: dict, technology: str, furnace_group_dict: dict, ef_dict: dict) -> dict:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    factor_dict_c = factor_dict.copy()

    # CCS FACTOR
    if technology in ['Smelting Reduction+CCUS']:
        smelting_furnace_factor = factor_dict['Smelting Furnace']
        electricity_share_factor = tech_parameter_getter(bc_parameters, technology, 'Share of electricity purchased in total demand')
        natural_gas_ccs = 0
        smelting_furnace_thermal_coal = tech_process_getter(bc_processes, technology, process='Smelting Furnace', material='Thermal coal') * smelting_furnace_factor
        thermal_coal_ef = ef_dict['Thermal coal']
        natural_gas_smelting_furnace = tech_process_getter(bc_processes, technology, process='Smelting Furnace', material='Natural gas') * smelting_furnace_factor
        natural_gas_ef = ef_dict['Natural gas']
        bof_gas_ef = ef_dict['BOF gas']
        factor_dict_c['CCS'] = (smelting_furnace_thermal_coal * thermal_coal_ef / 1000) + ((natural_gas_ccs + natural_gas_smelting_furnace) * natural_gas_ef / 1000) + ((1-electricity_share_factor) * 10 * bof_gas_ef / 1000)

    if technology in ['DRI-EAF+CCUS']:
        pellets_natural_gas = tech_process_getter(r_dict['Pelletisation'], technology, process='Pelletisation', material='Natural gas')
        shaft_furnace_natural_gas = tech_process_getter(r_dict['Shaft Furnace'], technology, process='Shaft Furnace', material='Natural gas')
        eaf_natural_gas = tech_process_getter(r_dict['EAF (Steel-making) + Casting'], technology, process='EAF (Steel-making) + Casting', process_detail='Energy-casting', material='Natural gas')
        eaf_process_emissions = tech_process_getter(r_dict['EAF (Steel-making) + Casting'], technology, process='EAF (Steel-making) + Casting', material='Process emissions')
        factor_dict_c['CCS'] = ((shaft_furnace_natural_gas + pellets_natural_gas + eaf_natural_gas) * (ef_dict['Natural gas'] / 1000)) + eaf_process_emissions

    if technology in ['DRI-Melt-BOF+CCUS']:
        ef_sum_container = []
        for process in ['Shaft Furnace', 'Remelt']:
            process_df = r_dict[process].copy()
            sum_val = sum_product_ef(process_df, ef_dict)
            ef_sum_container.append(sum_val)
        ef_sum_product = sum(ef_sum_container)
        factor_dict_c['CCS'] = ef_sum_product / 1000
        
    if technology in furnace_group_dict['ccu']:
        # co
        ETHANOL_PRODUCTION_FROM_CO = 180
        co_utilization_rate = tech_parameter_getter(bc_parameters, technology, 'CCS Capture rate')
        ccu_factor = ETHANOL_PRODUCTION_FROM_CO * co_utilization_rate / 1000
        factor_dict_c['CCU -CO-based'] = ccu_factor

        # co2
        ef_sum_container = []
        for process in ['Coke Production', 'Sintering', 'Pelletisation', 'Blast Furnace']:
            selected_process_df = r_dict[process]
            sum_val = sum_product_ef(selected_process_df, ef_dict)
            ef_sum_container.append(sum_val)
        ef_sum_product_large = sum(ef_sum_container)

        limestone_process_emissions = tech_process_getter(r_dict['Limestone'], technology, process='Limestone', step='Process emissions')
        used_co2 = tech_process_getter(bc_processes, technology, process='CCU -CO-based', material='Used CO2') * ccu_factor
        factor_dict_c['CCU -CO2-based'] = (ef_sum_product_large / 1000) + limestone_process_emissions - used_co2 
        
    return factor_dict_c


def fix_ccs_bat_ccus_factors(r_dict, factor_dict: dict, technology: str, furnace_group_dict: dict, ef_dict: dict) -> dict:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    factor_dict_c = factor_dict.copy()
    special_factor_dict = {'BAT BF-BOF+BECCUS': 0.5, 'BAT BF-BOF+CCUS': 0.52}

    if technology in ['BAT BF-BOF+CCUS', 'BAT BF-BOF+BECCUS']:
        electricity_share_factor = tech_parameter_getter(bc_parameters, technology, 'Share of electricity purchased in total demand')
        limestone_process_emissions = tech_process_getter(r_dict['Limestone'], technology, process='Limestone', step='Process emissions') # changed
        ef_dict_c = ef_dict.copy()
        ef_dict_c['Biomass'] = 95
        ef_sum_container = []
        for process in ['Coke Production', 'Sintering', 'Pelletisation', 'Blast Furnace']:
            selected_process_df = r_dict[process]
            sum_val = sum_product_ef(selected_process_df, ef_dict_c)
            ef_sum_container.append(sum_val)
        ef_sum_product_large = sum(ef_sum_container)
        ef_sum_product_small = sum_product_ef(r_dict['Blast Furnace'], ef_dict_c, ['Electricity'])
        s_factor = special_factor_dict[technology]
        ccs_factor = (ef_sum_product_large / 1000) + ((ef_sum_product_small * ef_dict_c['Electricity']) / 1000) + (s_factor * (1 - electricity_share_factor)) + limestone_process_emissions
        factor_dict_c['CCS'] = ccs_factor
        
    return factor_dict_c

def fix_ccu_factors(r_dict, factor_dict: dict, technology: str, furnace_group_dict: dict, ef_dict: dict) -> dict:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    factor_dict_c = factor_dict.copy()
    
    if technology in furnace_group_dict['ccu']:
        # co
        ETHANOL_PRODUCTION_FROM_CO = 180
        co_utilization_rate = tech_parameter_getter(bc_parameters, technology, 'CCS Capture rate')
        ccu_factor = ETHANOL_PRODUCTION_FROM_CO * co_utilization_rate / 1000
        factor_dict_c['CCU -CO-based'] = ccu_factor

        # co2
        ef_sum_container = []
        for process in ['Coke Production', 'Sintering', 'Pelletisation', 'Blast Furnace']:
            selected_process_df = r_dict[process]
            sum_val = sum_product_ef(selected_process_df, ef_dict)
            ef_sum_container.append(sum_val)
        ef_sum_product_large = sum(ef_sum_container)

        limestone_process_emissions = tech_process_getter(r_dict['Limestone'], technology, process='Limestone', step='Process emissions')
        used_co2 = tech_process_getter(bc_processes, technology, process='CCU -CO-based', material='Used CO2') * ccu_factor
        factor_dict_c['CCU -CO2-based'] = (ef_sum_product_large / 1000) + limestone_process_emissions - used_co2 
        
    return factor_dict_c


def ccs_df_editor(df_dict: dict, technology: str, furnace_group_dict: dict, factor_dict: dict, ef_dict: dict) -> dict:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    df_dict_c = df_dict.copy()
    
    if technology in furnace_group_dict['ccs']:
        
        ccs_df = df_dict_c['CCS'].copy()

        reboiler_duty_natural_gas = tech_process_getter(bc_processes, technology, process='CCS', material='Natural gas')

        # captured co2 value
        captured_co2 = tech_process_getter(bc_processes, technology, process='CCS', step='Captured CO2')
        captured_co2_factored = captured_co2 * factor_dict['CCS']
        ccs_df.loc[(ccs_df['step'] == 'Captured CO2'), 'value'] = captured_co2_factored
        compression_electricity = tech_process_getter(bc_processes, technology, process='CCS', process_detail='Compression')

        # Compression Electricity Value
        if technology in ['BAT BF-BOF+CCUS', 'DRI-EAF+CCUS', 'Smelting Reduction+CCUS']:
            ccs_df.loc[(ccs_df['process_detail'] == 'Compression'), 'value'] = captured_co2_factored * compression_electricity

        if technology in ['DRI-Melt-BOF+CCUS']:
            remelter_heating_efficiency = tech_parameter_getter(bc_parameters, technology, 'Efficiency of remelter heating') 
            ccs_df.loc[(ccs_df['process_detail'] == 'Compression'), 'value'] = captured_co2_factored * compression_electricity * remelter_heating_efficiency

        if technology in ['BAT BF-BOF+BECCUS']:
            electricity_share_factor = tech_parameter_getter(bc_parameters, technology, 'Share of electricity purchased in total demand')
            ccs_df.loc[(ccs_df['process_detail'] == 'Compression'), 'value'] = captured_co2_factored * compression_electricity * electricity_share_factor

        # Reboiler duty: Natural Gas / Electricity Value
        if technology in ['Smelting Reduction+CCUS']:
            ccs_df.loc[(ccs_df['material_category'] == 'Natural gas'), 'value'] = 0
            
        if technology in ['DRI-EAF+CCUS']:
            curr_ng_val = tech_process_getter(bc_processes, technology, process='CCS', process_detail='Reboiler duty', material='Natural gas')
            ccs_df.loc[ccs_df['process_detail'].eq('Reboiler duty') & ccs_df['material_category'].eq('Natural gas'), 'value'] = curr_ng_val * factor_dict['CCS']
            ccs_df.loc[ccs_df['process_detail'].eq('Reboiler duty') & ccs_df['material_category'].eq('Natural gas'), 'material_category'] = 'Electricity'
            
        if technology in ['BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCUS']:
            ef_dict_c = ef_dict.copy()
            ef_dict_c['Biomass'] = 95
            ef_sum_product_small = sum_product_ef(df_dict_c['Blast Furnace'], ef_dict_c, ['Electricity'])
            elec_reboiler = 3.6 * ef_sum_product_small / 1000
            ccs_df.loc[(ccs_df['material_category'] == 'Natural gas'), 'value'] = elec_reboiler
            ccs_df.loc[(ccs_df['material_category'] == 'Natural gas'), 'material_category'] = 'Electricity'
            
        if technology in ['DRI-Melt-BOF+CCUS']:
            selected_processes_df = concat_process_dfs(df_dict_c, ['Pelletisation', 'Shaft Furnace', 'Remelt'])
            ef_sum_product = sum_product_ef(selected_processes_df, ef_dict, ['BOF gas', 'BF gas', 'COG'])
            elec_reboiler = 3.6 * ef_sum_product / 1000
            ccs_df.loc[(ccs_df['material_category'] == 'Natural gas'), 'value'] = elec_reboiler
            ccs_df.loc[(ccs_df['material_category'] == 'Natural gas'), 'material_category'] = 'Electricity'

        if technology in ['BAT BF-BOF+CCUS']:
            selected_processes_df = concat_process_dfs(df_dict_c, ['Blast Furnace'])
            ef_sum_product = sum_product_ef(selected_processes_df, ef_dict, ['Electricity'])
            ccs_df.loc[(ccs_df['material_category'] == 'Natural gas'), 'value'] = reboiler_duty_natural_gas * ef_sum_product / 1000

        # Remove last value for smelting reduction
        if technology in ['Smelting Reduction+CCUS']:
            ccs_df = ccs_df[1:]
        
        df_dict_c['CCS'] = ccs_df

    return df_dict_c


def ccu_df_editor(df_dict: dict, technology: str, furnace_group_dict: dict, factor_dict: dict, ef_dict: dict) -> dict:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    df_dict_c = df_dict.copy()
    
    if technology in furnace_group_dict['ccu']:
        ccu_co = df_dict_c['CCU -CO-based'].copy()
        ccu_co2 = df_dict_c['CCU -CO2-based'].copy()
        
        used_co2 = tech_process_getter(bc_processes, technology, process='CCU -CO-based', material='Used CO2')
        ccu_co.loc[(ccu_co['material_category'] == 'Used CO2'), 'value'] = used_co2 * factor_dict['CCU -CO-based']
        ccu_co.loc[(ccu_co['material_category'] == 'Electricity'), 'value'] = 0
        df_dict_c['CCU -CO-based'] = ccu_co

        # co2
        selected_processes_df = concat_process_dfs(df_dict_c, ['Blast Furnace'])
        ef_sum_product_small = sum_product_ef(selected_processes_df, ef_dict, ['Electricity'])
        ccu_co2.loc[(ccu_co2['process_detail'] == 'Reboiler duty'), 'value'] = used_co2 * ef_sum_product_small / 1000
        compression_electricity = tech_process_getter(bc_processes, technology, process='CCU -CO2-based', process_detail='Compression')
        ccu_co2.loc[(ccu_co2['process_detail'] == 'Compression'), 'value'] = compression_electricity * factor_dict['CCU -CO2-based']
        captured_co2 = tech_process_getter(df_dict_c['CCU -CO2-based'], technology, process='CCU -CO2-based', step='Captured CO2')
        ccu_co2.loc[(ccu_co2['material_category'] == 'Captured CO2'), 'value'] = captured_co2 * factor_dict['CCU -CO2-based']
        df_dict_c['CCU -CO2-based'] = ccu_co2
    
    return df_dict_c


# SELF GEN ELECTRICITY
def self_gen_df_editor(
    df_dict: dict,
    technology: str,
    furnace_group_dict: dict,
    factor_dict: dict,
    tech_processes_dict: dict) -> dict:

    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    df_dict_c = df_dict.copy()

    if technology in electricity_self_gen_group:
        self_gen_name = 'Self-Generation Of Electricity'
        self_gen_df = df_dict_c[self_gen_name].copy()

        all_electricity_values = get_all_electricity_values(bc_processes, technology, tech_processes_dict[technology], factor_mapper=factor_dict)
        electricity_share_factor = tech_parameter_getter(bc_parameters, technology, 'Share of electricity purchased in total demand')

        if technology in furnace_group_dict['smelting_reduction']:
            # calulate bof gas
            temp_elec_values = []
            bof_gas = tech_process_getter(bc_processes, technology, process=self_gen_name, material='BOF gas')
            for process in ['Coke Production','Sintering','Pelletisation','Smelting Furnace']:
                elec_val = tech_process_getter(bc_processes, technology, process=process, material='Electricity')
                factor = factor_dict[process]
                temp_elec_values.append(elec_val * factor)
            temp_elec_values.append(factor_dict['Oxygen Generation'])
            bosc_df = df_dict_c['Basic Oxygen Steelmaking + Casting'].copy()
            oxygen_elec_value = bosc_df.loc[bosc_df['process_detail'].eq('Energy-oxygen furnace') & bosc_df['material_category'].eq('Electricity'), 'value'].values[0]
            casting_value = bosc_df.loc[bosc_df['process_detail'].eq('Energy-casting') & bosc_df['material_category'].eq('Electricity'), 'value'].values[0]
            temp_elec_values.append(oxygen_elec_value * factor_dict['Basic Oxygen Steelmaking + Casting'])
            temp_elec_values.append(casting_value * factor_dict['Basic Oxygen Steelmaking + Casting'])
            if technology in ['Smelting Reduction+CCUS']:
                electricity_value_general = tech_process_getter(bc_processes, technology, process='CCS', material='Electricity')
                cap_co2_value = tech_process_getter(bc_processes, technology, process='CCS', step='Captured CO2')
                val_to_append = cap_co2_value * factor_dict['CCS'] * electricity_value_general
                temp_elec_values.append(val_to_append)

            self_gen_df.loc[(self_gen_df['material_category'] == 'BOF gas'), 'value'] = sum(temp_elec_values) * (1 - electricity_share_factor) * bof_gas
            
            # calulate thermal coal
            thermal_coal = tech_process_getter(bc_processes, technology, process=self_gen_name, material='Thermal coal') # changed
            all_steam_values = get_all_steam_values(bc_processes, technology, tech_processes_dict[technology], factor_dict) # changed
            self_gen_df.loc[(self_gen_df['material_category'] == 'Thermal coal'), 'value'] = sum(all_steam_values) * thermal_coal

        if technology in furnace_group_dict['blast_furnace']:
            if technology in ['BAT BF-BOF+CCUS', 'BAT BF-BOF+BECCUS']:
                # Custom calculation for all_electricity_values
                all_electricity_values = all_electricity_values[:-1]
                elec_comp_val = tech_process_getter(bc_processes, technology, process='CCS', process_detail='Compression', material='Electricity')
                cap_co2_val = tech_process_getter(bc_processes, technology, process='CCS', step='Captured CO2')
                cap_co2_val_factored = cap_co2_val * factor_dict['CCS']
                val_to_append = cap_co2_val_factored * elec_comp_val
                all_electricity_values.append(val_to_append)
                
            cog = tech_process_getter(bc_processes, technology, process=self_gen_name, material='COG')
            cog_val = sum(all_electricity_values) * (1 - electricity_share_factor) * cog
            self_gen_df.loc[(self_gen_df['material_category'] == 'COG'), 'value'] = cog_val
            bf_gas = tech_process_getter(bc_processes, technology, process=self_gen_name, material='BF gas')
            bf_gas_val = sum(all_electricity_values) * (1 - electricity_share_factor) * bf_gas
            self_gen_df.loc[(self_gen_df['material_category'] == 'BF gas'), 'value'] = bf_gas_val
            
            if technology in ['BAT BF-BOF_bio PCI', 'BAT BF-BOF_H2 PCI', 'BAT BF-BOF+CCUS', 'BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCU']:
                thermal_coal = tech_process_getter(bc_processes, technology, process=self_gen_name, material='Thermal coal')
                all_steam_values = get_all_steam_values(bc_processes, technology, tech_processes_dict[technology], factor_dict) # changed
                thermal_coal_val = sum(all_steam_values) * thermal_coal
                self_gen_df.loc[(self_gen_df['material_category'] == 'Thermal coal'), 'value'] = thermal_coal_val

        df_dict_c[self_gen_name] = self_gen_df

    return df_dict_c


def concat_process_dfs(df_dict: pd.DataFrame, process_list: list) -> pd.DataFrame:
    df_list = []
    for process in process_list:
        df_list.append(df_dict[process])
    return pd.concat(df_list)


def fix_exceptions(df_dict: dict, technology: str, furnace_group_dict: dict, factor_dict: dict, process_dict: dict) -> dict:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    df_dict_c = df_dict.copy()
    
    # Electricity
    if (technology in furnace_group_dict['blast_furnace']) or (technology in ['Smelting Reduction']):
        electricity_share = tech_parameter_getter(bc_parameters, technology, 'Share of electricity purchased in total demand')
        for process in df_dict_c.keys():
            if 'Electricity' in df_dict_c[process]['material_category'].unique():
                if process == 'Basic Oxygen Steelmaking + Casting':
                    t_df = df_dict_c[process]
                    oxygen_elec_value = t_df.loc[t_df['process_detail'].eq('Energy-oxygen furnace') & t_df['material_category'].eq('Electricity'), 'value']
                    casting_value = t_df.loc[t_df['process_detail'].eq('Energy-casting') & t_df['material_category'].eq('Electricity'), 'value']
                    t_df.loc[t_df['process_detail'].eq('Energy-oxygen furnace') & t_df['material_category'].eq('Electricity'), 'value'] = oxygen_elec_value * electricity_share
                    t_df.loc[t_df['process_detail'].eq('Energy-casting') & t_df['material_category'].eq('Electricity'), 'value'] = casting_value * electricity_share
                    df_dict_c[process] = t_df
                elif process == 'Oxygen Generation':
                    t_df = df_dict_c[process]
                    curr_val = t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value']
                    oxygen_elec_value_final = factor_dict[process]
                    if technology in ['BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCUS']:
                        oxygen_elec_value_final = factor_dict[process] * electricity_share
                    t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value'] = oxygen_elec_value_final
                    df_dict_c[process] = t_df
                elif process != 'Oxygen Generation':
                    t_df = df_dict_c[process]
                    curr_val = t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value']
                    t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value'] = curr_val * electricity_share
                    df_dict_c[process] = t_df
                    
    if technology in ['Smelting Reduction+CCUS']:
        for process in df_dict_c.keys():
            if process == 'Oxygen Generation':
                t_df = df_dict_c[process]
                curr_val = t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value']
                t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value'] = factor_dict[process]
                df_dict_c[process] = t_df
            if process == 'CCS':
                t_df = df_dict_c[process]
                electricity_value_general = tech_process_getter(bc_processes, technology, process=process, material='Electricity')
                cap_co2_value = tech_process_getter(bc_processes, technology, process=process, material='Captured CO2')
                t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value'] = cap_co2_value * factor_dict[process] * electricity_value_general
                df_dict_c[process] = t_df
                
    if technology in furnace_group_dict['dri-bof']:
        for process in df_dict_c.keys():
            if process == 'Basic Oxygen Steelmaking + Casting':
                t_df = df_dict_c[process]
                factor = factor_dict[process]
                oxygen_elec_value = tech_process_getter(bc_processes, technology, process=process, process_detail='Energy-oxygen furnace', material='Electricity')
                casting_value = tech_process_getter(bc_processes, technology, process=process, process_detail='Energy-casting', material='Electricity')
                t_df.loc[t_df['process_detail'].eq('Energy-oxygen furnace') & t_df['material_category'].eq('Electricity'), 'value'] = oxygen_elec_value * factor
                t_df.loc[t_df['process_detail'].eq('Energy-casting') & t_df['material_category'].eq('Electricity'), 'value'] = casting_value * factor
                df_dict_c[process] = t_df
            if process == 'Oxygen Generation':
                t_df = df_dict_c[process]
                curr_val = t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value']
                t_df.loc[t_df['process'].eq(process) & t_df['material_category'].eq('Electricity'), 'value'] = factor_dict[process]
                df_dict_c[process] = t_df
        if technology in ['DRI-Melt-BOF_100% zero-C H2']:
            t_df = df_dict_c['Pelletisation']
            t_df.loc[t_df['step'].eq('Natural gas') & t_df['material_category'].eq('Natural gas'), 'material_category'] = 'Electricity'
            df_dict_c['Pelletisation'] = t_df

    # Natural gas
    if technology in furnace_group_dict['dri-eaf']:
        for process in df_dict_c.keys():
            if process == 'EAF (Steel-making) + Casting':
                t_df = df_dict_c[process]
                factor = factor_dict[process]
                # change natural gas numbers
                pre_heating_ng = tech_process_getter(bc_processes, technology, process=process, process_detail='Pre-heating and process control', material='Natural gas')
                casting_value = tech_process_getter(bc_processes, technology, process=process, process_detail='Energy-casting', material='Natural gas')
                t_df.loc[t_df['process_detail'].eq('Pre-heating and process control') & t_df['material_category'].eq('Natural gas'), 'value'] = pre_heating_ng * factor
                t_df.loc[t_df['process_detail'].eq('Energy-casting') & t_df['material_category'].eq('Natural gas'), 'value'] = casting_value * factor
                
                # rename biomethane to natural gas
                if technology in ['DRI-EAF_50% bio-CH4']:
                    t_df.loc[t_df['material_category'].eq('Biomethane'), 'material_category'] = 'Natural gas'

                df_dict_c[process] = t_df

    # Coke
    if technology in ['Smelting Reduction+CCUS']:
        smelting_furnace_df = df_dict_c['Smelting Furnace'].copy()
        smelting_furnace_df.loc[smelting_furnace_df['material_category'] == 'Coke', 'value'] = 0
        df_dict_c['Smelting Furnace'] = smelting_furnace_df
        
    if technology in furnace_group_dict['blast_furnace']:
        blast_furnace_df = df_dict_c['Blast Furnace'].copy()
        
        lcv = tech_parameter_getter(bc_parameters, technology, 'LCV of injected reductant')
        bat_lcv = tech_parameter_getter(bc_parameters, 'BAT BF-BOF', 'LCV of injected reductant')
        blast_furnace_factor = factor_dict['Blast Furnace']
        coke = tech_process_getter(bc_processes, technology, process='Blast Furnace', material='Coke')
        coke_lcv = tech_parameter_getter(bc_parameters, technology, 'Coke LCV')
        coke_lcv_calculation = coke * coke_lcv * blast_furnace_factor
        blast_furnace_df.loc[blast_furnace_df['material_category'] == 'Coke', 'value'] = coke_lcv_calculation
        
        if technology in ['BAT BF-BOF+BECCUS', 'BAT BF-BOF_bio PCI']:
            # All electricities
            # Biomass
            biomass = tech_process_getter(bc_processes, technology, process='Blast Furnace', process_detail='Tuyere injection', material='Biomass')
            biomass_calculation = biomass * blast_furnace_factor * lcv / 1000
            blast_furnace_df.loc[blast_furnace_df['process_detail'].eq('Tuyere injection') & blast_furnace_df['material_category'].eq('Biomass'), 'value'] = biomass_calculation
            
        if technology in ['BAT BF-BOF+CCU']:
            # All electricities
            # Plastic Waste
            plastic_waste = tech_process_getter(bc_processes, technology, process='Blast Furnace', material='Plastic waste')
            plastic_waste_calculation = plastic_waste * blast_furnace_factor * lcv / 1000
            blast_furnace_df.loc[blast_furnace_df['material_category'] == 'Plastic waste', 'value'] = plastic_waste_calculation
        
        if technology in ['BAT BF-BOF', 'Avg BF-BOF', 'BAT BF-BOF+CCUS', 'BAT BF-BOF_H2 PCI']:
            # All electricities  
            # Thermal Coal
            thermal_coal = tech_process_getter(bc_processes, technology, process='Blast Furnace', material='Thermal coal')
            thermal_coal_calculation = thermal_coal * blast_furnace_factor * bat_lcv / 1000
            blast_furnace_df.loc[blast_furnace_df['material_category'] == 'Thermal coal', 'value'] = thermal_coal_calculation
            
        if technology in ['BAT BF-BOF_H2 PCI']:
            # Hydrogen
            hydrogen = tech_process_getter(bc_processes, technology, process='Blast Furnace', material='Hydrogen')
            hydrogen_calculation = hydrogen * blast_furnace_factor * lcv / 1000
            blast_furnace_df.loc[blast_furnace_df['material_category'] == 'Hydrogen', 'value'] = hydrogen_calculation
                
        df_dict_c['Blast Furnace'] = blast_furnace_df

    if technology in ['DRI-EAF_50% green H2', 'DRI-EAF_50% bio-CH4', 'DRI-Melt-BOF', 'DRI-EAF+CCUS', 'DRI-Melt-BOF+CCUS', 'DRI-EAF']:
        # No change for the following technologies: DRI-EAF_100% green H2, DRI-Melt-BOF_100% zero-C H2, EAF
        
        # Coke
        shaft_furnace_df = df_dict_c['Shaft Furnace'].copy()
        shaft_furnace_factor = factor_dict['Shaft Furnace']
        coke = tech_process_getter(bc_processes, technology, process='Shaft Furnace', material='Coke')
        
        if technology in ['DRI-EAF+CCUS', 'DRI-EAF_50% bio-CH4', 'DRI-EAF']:
            shaft_furnace_df.loc[shaft_furnace_df['material_category'] == 'Coke', 'value'] = coke * shaft_furnace_factor
            
        if technology in ['DRI-Melt-BOF+CCUS']:
            oxygen_consumption = tech_parameter_getter(bc_parameters, technology, 'Oxygen consumption')
            coke_calculation = coke * shaft_furnace_factor * oxygen_consumption
            
        if technology in ['DRI-EAF_50% green H2']:
            # Coke
            shaft_furnace_df.loc[shaft_furnace_df['material_category'] == 'Coke', 'value'] = 0
            
        if technology in ['DRI-Melt-BOF']:
            iron_heat_capacity_solid = tech_parameter_getter(bc_parameters, technology, 'Iron heat capacity - solid')
            shaft_furnace_df.loc[shaft_furnace_df['material_category'] == 'Coke', 'value'] = coke * shaft_furnace_factor * iron_heat_capacity_solid
            
        # Thermal Coal
        if technology in ['DRI-EAF+CCUS']:
            shaft_furnace_df.loc[shaft_furnace_df['material_category'] == 'Thermal coal', 'value'] = 0
        
        if technology in ['DRI-EAF']:
            coal = tech_process_getter(bc_processes, technology, process='Shaft Furnace', material='Coal')
            dri_metallic_fe = tech_parameter_getter(bc_parameters, technology, 'DRI metallic Fe concentration')
            shaft_furnace_df.loc[shaft_furnace_df['material_category'] == 'Coal', 'value'] = coal * shaft_furnace_factor * dri_metallic_fe
            
        if technology in ['DRI-EAF_50% bio-CH4', 'DRI-EAF_50% green H2', 'DRI-Melt-BOF', 'DRI-Melt-BOF+CCUS']:
            thermal_coal = tech_process_getter(bc_processes, technology, process='Shaft Furnace', material='Thermal coal')
    
            if technology in ['DRI-EAF_50% bio-CH4']:
                biomethane_share = tech_parameter_getter(bc_parameters, technology, 'Biomethane share in methane input')
                shaft_furnace_df.loc[shaft_furnace_df['material_category'] == 'Thermal coal', 'value'] = thermal_coal * shaft_furnace_factor * biomethane_share / 1000
            
            if technology in ['DRI-EAF_50% green H2']:
                h2_requirements = tech_parameter_getter(bc_parameters, technology, 'H2 required per 1 t of Fe')
                shaft_furnace_df.loc[shaft_furnace_df['material_category'] == 'Thermal coal', 'value'] = thermal_coal * shaft_furnace_factor * h2_requirements / 1000
                
            if technology in ['DRI-Melt-BOF', 'DRI-Melt-BOF+CCUS']:
                oxygen_consumption = tech_parameter_getter(bc_parameters, technology, 'Oxygen consumption')
                shaft_furnace_df.loc[shaft_furnace_df['material_category'] == 'Thermal coal', 'value'] = thermal_coal * shaft_furnace_factor * oxygen_consumption / 1000
                
        df_dict_c['Shaft Furnace'] = shaft_furnace_df
        
    if technology in ['Electrowinning-EAF', 'Electrowinning-EAF']:
        # Electrolyzer: Coke & Thermal Coal | can be left as zero
        pass
    
    return df_dict_c


def get_material_values(values_dict: dict, tech_processes: dict, technology: str, material: str, stage: str, factor_map: dict) -> pd.DataFrame:
    cols_ref = ['technology', 'process', 'material', 'stage', 'value', 'process_factor_value']
    container = []
    for process in tech_processes[technology]:
        process_factor = factor_map[process]
        if material in values_dict[process]['material_category'].unique():
            df = values_dict[process].copy()
            value = df[df['material_category'] == material]['value'].values.sum()
            container.append([technology, process, material, stage, value, process_factor])
    row_container = []
    for row in container:
        if row:
            row_dict = dict(zip(cols_ref, row))
            row_container.append(row_dict)
    return pd.DataFrame(data=row_container)

def switch_ironore_and_pellets(df_dict: dict) -> pd.DataFrame:
    
    df_dict_c = df_dict.copy()
    if 'Shaft Furnace' in df_dict_c.keys():
        sf_df = df_dict_c['Shaft Furnace'].copy()
        iron_ore_val = sf_df.loc[sf_df['material_category'].eq('Iron ore'), 'value']
        pellets_val = sf_df.loc[sf_df['material_category'].eq('Pellets'), 'value']
        sf_df.loc[sf_df['material_category'].eq('Iron ore'), 'value'] = pellets_val
        sf_df.loc[sf_df['material_category'].eq('Pellets'), 'value'] = iron_ore_val
        df_dict_c['Shaft Furnace'] = sf_df
    
    return df_dict_c


def full_model_flow(technology: str, material: str = None) -> Union[pd.DataFrame, pd.DataFrame]:
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    s1_emissions_factors = read_pickle_folder(PKL_DATA_IMPORTS, "s1_emissions_factors")
    EF_DICT = dict(zip(s1_emissions_factors["Metric"], s1_emissions_factors["Value"]))
    
    if material:
        logger.info(f"- Running the full model and material flow for {technology}")
        df_container = []
        
        process_prod_factor_mapper = create_production_factors(technology, FURNACE_GROUP_DICT, HARD_CODED_FACTORS)
        reformated_dict = create_mini_process_dfs(bc_processes, technology, TECHNOLOGY_PROCESSES, process_prod_factor_mapper)
        df_container.append(get_material_values(reformated_dict, TECHNOLOGY_PROCESSES, technology, material, 'Initial Creation', process_prod_factor_mapper))
        
        reformated_dict_c = reformated_dict.copy()
        
        reformated_dict_c = switch_ironore_and_pellets(reformated_dict_c) # delete this line once fixed!!!!!
        
        reformated_dict_c = limestone_df_editor(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper)
        df_container.append(get_material_values(reformated_dict_c, TECHNOLOGY_PROCESSES, technology, material, 'Limestone Editor', process_prod_factor_mapper))
        
        process_prod_factor_mapper = fix_ccs_factors(reformated_dict_c, process_prod_factor_mapper, technology, FURNACE_GROUP_DICT, EF_DICT)
        
        reformated_dict_c = fix_exceptions(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper, TECHNOLOGY_PROCESSES)
        df_container.append(get_material_values(reformated_dict_c, TECHNOLOGY_PROCESSES, technology, material, 'Fix Exceptions', process_prod_factor_mapper))
        
        process_prod_factor_mapper = fix_ccs_bat_ccus_factors(reformated_dict_c, process_prod_factor_mapper, technology, FURNACE_GROUP_DICT, EF_DICT)
        process_prod_factor_mapper = fix_ccu_factors(reformated_dict_c, process_prod_factor_mapper, technology, FURNACE_GROUP_DICT, EF_DICT)
        reformated_dict_c = ccs_df_editor(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper, EF_DICT)
        df_container.append(get_material_values(reformated_dict_c, TECHNOLOGY_PROCESSES, technology, material, 'CCS', process_prod_factor_mapper))
        
        reformated_dict_c = ccu_df_editor(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper, EF_DICT)
        df_container.append(get_material_values(reformated_dict_c, TECHNOLOGY_PROCESSES, technology, material, 'CCU', process_prod_factor_mapper))
        
        reformated_dict_c = self_gen_df_editor(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper, TECHNOLOGY_PROCESSES)
        df_container.append(get_material_values(reformated_dict_c, TECHNOLOGY_PROCESSES, technology, material, 'Self Gen', process_prod_factor_mapper))

        combined_df = pd.concat(reformated_dict_c.values()).reset_index(drop=True)
        combined_df = format_combined_df(combined_df, PER_T_STEEL_DICT_UNITS)
        
        material_df = pd.concat(df_container)
        return combined_df, material_df
    
    else:
        logger.info(f"- Running the single model flow for {technology}")
        
        process_prod_factor_mapper = create_production_factors(technology, FURNACE_GROUP_DICT, HARD_CODED_FACTORS)
        reformated_dict = create_mini_process_dfs(bc_processes, technology, TECHNOLOGY_PROCESSES, process_prod_factor_mapper)
        reformated_dict_c = reformated_dict.copy()
        reformated_dict_c = switch_ironore_and_pellets(reformated_dict_c) # delete this line once fixed!!!!!
        reformated_dict_c = limestone_df_editor(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper)
        process_prod_factor_mapper = fix_ccs_factors(reformated_dict_c, process_prod_factor_mapper, technology, FURNACE_GROUP_DICT, EF_DICT)
        reformated_dict_c = fix_exceptions(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper, TECHNOLOGY_PROCESSES)
        process_prod_factor_mapper = fix_ccs_bat_ccus_factors(reformated_dict_c, process_prod_factor_mapper, technology, FURNACE_GROUP_DICT, EF_DICT)
        process_prod_factor_mapper = fix_ccu_factors(reformated_dict_c, process_prod_factor_mapper, technology, FURNACE_GROUP_DICT, EF_DICT)
        reformated_dict_c = ccs_df_editor(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper, EF_DICT)
        reformated_dict_c = ccu_df_editor(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper, EF_DICT)
        reformated_dict_c = self_gen_df_editor(reformated_dict_c, technology, FURNACE_GROUP_DICT, process_prod_factor_mapper, TECHNOLOGY_PROCESSES)
        combined_df = pd.concat(reformated_dict_c.values()).reset_index(drop=True)
        combined_df = format_combined_df(combined_df, PER_T_STEEL_DICT_UNITS)
        return combined_df



def generate_full_consumption_table(technology_list: list) -> pd.DataFrame:
    logger.info("- Generating the full resource consumption table")
    summary_df_list = []
    for technology in technology_list:
        logger.info(f"*** Starting standardisation flow for {technology} ***")
        summary_df_list.append(full_model_flow(technology))
    df = pd.concat(summary_df_list)
    df.drop(labels=['process', 'process_detail', 'step'], axis=1, inplace=True)
    return df.groupby(['material_category', 'technology', 'unit']).sum().reset_index()


def concat_process_dfs(df_dict: pd.DataFrame, process_list: list) -> pd.DataFrame:
    df_list = []
    for process in process_list:
        df_list.append(df_dict[process])
    concat_df = pd.concat(df_list)
    return concat_df


@timer_func
def standardise_business_cases(serialize_only: bool = False) -> pd.DataFrame:
    """Standardises the business cases for each technology into per t steel.

    Args:
        serialize_only (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A tabular dataframe containing the standardised business cases
    """
    full_summary_df = generate_full_consumption_table(TECH_REFERENCE_LIST)
    if serialize_only:
        serialize_file(full_summary_df, PKL_DATA_INTERMEDIATE, "standardised_business_cases")
    return full_summary_df
