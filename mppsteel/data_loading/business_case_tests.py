"""Script to test the business cases"""

from itertools import groupby

import pandas as pd
import numpy as np

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

# For logger
from mppsteel.utility.utils import (
    serialize_file, extract_data,
    read_pickle_folder, timer_func, enumerate_columns
)
from mppsteel.utility.log_utility import get_logger

from mppsteel.model_config import (
    IMPORT_DATA_PATH,
    PKL_DATA_IMPORTS,
    PKL_DATA_INTERMEDIATE,
)

from mppsteel.utility.reference_lists import (
    TECH_REFERENCE_LIST,
    FURNACE_GROUP_DICT,
    TECHNOLOGY_PROCESSES,
    bosc_factor_group,
    eaf_factor_group,
    electricity_and_steam_self_gen_group,
    electricity_self_gen_group,
    HARD_CODED_FACTORS,
)

from mppsteel.data_loading.standardise_business_cases import (
    full_model_flow, create_hardcoded_exceptions,
    business_case_formatter_splitter,
    create_production_factors, furnace_group_from_tech
)

# Create logger
logger = get_logger("Business Case Tests")

return_strings = lambda x: [y for y in x if isinstance(y, str)]

def create_full_process_summary(bc_process_df: pd.DataFrame):
    material_list = return_strings(bc_process_df['material_category'].unique())
    main_container = []
    for technology in tqdm(TECH_REFERENCE_LIST, total=len(TECH_REFERENCE_LIST), desc='Business Case Full Summary'):
        for material_ref in material_list:
            if material_ref in return_strings(bc_process_df[bc_process_df['technology'] == technology]['material_category'].unique()):
                throw, keep = full_model_flow(technology, material_ref)
                main_container.append(keep)
    return pd.concat(main_container).reset_index(drop=True)

def master_getter(df, materials_ref, tech, material, rounding = 3):
    if material in materials_ref:
        return round(df.loc[tech, material].values[0], rounding)
    return 0

def process_inspector(df: pd.DataFrame, excel_bc_summary: pd.DataFrame, rounding = 3):
    df_c = df.copy()
    df_c['ref_value'] = ''
    df_c['matches_ref'] = ''
    materials_ref = excel_bc_summary.index.get_level_values(1).unique()

    def value_mapper(row, enum_dict):
        ref_value = master_getter(excel_bc_summary, materials_ref, row[enum_dict['technology']], row[enum_dict['material']], rounding)
        calculated_value = round(row[enum_dict['value']], rounding)
        if calculated_value == ref_value:
            row[enum_dict['matches_ref']] = 1
        else:
            row[enum_dict['matches_ref']] = 0
        row[enum_dict['ref_value']] = ref_value
    tqdma.pandas(desc="Prrocess Inspector")
    enumerated_cols = enumerate_columns(df_c.columns)
    df_c = df_c.progress_apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    return df_c


def inspector_getter(df, technology, material=None, process=None):
    row_order = ['process_factor_value', 'value', 'ref_value', 'matches_ref']
    df_c = df.copy()
    df_c.set_index(['technology',  'material', 'stage', 'process'], inplace=True)
    if material:
        return df_c.xs(key=(technology, material), level=['technology', 'material'])[row_order]
    if process:
        return df_c.xs(key=(technology, process), level=['technology', 'process'])[row_order]

def get_summary_dict_from_idf(df, technology, material, function_order=None, rounding=3):
    if not function_order:
        function_order = ['Initial Creation', 'Limestone Editor', 'CCS', 'CCU', 'Self Gen']
    df_c = df.set_index(['technology', 'material']).xs((technology, material)).reset_index().sort_index()
    return df_c.groupby(['technology', 'material', 'stage']) \
        .agg('sum')['value'].droplevel(['technology', 'material']) \
        .round(rounding).reindex(function_order) \
        .to_dict()

def all_equal(iterable):
    g = groupby(iterable)
    return next(g, True) and not next(g, False)

def all_process_values(dfi, tech, material, rounding=3):
    cont_dict = {}
    df = inspector_getter(dfi, tech, material).copy()['value']
    for stage in df.index.get_level_values(0).unique():
        cont_dict[stage] = df.loc[stage].round(rounding).to_dict()
    return cont_dict

def inspector_df_flow(bc_master_df: pd.DataFrame):
    logger.info(f'Running all model flows')
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    create_full_process_summary_df = create_full_process_summary(bc_processes)
    bc_master_c = bc_master_df.copy()
    return process_inspector(create_full_process_summary_df, bc_master_c)

def check_matches(i_df: pd.DataFrame, materials_ref: list, technology: str, file_obj = None, rounding: int = 3):
    logger.info(f'-- Printing results for {technology}')
    process_prod_factor_mapper = create_production_factors(technology, FURNACE_GROUP_DICT, HARD_CODED_FACTORS)
    furnace_group = furnace_group_from_tech(FURNACE_GROUP_DICT)[technology]
    materials_list = materials_ref[:-4].copy()
    tech_processes = TECHNOLOGY_PROCESSES[technology].copy()
    
    
    hard_coded_exception_check = False
    bosc_factor_group_check = False
    eaf_factor_group_check = False
    electricity_and_steam_self_gen_group_check = False
    electricity_self_gen_group_check = False
    
    if technology in create_hardcoded_exceptions(HARD_CODED_FACTORS, FURNACE_GROUP_DICT):
        hard_coded_exception_check = True
    if technology in bosc_factor_group:
        bosc_factor_group_check = True
    if technology in eaf_factor_group:
        eaf_factor_group_check = True
    if technology in electricity_and_steam_self_gen_group:
        electricity_and_steam_self_gen_group_check = True
    if technology in electricity_self_gen_group:
        electricity_self_gen_group_check = True
        
    pretty_dict_flow = lambda dict_obj: ' -> '.join([f'{step}: {value}' for step, value in dict_obj.items()])
    
    if file_obj:
        def write_line_to_file(line, file_obj=file_obj):
            file_obj.write(f'\n{line}')
        
    write_line_to_file(f'============================ RESULTS FOR {technology} ============================')
    write_line_to_file('')
    write_line_to_file(f'Furnace Group: {furnace_group}')
    write_line_to_file(f'hard_coded_exception_check: {hard_coded_exception_check or False}')
    write_line_to_file(f'bosc_factor_group_check: {bosc_factor_group_check or False}')
    write_line_to_file(f'eaf_factor_group_check: {eaf_factor_group_check or False}')
    write_line_to_file(f'electricity_and_steam_self_gen_group_check: {electricity_and_steam_self_gen_group_check or False}')
    write_line_to_file(f'electricity_self_gen_group_check: {electricity_self_gen_group_check or False}')

    write_line_to_file('')

        # iterate over every material
        
    for process in tech_processes:
        write_line_to_file(f'============== {process} results ==============')
        process_factor = round(process_prod_factor_mapper[process], rounding)
        write_line_to_file(f'Factor: {process_factor}')
        write_line_to_file('-------------------')
        df_i = inspector_getter(i_df, technology, process=process).copy()

        # iterate over every material
        for material in df_i.index.get_level_values(0).unique():
            write_line_to_file(f'Material: {material}')
            write_line_to_file('')
            
            df_im = df_i.xs(key=material, level='material').copy()
            value_match_array = df_im['matches_ref'].values
            stage_index = df_im.index
            ref_value = round(df_im.ref_value.values[0], rounding)
            values = df_im.value.values.round(rounding)
            stage_values = dict(zip(stage_index, values))
            stage_values_string = pretty_dict_flow(stage_values)
            summary_values = get_summary_dict_from_idf(i_df, technology, material, rounding=rounding)
            summary_values_flow = pretty_dict_flow(summary_values)
            self_gen_summary_value = summary_values['Self Gen']
            
            all_values = all_process_values(i_df, technology, material, rounding)
            av_keys = all_values.keys()
            summ_dict = {}
            for key in av_keys:
                summ_dict[key] = round(sum(all_values[key].values()), rounding)

            write_line_to_file('Summary values across processes')
            write_line_to_file(f'Total usage (Excel Version): {ref_value}')
            write_line_to_file(f'Total usage (Python Version): {self_gen_summary_value}')
            write_line_to_file(summ_dict)
            

            write_line_to_file('')
            if self_gen_summary_value == ref_value:
                write_line_to_file(f'Final value matches excel version')
                write_line_to_file(f'ACTION: Do Nothing')

            else:
                for stage, val in summary_values.items():
                    if val == ref_value:
                        write_line_to_file('*** INSIGHT: Match Overwritten ***')
                        write_line_to_file(f'Initial match at {stage} but overwritten later. Follow the stack below.')
                        write_line_to_file(f'Aggregate stack: {summary_values_flow}')
                        write_line_to_file('')

                check_equality = lambda it: all(x==it[0] for x in it)
                stage_vals = list(stage_values.values())
                summary_vals = list(summary_values.values())

                write_line_to_file('')
                if check_equality(summary_vals):
                    write_line_to_file(f'No differences amongst total material consumption for each stage.')
                else:
                    write_line_to_file(f'There are differences amongst total material consumption for each stage.')
                    write_line_to_file(f'ACTION: Check the calculations.')
                    write_line_to_file(f'Aggregate stack: {summary_values_flow}')


                write_line_to_file('')
                if check_equality(stage_vals):
                    write_line_to_file(f'No differences amongst stages for {process}')
                    write_line_to_file(f'ACTION: Check process factor value ({process_factor}).')
                else:
                    write_line_to_file(f'There are differences amongst total material consumption within {process}. Follow the stack below.')
                    write_line_to_file(f'Check the calculations sources of discrepancy.')
                    write_line_to_file(f'Process Stack: {stage_values_string}')

                    # case 2: Pairwise checks: Finding the stage where the discrepancy starts
                    index_track = 0
                    for value in range(value_match_array.size-1):
                        subarray = np.array(value_match_array[index_track:index_track+2])
                        if np.array_equal(subarray, np.array([1,0])):
                            second_stage = stage_index[index_track+1]
                            write_line_to_file(f'*** INSIGHT: Problem function found ***')
                            write_line_to_file(f'Fix this function -> {second_stage}')
                        index_track += 1

                    # case 3: Pairwise checks: Fixes that must be later reversed  
                    index_track = 0  
                    for value in range(value_match_array.size-1):
                        subarray = np.array(value_match_array[index_track:index_track+2])
                        if np.array_equal(subarray, np.array([0,1])):
                            second_stage = stage_index[index_track+1]
                            write_line_to_file(f'*** INSIGHT: THIS FUNCTION DOES THE RIGHT THING: {second_stage} ***')
                            write_line_to_file(f'-- Stage calculations: {stage_values_string}')
                        index_track += 1
                write_line_to_file('')
                write_line_to_file(all_values)
                
                write_line_to_file('')
            write_line_to_file('-------------------')
    write_line_to_file(f'============================ END OF RESULTS ============================')

def write_tech_report_to_file(df_i: pd.DataFrame, materials_ref: pd.Index, technology: str, folder_path: str):
    logger.info(f'-- {technology} test')
    file_path = f'{folder_path}/{technology}.txt'
    f = open(file_path, 'w', encoding='utf-8')
    check_matches(df_i, materials_ref, technology, file_obj=f)
    f.close()

@timer_func
def create_bc_test_df(serialize_only: bool):
    logger.info(f'Creating business case tests')
    business_case_master = extract_data(IMPORT_DATA_PATH, "Business Cases Excel Master", "csv")
    bc_master = business_case_master.drop(labels=['Type of metric', 'Unit'],axis=1).melt(
        id_vars=['Material'], var_name='Technology').set_index(['Technology', 'Material']).copy()
    df_inspector = inspector_df_flow(bc_master)
    if serialize_only:
        serialize_file(df_inspector, PKL_DATA_INTERMEDIATE, "business_case_test_df")

def test_all_technology_business_cases(folder_path: str):
    logger.info(f'Writing business case tests to path: {folder_path}')
    business_case_master = extract_data(IMPORT_DATA_PATH, "Business Cases Excel Master", "csv")
    bc_master = business_case_master.drop(labels=['Type of metric', 'Unit'],axis=1).melt(
        id_vars=['Material'], var_name='Technology').set_index(['Technology', 'Material']).copy()
    materials_ref = list(bc_master.index.get_level_values(1).unique())
    df_inspector = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'business_case_test_df', 'df')
    for technology in tqdm(TECH_REFERENCE_LIST, total=len(TECH_REFERENCE_LIST), desc='Writing tests to file'):
        write_tech_report_to_file(df_inspector, materials_ref, technology, folder_path)
