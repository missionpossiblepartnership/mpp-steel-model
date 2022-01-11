'''Model flow functions for the main script'''

from datetime import datetime

from mppsteel.utility.utils import (
    get_logger, pickle_to_csv, stdout_query,
    create_folder_if_nonexist, get_today_time
    )

from mppsteel.data_loading.data_import import load_data
from mppsteel.data_loading.reg_steel_demand_formatter import get_steel_demand
from mppsteel.minimodels.hydrogen_minimodel import generate_hydrogen_timeseries
from mppsteel.minimodels.timeseries_generator import generate_timeseries
from mppsteel.data_loading.business_case_standardisation import (
    standardise_business_cases,
)
from mppsteel.data_loading.pe_model_formatter import format_pe_data
from mppsteel.data_loading.steel_plant_formatter import steel_plant_processor
from mppsteel.data_loading.country_reference import create_country_ref
from mppsteel.data_loading.data_interface import (
    create_capex_opex_dict,
    generate_preprocessed_emissions_data,
)
from mppsteel.model.emissions_reference_tables import generate_emissions_flow
from mppsteel.model.capex_switching import create_capex_timeseries
from mppsteel.model.emissions import calculate_emissions
from mppsteel.model.investment_cycles import investment_cycle_flow
from mppsteel.model.variable_plant_cost_archetypes import generate_variable_plant_summary
from mppsteel.model.solver import solver_flow
from mppsteel.results.production import production_results_flow
from mppsteel.results.investments import investment_results

from mppsteel.model_config import MODEL_YEAR_END, OUTPUT_FOLDER

logger = get_logger("Main Model Code")

# Model phasing
def data_import_stage():
    load_data(serialize_only=True)
    get_steel_demand(serialize_only=True)
    format_pe_data(serialize_only=True)
    generate_hydrogen_timeseries(serialize_only=True)
    generate_timeseries(serialize_only=True)
    standardise_business_cases(serialize_only=True)
    create_country_ref(serialize_only=True)

def data_preprocessing_phase(scenario_dict: dict):
    steel_plant_processor(serialize_only=True, remove_non_operating_plants=True)
    create_capex_opex_dict(serialize_only=True)
    generate_preprocessed_emissions_data(serialize_only=True)
    generate_emissions_flow(serialize_only=True)
    create_capex_timeseries(serialize_only=True)
    calculate_emissions(year_end=MODEL_YEAR_END, output_type="summary", serialize_only=True)
    investment_cycle_flow(serialize_only=True)
    generate_variable_plant_summary(scenario_dict, serialize_only=True)

def model_calculation_phase(scenario_dict: dict):
    solver_flow(scenario_dict, year_end=MODEL_YEAR_END, serialize_only=True)

def model_results_phase(scenario_dict: dict):
    production_results_flow(scenario_dict, serialize_only=True)
    investment_results(scenario_dict, serialize_only=True)

def model_outputs_phase(new_folder: bool = False):
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_time = datetime.today().strftime('%d-%m-%y %H:%M')
        folder_filepath = f'{OUTPUT_FOLDER}/{folder_time}'
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    pkl_files = [
        'production_stats_all', 'production_emissions',
        'global_metaresults', 'investment_results_df']
    for pkl_file in pkl_files:
        pickle_to_csv(save_path, pkl_file)

# Group phases
def data_import_refresh():
    data_import_stage()

def data_preprocessing_refresh(scenario_dict: dict):
    data_preprocessing_phase(scenario_dict)

def data_import_and_preprocessing_refresh(scenario_dict: dict):
    data_import_stage()
    data_preprocessing_phase(scenario_dict)

def half_model_run(scenario_dict: dict, dated_output_folder: bool):
    model_calculation_phase(scenario_dict)
    model_results_phase(scenario_dict)
    model_outputs_phase(dated_output_folder)

def results_and_output(scenario_dict: dict, dated_output_folder: bool):
    model_results_phase(scenario_dict)
    model_outputs_phase(dated_output_folder)

def outputs_only(dated_output_folder: bool):
    model_outputs_phase(dated_output_folder)

def full_flow(scenario_dict: dict):
    data_import_and_preprocessing_refresh(scenario_dict)
    half_model_run(scenario_dict)

def business_case_flow():
    standardise_business_cases(serialize_only=True)

def stdout_question(count_iter: int, scenario_type: str, scenario_options: dict, default_dict: dict):
    query = f'''
    Scenario Option {count_iter+1}/{len(scenario_options)}: {scenario_type}
    Default value: {default_dict[scenario_type]}.
    To keep default, leave blank and press ENTER, else enter a different value from the options presented.
    ---> Options {scenario_options[scenario_type]}
    '''
    return query

def get_inputted_scenarios(scenario_options: dict, default_scenario: dict):
    inputted_scenario_args = {}
    for count, scenario in enumerate(scenario_options.keys()):
        question = stdout_question(count, scenario, scenario_options, default_scenario)
        inputted_scenario_args[scenario] = stdout_query(question, default_scenario[scenario], scenario_options[scenario])
    return inputted_scenario_args
