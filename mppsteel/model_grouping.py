'''Model flow functions for the main script'''
import argparse

from mppsteel.utility.utils import (
    get_logger, pickle_to_csv, stdout_query,
    create_folder_if_nonexist, get_currency_rate
    )

from mppsteel.data_loading.data_import import load_data
from mppsteel.data_loading.reg_steel_demand_formatter import get_steel_demand
from mppsteel.minimodels.timeseries_generator import generate_timeseries
from mppsteel.data_loading.business_case_standardisation import (
    standardise_business_cases
)
from mppsteel.data_loading.business_case_tests import (
    create_bc_test_df, test_all_technology_business_cases
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
from mppsteel.results.graph_production import create_graphs

from mppsteel.model_config import MODEL_YEAR_END, OUTPUT_FOLDER, PKL_DATA_FINAL, PKL_DATA_INTERMEDIATE, BC_TEST_FOLDER

logger = get_logger("Main Model Code")

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

def add_currency_rates_to_scenarios(scenario_dict: dict):
    scenario_dict['eur_usd'] = get_currency_rate('eur')
    scenario_dict['usd_eur'] = get_currency_rate('usd')
    return scenario_dict


# Model phasing
def data_import_stage():
    load_data(serialize_only=True)
    get_steel_demand(serialize_only=True)
    format_pe_data(serialize_only=True)
    standardise_business_cases(serialize_only=True)
    create_country_ref(serialize_only=True)

def data_preprocessing_phase(scenario_dict: dict):
    generate_timeseries(serialize_only=True, scenario_dict=scenario_dict)
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

def model_outputs_phase(new_folder: bool = False, timestamp: str = ''):
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f'{OUTPUT_FOLDER}/{timestamp}'
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    pkl_files = [
        'production_stats_all', 'production_emissions',
        'global_metaresults', 'investment_results']
    for pkl_file in pkl_files:
        pickle_to_csv(save_path, PKL_DATA_FINAL, pkl_file)
    pickle_to_csv(save_path, PKL_DATA_INTERMEDIATE, 'emissions_switching_df_full')

def model_graphs_phase(new_folder: bool = False, timestamp: str = ''):
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f'{OUTPUT_FOLDER}/{timestamp}/graphs'
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    create_graphs(save_path)

# Group phases
def data_import_refresh():
    data_import_stage()

def data_preprocessing_refresh(scenario_dict: dict):
    data_preprocessing_phase(scenario_dict)

def data_import_and_preprocessing_refresh(scenario_dict: dict):
    data_import_stage()
    data_preprocessing_phase(scenario_dict)

def scenario_batch_run(scenario_dict: dict, dated_output_folder: bool, timestamp: str):
    data_preprocessing_phase(scenario_dict)
    model_calculation_phase(scenario_dict)
    model_results_phase(scenario_dict)
    model_outputs_phase(dated_output_folder, timestamp)

def half_model_run(scenario_dict: dict, dated_output_folder: bool, timestamp: str):
    model_calculation_phase(scenario_dict)
    model_results_phase(scenario_dict)
    model_outputs_phase(dated_output_folder, timestamp)
    model_graphs_phase(dated_output_folder, timestamp)

def results_and_output(scenario_dict: dict, dated_output_folder: bool, timestamp: str):
    model_results_phase(scenario_dict)
    model_outputs_phase(dated_output_folder, timestamp)
    model_graphs_phase(dated_output_folder, timestamp)

def outputs_only(dated_output_folder: bool, timestamp: str):
    model_outputs_phase(dated_output_folder, timestamp)
    model_graphs_phase(dated_output_folder, timestamp)

def graphs_only(timestamp: str, dated_output_folder: bool):
    model_graphs_phase(dated_output_folder, timestamp)

def full_flow(scenario_dict: dict, dated_output_folder: bool, timestamp: str):
    data_import_and_preprocessing_refresh(scenario_dict)
    half_model_run(scenario_dict, dated_output_folder, timestamp)

def business_case_tests(new_folder: bool = False, timestamp: str = '', create_test_df: bool = True):
    save_path = BC_TEST_FOLDER
    if new_folder:
        folder_filepath = f'{BC_TEST_FOLDER}/{timestamp}'
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    if create_test_df:
        create_bc_test_df(serialize_only=True)
    test_all_technology_business_cases(save_path)

def generate_minimodels(scenario_dict: dict):
    generate_timeseries(serialize_only=True, scenario_dict=scenario_dict)

def investment_flow(scenario_dict: dict):
    investment_results(scenario_dict, serialize_only=True)

parser = argparse.ArgumentParser(description='The MPP Python Steel Model Command Line Interface', add_help=False)
parser.add_argument(
    "-f", "--full_model", action="store_true", help="Runs the complete model flow")
parser.add_argument(
    "-s", "--solver", action="store_true", help="Runs the solver scripts directly")
parser.add_argument(
    "-p", "--preprocessing", action="store_true", help="Runs the preprocessing scripts directly")
parser.add_argument(
    "-m", "--production_and_investment", action="store_true", help="Runs the production and investment scripts")
parser.add_argument(
    "-o", "--output", action="store_true", help="Runs the output scripts directly")
parser.add_argument(
    "-h", "--half_model", action="store_true", help="Runs the half model sctips scripts directly")
parser.add_argument(
    "-i", "--data_import", action="store_true", help="Runs the data import scripts scripts directly")
parser.add_argument(
    "-d", "--data_refresh", action="store_true", help="Runs the data refresh scripts directly")
parser.add_argument(
    "-r", "--results", action="store_true", help="Runs the model results scripts directly")
parser.add_argument(
    "-b", "--business_cases", action="store_true", help="Runs the business cases script directly")
parser.add_argument(
    "-v", "--variable_costs", action="store_true", help="Runs the variable costs sumary script directly")
parser.add_argument(
    "-q", "--custom_scenario", action="store_true", help="Adds custom scenario inputs to the model")
parser.add_argument(
    "-c", "--choose_scenario", action="store", help="Runs a single fixed scenario to the model that you can specify by name")
parser.add_argument(
    "-a", "--all_scenarios", action="store_true", help="Runs all fixed scenarios in the model")
parser.add_argument(
    "-t", "--results_and_output", action="store_true", help="Runs the results and output scripts directly")
parser.add_argument(
    "-g", "--graphs", action="store_true", help="Runs the graph output script directly")
parser.add_argument(
    "-n", "--minimodels", action="store_true", help="Runs the minimodels script directly")
parser.add_argument(
    "-e", "--investment", action="store_true", help="Runs the investments script directly")
