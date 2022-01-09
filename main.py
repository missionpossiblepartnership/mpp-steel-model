"""Runs the data loading scripts"""

import argparse

from mppsteel.utility.utils import get_logger, pickle_to_csv, stdout_query, TIME_CONTAINER

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

from mppsteel.model_config import MODEL_YEAR_END, DEFAULT_SCENARIO, SCENARIO_OPTIONS

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

def model_outputs_phase():
    pickle_to_csv('production_stats_all')
    pickle_to_csv('production_emissions')
    pickle_to_csv('global_metaresults')
    pickle_to_csv('investment_results_df')

# Group phases
def data_import_refresh():
    data_import_stage()

def data_preprocessing_refresh(scenario_dict: dict):
    data_preprocessing_phase(scenario_dict)

def data_import_and_preprocessing_refresh(scenario_dict: dict):
    data_import_stage()
    data_preprocessing_phase(scenario_dict)

def half_model_run(scenario_dict: dict):
    model_calculation_phase(scenario_dict)
    model_results_phase(scenario_dict)
    model_outputs_phase()

def outputs_only():
    model_outputs_phase()

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


parser = argparse.ArgumentParser(description='The MPP Python Steel Model Command Line Interface')
parser.add_argument(
    "--f", action="store_true", help="Runs the complete model flow")
parser.add_argument(
    "--s", action="store_true", help="Runs the solver scripts directly")
parser.add_argument(
    "--p", action="store_true", help="Runs the preprocessing scripts directly")
parser.add_argument(
    "--m", action="store_true", help="Runs the production and investment scripts")
parser.add_argument(
    "--o", action="store_true", help="Runs the output scripts directly")
parser.add_argument(
    "--h", action="store_true", help="Runs the half model sctips scripts directly")
parser.add_argument(
    "--i", action="store_true", help="Runs the data import scripts scripts directly")
parser.add_argument(
    "--r", action="store_true", help="Runs the data refresh scripts directly")
parser.add_argument(
    "--b", action="store_true", help="Runs the business cases script directly")
parser.add_argument(
    "--v", action="store_true", help="Runs the variable costs sumary script directly")
parser.add_argument(
    "--q", action="store_true", help="Adds custom scenario inputs to the model")

if __name__ == "__main__":

    args = parser.parse_args()

    scenario_args = DEFAULT_SCENARIO

    if args.q:
        logger.info(f'Including custom parameter inputs')
        scenario_args = get_inputted_scenarios(SCENARIO_OPTIONS, scenario_args)

    logger.info(f'Running model with the following parameters {scenario_args}')

    if args.f:
        full_flow(scenario_args)

    if args.s:
        model_calculation_phase(scenario_args)

    if args.m:
        model_results_phase(scenario_args)

    if args.o:
        outputs_only()

    if args.h:
        half_model_run(scenario_args)

    if args.i:
        data_import_refresh()

    if args.p:
        data_preprocessing_phase(scenario_args)

    if args.r:
        data_import_and_preprocessing_refresh(scenario_args)

    if args.b:
        standardise_business_cases(serialize_only=True)

    if args.v:
        generate_variable_plant_summary(scenario_args, serialize_only=True)

    TIME_CONTAINER.return_time_container()
