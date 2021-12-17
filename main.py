"""Runs the data loading scripts"""

import argparse

from mppsteel.utility.utils import get_logger, pickle_to_csv, TIME_CONTAINER

from mppsteel.data_loading.data_import import load_data
from mppsteel.minimodels.hydrogen_minimodel import generate_hydrogen_timeseries
from mppsteel.minimodels.timeseries_generator import generate_timeseries
from mppsteel.data_loading.business_case_standardisation import (
    standardise_business_cases,
)
from mppsteel.data_loading.natural_resource_data_interface import (
    natural_resource_preprocessor,
)
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

from mppsteel.model_config import MODEL_YEAR_END

logger = get_logger("Main Model Code")

# Model phasing
def data_import_stage():
    load_data(serialize_only=True)
    generate_hydrogen_timeseries(serialize_only=True)
    generate_timeseries(serialize_only=True)
    standardise_business_cases(serialize_only=True)
    # natural_resource_preprocessor(serialize_only=True)
    create_country_ref(serialize_only=True)

def data_preprocessing_phase():
    steel_plant_processor(serialize_only=True, remove_non_operating_plants=True)
    create_capex_opex_dict(serialize_only=True)
    generate_preprocessed_emissions_data(serialize_only=True)
    generate_emissions_flow(serialize_only=True)
    create_capex_timeseries(serialize_only=True)
    calculate_emissions(
        year_end=MODEL_YEAR_END, output_type="summary", serialize_only=True)
    investment_cycle_flow(serialize_only=True)
    generate_variable_plant_summary(serialize_only=True)

def model_calculation_phase():
    solver_flow(year_end=MODEL_YEAR_END, serialize_only=True)

def model_results_phase():
    production_results_flow(serialize_only=True)
    investment_results(serialize_only=True)

def model_outputs_phase():
    pickle_to_csv('production_stats_all')
    pickle_to_csv('production_emissions')
    pickle_to_csv('global_metaresults')
    pickle_to_csv('investment_results_df')

# Group phases
def data_import_refresh():
    data_import_stage()

def data_preprocessing_refresh():
    data_preprocessing_phase()

def data_import_and_preprocessing_refresh():
    data_import_stage()
    data_preprocessing_phase()

def half_model_run():
    model_calculation_phase()
    model_results_phase()
    model_outputs_phase()

def outputs_only():
    model_outputs_phase()

def full_flow():
    data_import_and_preprocessing_refresh()
    half_model_run()

def business_case_flow():
    standardise_business_cases(serialize_only=True)


parser = argparse.ArgumentParser(description='The MPP Python Steel Model Command Line Interface')
parser.add_argument(
    "--a", action="store_true", help="Runs the complete model flow")
parser.add_argument(
    "--s", action="store_true", help="Runs the solver scripts directly")
parser.add_argument(
    "--p", action="store_true", help="Runs the production scripts directly")
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

if __name__ == "__main__":

    args = parser.parse_args()

    if args.a:
        full_flow()

    if args.s:
        model_calculation_phase()

    if args.p:
        model_results_phase()

    if args.o:
        outputs_only()

    if args.h:
        half_model_run()

    if args.i:
        data_import_refresh()

    if args.r:
        data_import_and_preprocessing_refresh()

    TIME_CONTAINER.return_time_container()