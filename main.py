"""Runs the data loading scripts"""

import argparse

from mppsteel.utility.utils import get_logger, TIME_CONTAINER

from mppsteel.model_config import DEFAULT_SCENARIO, SCENARIO_OPTIONS

from model_grouping import *

logger = get_logger("Main Model Code")

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
    "--d", action="store_true", help="Runs the data refresh scripts directly")
parser.add_argument(
    "--r", action="store_true", help="Runs the model results scripts directly")
parser.add_argument(
    "--b", action="store_true", help="Runs the business cases script directly")
parser.add_argument(
    "--v", action="store_true", help="Runs the variable costs sumary script directly")
parser.add_argument(
    "--q", action="store_true", help="Adds custom scenario inputs to the model")
parser.add_argument(
    "--t", action="store_true", help="Runs the results and output scripts directly")

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

    if args.d:
        data_import_and_preprocessing_refresh(scenario_args)

    if args.r:
        model_results_phase(scenario_args)

    if args.b:
        standardise_business_cases(serialize_only=True)

    if args.v:
        generate_variable_plant_summary(scenario_args, serialize_only=True)
    
    if args.t:
        results_and_output(scenario_args)

    TIME_CONTAINER.return_time_container()
