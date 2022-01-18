"""Runs the data loading scripts"""

from datetime import datetime

from mppsteel.utility.utils import get_logger, create_folders_if_nonexistant, TIME_CONTAINER

from mppsteel.model_config import DEFAULT_SCENARIO, SCENARIO_OPTIONS, FOLDERS_TO_CHECK_IN_ORDER

from mppsteel.model_grouping import *

logger = get_logger("Main Model Code")

if __name__ == "__main__":

    args = parser.parse_args()

    scenario_args = DEFAULT_SCENARIO
    scenario_args = add_currency_rates_to_scenarios(scenario_args)

    timestamp = datetime.today().strftime('%d-%m-%y %H-%M')
    logger.info(f'Model running at {timestamp}')

    create_folders_if_nonexistant(FOLDERS_TO_CHECK_IN_ORDER)

    if args.q:
        logger.info(f'Including custom parameter inputs')
        scenario_args = get_inputted_scenarios(scenario_options=SCENARIO_OPTIONS, default_scenario=scenario_args)

    logger.info(f'Running model with the following parameters {scenario_args}')

    if args.f:
        full_flow(scenario_dict=scenario_args, dated_output_folder=True, timestamp=timestamp)

    if args.s:
        model_calculation_phase(scenario_dict=scenario_args)

    if args.m:
        model_results_phase(scenario_dict=scenario_args)

    if args.o:
        outputs_only(dated_output_folder=True, timestamp=timestamp)

    if args.h:
        half_model_run(scenario_dict=scenario_args, dated_output_folder=True, timestamp=timestamp)

    if args.i:
        data_import_refresh()

    if args.p:
        data_preprocessing_phase(scenario_dict=scenario_args)

    if args.d:
        data_import_and_preprocessing_refresh(scenario_dict=scenario_args)

    if args.r:
        model_results_phase(scenario_dict=scenario_args)

    if args.b:
        standardise_business_cases(serialize_only=True)
        business_case_tests(new_folder=True, timestamp=timestamp, create_test_df=False)

    if args.v:
        generate_variable_plant_summary(scenario_dict=scenario_args, serialize_only=True)

    if args.t:
        results_and_output(scenario_dict=scenario_args, dated_output_folder=True, timestamp=timestamp)

    if args.g:
        graphs_only(timestamp=timestamp, dated_output_folder=True)

    if args.n:
        generate_minimodels(scenario_dict=scenario_args)

    if args.e:
        investment_flow(scenario_dict=scenario_args)

    TIME_CONTAINER.return_time_container()
