"""Runs the data loading scripts"""

from datetime import datetime
from itertools import product

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import create_folders_if_nonexistant
from mppsteel.utility.function_timer_utility import TIME_CONTAINER

from mppsteel.config.model_config import FOLDERS_TO_CHECK_IN_ORDER

from mppsteel.config.model_scenarios import DEFAULT_SCENARIO, SCENARIO_SETTINGS, SCENARIO_OPTIONS

from mppsteel.config.model_grouping import *

logger = get_logger("Main Model Code")

if __name__ == "__main__":

    args = parser.parse_args()

    scenario_args = DEFAULT_SCENARIO
    scenario_args = add_currency_rates_to_scenarios(scenario_args)

    timestamp = datetime.today().strftime('%d-%m-%y %H-%M')
    logger.info(f'Model running at {timestamp}')

    create_folders_if_nonexistant(FOLDERS_TO_CHECK_IN_ORDER)

    if args.custom_scenario:
        logger.info(f'Including custom parameter inputs')
        scenario_args = get_inputted_scenarios(scenario_options=SCENARIO_SETTINGS, default_scenario=scenario_args)

    if args.choose_scenario:
        if args.choose_scenario in SCENARIO_OPTIONS.keys():
            logger.info(f'CORRECT SCENARIO CHOSEN: {args.choose_scenario}')
            scenario_args = SCENARIO_OPTIONS[args.choose_scenario]
        else:
            scenario_options = list(SCENARIO_OPTIONS.keys())
            logger.info(f'INVALID SCENARIO INPUT: {args.choose_scenario}, please choose from {scenario_options}')

    if args.all_scenarios:
        all_options = list(product(*SCENARIO_SETTINGS.values()))
        logger.info(f'Running ALL {len(all_options)} SCENARIO options')
        '''
        data_import_refresh()
        for scenario in all_options:
            scenario_run = add_currency_rates_to_scenarios(scenario)
            scenario_batch_run(scenario_dict=scenario_run, dated_output_folder=True, timestamp=timestamp)
        '''

    logger.info(f'''Running model with the following parameters:  
    {scenario_args}''')

    if args.full_model:
        full_flow(scenario_dict=scenario_args, dated_output_folder=True, timestamp=timestamp)

    if args.solver:
        model_calculation_phase(scenario_dict=scenario_args)

    if args.minimodels:
        model_results_phase(scenario_dict=scenario_args)

    if args.output:
        outputs_only(dated_output_folder=True, timestamp=timestamp)

    if args.half_model:
        half_model_run(scenario_dict=scenario_args, dated_output_folder=True, timestamp=timestamp)

    if args.data_import:
        data_import_refresh()

    if args.preprocessing:
        data_preprocessing_phase(scenario_dict=scenario_args)

    if args.data_refresh:
        data_import_and_preprocessing_refresh(scenario_dict=scenario_args)

    if args.results:
        model_results_phase(scenario_dict=scenario_args)

    if args.business_cases:
        standardise_business_cases(serialize=True)
        #business_case_tests(new_folder=True, timestamp=timestamp, create_test_df=True)

    if args.variable_costs:
        generate_variable_plant_summary(scenario_dict=scenario_args, serialize=True)

    if args.results_and_output:
        results_and_output(scenario_dict=scenario_args, dated_output_folder=True, timestamp=timestamp)

    if args.graphs:
        graphs_only(timestamp=timestamp, dated_output_folder=True)

    if args.minimodels:
        generate_minimodels(scenario_dict=scenario_args)

    if args.production:
        production_flow(scenario_dict=scenario_args)

    if args.cos:
        cos_flow(scenario_dict=scenario_args)

    if args.metaresults:
        global_metaresults_flow(scenario_dict=scenario_args)

    if args.investment:
        investment_flow(scenario_dict=scenario_args)

    if args.ta:
        tco_switch_reference(scenario_dict=scenario_args)

    if args.tco:
        tco_switch_reference(scenario_dict=scenario_args)

    if args.abatement:
        abatement_switch_reference(scenario_dict=scenario_args)

    if args.emissivity:
        abatement_switch_reference(scenario_dict=scenario_args)

    TIME_CONTAINER.return_time_container()
