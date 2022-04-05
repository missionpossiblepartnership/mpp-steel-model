"""Runs the data loading scripts"""
from datetime import datetime

from mppsteel.utility.utils import multiprocessing_scenarios
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import (
    create_folders_if_nonexistant
)
from mppsteel.utility.function_timer_utility import TIME_CONTAINER

from mppsteel.config.model_config import FOLDERS_TO_CHECK_IN_ORDER

from mppsteel.config.model_scenarios import (
    DEFAULT_SCENARIO, SCENARIO_SETTINGS, SCENARIO_OPTIONS,
    ABATEMENT_SCENARIO, BAU_SCENARIO, CARBON_COST, TECH_MORATORIUM
)

from mppsteel.config.model_grouping import *

logger = get_logger(__name__)

if __name__ == "__main__":

    args = parser.parse_args()

    scenario_args = ABATEMENT_SCENARIO
    scenario_args = add_currency_rates_to_scenarios(scenario_args)

    timestamp = datetime.today().strftime('%d-%m-%y %H-%M')
    logger.info(f'Model running at {timestamp}')
    model_output_folder = f"{scenario_args['scenario_name']} {timestamp}"

    create_folders_if_nonexistant(FOLDERS_TO_CHECK_IN_ORDER)

    if args.custom_scenario:
        logger.info('Including custom parameter inputs.')
        scenario_args = get_inputted_scenarios(scenario_options=SCENARIO_SETTINGS, default_scenario=scenario_args)

    if args.choose_scenario:
        if args.choose_scenario in SCENARIO_OPTIONS.keys():
            logger.info(f'CORRECT SCENARIO CHOSEN: {args.choose_scenario}')
            scenario_args = SCENARIO_OPTIONS[args.choose_scenario]
        else:
            scenario_options = list(SCENARIO_OPTIONS.keys())
            logger.info(f'INVALID SCENARIO INPUT: {args.choose_scenario}, please choose from {scenario_options}')

    if args.main_scenarios:
        
        options = ['baseline', 'tech_moratorium', 'abatement', 'carbon_cost']
        logger.info(f'Running {options} scenario options')

        # Multiprocessing
        multiprocessing_scenarios(options, scenario_batch_run)

    logger.info(f'''Running model with the following parameters:  
    {scenario_args}''')

    if args.full_model:
        full_flow(scenario_dict=scenario_args, dated_output_folder=True, model_output_folder=model_output_folder)

    if args.solver:
        solver_flow(scenario_dict=scenario_args, year_end=MODEL_YEAR_END, serialize=True)

    if args.minimodels:
        model_results_phase(scenario_dict=scenario_args)

    if args.output:
        outputs_only(scenario_dict=scenario_args, dated_output_folder=True, model_output_folder=model_output_folder)

    if args.half_model:
        half_model_run(scenario_dict=scenario_args, dated_output_folder=True, model_output_folder=model_output_folder)

    if args.data_import:
        data_import_refresh(scenario_dict=scenario_args)

    if args.preprocessing:
        data_preprocessing_scenarios(scenario_dict=scenario_args)

    if args.data_refresh:
        data_import_and_preprocessing_refresh(scenario_dict=scenario_args)

    if args.results:
        model_results_phase(scenario_dict=scenario_args)

    if args.generic_preprocessing:
        data_preprocessing_generic(scenario_dict=scenario_args)

    if args.variable_costs:
        generate_variable_plant_summary(scenario_dict=scenario_args, serialize=True)

    if args.levelized_cost:
        lcost_flow(scenario_dict=scenario_args)

    if args.results_and_output:
        results_and_output(scenario_dict=scenario_args, dated_output_folder=True, model_output_folder=model_output_folder)

    if args.graphs:
        graphs_only(scenario_dict=scenario_args, model_output_folder=model_output_folder, dated_output_folder=True)

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
        model_presolver(scenario_dict=scenario_args)

    if args.tco:
        tco_switch_reference(scenario_dict=scenario_args)

    if args.abatement:
        abatement_switch_reference(scenario_dict=scenario_args)

    if args.emissivity:
        generate_emissions_flow(scenario_dict=scenario_args, serialize=True)

    if args.green_replacement_ratio:
        gcr_flow(scenario_dict=scenario_args)

    TIME_CONTAINER.return_time_container()
