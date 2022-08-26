"""Runs the data loading scripts"""

from datetime import datetime
from distributed import Client

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import (
    create_folders_if_nonexistant,
)
from mppsteel.utility.function_timer_utility import TIME_CONTAINER
from mppsteel.config.model_config import (
    DATETIME_FORMAT,
    DEFAULT_NUMBER_OF_RUNS,
    FOLDERS_TO_CHECK_IN_ORDER,
)
from mppsteel.config.model_scenarios import (
    BATCH_ITERATION_SCENARIOS,
    MAIN_SCENARIO_RUNS,
    DEFAULT_SCENARIO,
    SCENARIO_SETTINGS,
    SCENARIO_OPTIONS,
    ABATEMENT_SCENARIO,
    BAU_SCENARIO,
    CARBON_COST,
    TECH_MORATORIUM,
)
from mppsteel.config.runtime_args import parser
from mppsteel.config.model_grouping import *
from mppsteel.config.reference_lists import ITERATION_FILES_TO_AGGREGATE

from mppsteel.config.model_grouping import data_import_and_preprocessing_refresh, scenario_batch_run
from mppsteel.multi_run_module.multiprocessing_functions import multiprocessing_scenarios_single_run

logger = get_logger(__name__)

if __name__ == "__main__":

    args = parser.parse_args()

    # INITAL SCENARIO ARGUMENTS
    scenario_args: MYPY_SCENARIO_TYPE = DEFAULT_SCENARIO
    number_of_runs = DEFAULT_NUMBER_OF_RUNS
    if args.number_of_runs:
        number_of_runs = int(args.number_of_runs)

    if (
        args.multi_run_half
        or args.multi_run_full
        or args.multi_run_multi_scenario
        or args.model_iterations_run
    ):
        client = Client()  # manages localhost app for modin operations

    if args.choose_scenario:
        if args.choose_scenario in SCENARIO_OPTIONS.keys():
            logger.info(f"CORRECT SCENARIO CHOSEN: {args.choose_scenario}")
            scenario_args = SCENARIO_OPTIONS[args.choose_scenario]
        else:
            scenario_name_options = list(SCENARIO_OPTIONS.keys())
            logger.info(
                f"INVALID SCENARIO INPUT: {args.choose_scenario}, please choose from {scenario_name_options}"
            )

    if args.custom_scenario:
        logger.info("Including custom parameter inputs.")
        scenario_args = get_inputted_scenarios(
            scenario_options=SCENARIO_SETTINGS, default_scenario=scenario_args
        )

    # SCENARIO CUSTOMIZATION
    scenario_args = add_currency_rates_to_scenarios(scenario_args)
    scenario_name = str(scenario_args["scenario_name"])

    timestamp = datetime.now().strftime(DATETIME_FORMAT)
    logger.info(f"Model running at {timestamp}")
    model_output_folder = f"{scenario_args['scenario_name']} {timestamp}"

    intermediate_path = get_scenario_pkl_path(
        scenario_name, "intermediate"
    )
    final_path = get_scenario_pkl_path(scenario_name, "final")
    create_folders_if_nonexistant(FOLDERS_TO_CHECK_IN_ORDER)
    create_folders_if_nonexistant([intermediate_path, final_path])

    # SCENARIO FLOWS
    if args.main_scenarios:
        logger.info(f"Running {MAIN_SCENARIO_RUNS} scenario options")
        full_multiple_run_flow(scenario_name, MAIN_SCENARIO_RUNS, timestamp)

    if args.multi_run_multi_scenario:
        multi_run_multi_scenario(MAIN_SCENARIO_RUNS, number_of_runs, timestamp)

    if args.model_iterations_run:
        full_model_iteration_run(
            batch_iteration_scenarios=BATCH_ITERATION_SCENARIOS,
            files_to_aggregate=ITERATION_FILES_TO_AGGREGATE,
            timestamp=timestamp,
        )

    logger.info(
        f"""Running model with the following parameters:  
    {scenario_args}"""
    )

    if args.full_model:
        full_flow(
            scenario_dict=scenario_args,
            new_folder=True,
            output_folder=model_output_folder,
            pkl_paths=None
        )

    if args.multi_run_full:
        multi_run_full_function(
            scenario_dict=scenario_args, 
            number_of_runs=number_of_runs,
            timestamp=timestamp
        )

    if args.multi_run_half:
        multi_run_half_function(
            scenario_dict=scenario_args, 
            number_of_runs=number_of_runs,
            timestamp=timestamp
        )

    if args.solver:
        main_solver_flow(scenario_dict=scenario_args, serialize=True)

    if args.output:
        outputs_only(
            scenario_dict=scenario_args,
            new_folder=True,
            output_folder=model_output_folder,
        )

    if args.scenario_model_run:
        scenario_model_run(
            scenario_dict=scenario_args,
            pkl_paths=None,
            new_folder=True,
            output_folder=model_output_folder,
        )

    if args.half_model_run:
        half_model_run(
            scenario_dict=scenario_args,
            output_folder=model_output_folder
        )

    if args.data_import:
        load_import_data(serialize=True)

    if args.preprocessing:
        data_preprocessing_scenarios(scenario_dict=scenario_args)

    if args.presolver:
        model_presolver(scenario_dict=scenario_args)

    if args.results:
        model_results_phase(scenario_dict=scenario_args)

    if args.generic_preprocessing:
        generic_data_preprocessing_only(scenario_dict=scenario_args)

    if args.variable_costs:
        generate_variable_plant_summary(
            scenario_dict=scenario_args, serialize=True
        )

    if args.levelized_cost:
        generate_levelized_cost_results(
            scenario_dict=scenario_args,
            pkl_paths=None,
            serialize=True,
            standard_plant_ref=True,
        )

    if args.results_and_output:
        results_and_output(
            scenario_dict=scenario_args,
            new_folder=True,
            output_folder=model_output_folder,
        )

    if args.graphs:
        graphs_only(
            scenario_dict=scenario_args,
            new_folder=True,
            output_folder=model_output_folder,
        )

    if args.total_opex:
        total_opex_calculations(scenario_dict=scenario_args)

    if args.production:
        production_results_flow(scenario_args, pkl_paths=None, serialize=True)

    if args.cos:
        generate_cost_of_steelmaking_results(
            scenario_args, pkl_paths=None, serialize=True
        )

    if args.metaresults:
        metaresults_flow(scenario_args, pkl_paths=None, serialize=True)

    if args.investment:
        investment_results(scenario_dict=scenario_args)

    if args.join_final_data:
        join_scenario_data(
            scenario_options=MAIN_SCENARIO_RUNS,
            new_folder=True,
            timestamp=timestamp,
            final_outputs_only=True,
        )

    if args.tco:
        tco_presolver_reference(scenario_args, pkl_paths=None, serialize=True)

    if args.abatement:
        abatement_presolver_reference(scenario_args, pkl_paths=None, serialize=True)

    if args.emissivity:
        generate_emissions_flow(scenario_dict=scenario_args, serialize=True)

    if args.investment_cycles:
        investment_cycle_flow(scenario_dict=scenario_args, serialize=True)

    if args.pe_models:
        format_pe_data(scenario_dict=scenario_args)

    if args.steel_plants:
        steel_plant_processor(scenario_dict=scenario_args)

    time_container = TIME_CONTAINER.return_time_container()
    logger.info(time_container)
