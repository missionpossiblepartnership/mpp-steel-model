"""Runs the data loading scripts"""

from datetime import datetime
from mppsteel.config.multiple_runs import (
    join_scenario_data, 
    make_multiple_model_runs,
    multiprocessing_scenarios_single_run,
    multiprocessing_scenarios_preprocessing,
    multiprocessing_scenarios_solver_flow
)

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import create_folders_if_nonexistant
from mppsteel.utility.function_timer_utility import TIME_CONTAINER

from mppsteel.config.model_config import DATETIME_FORMAT, DEFAULT_NUMBER_OF_RUNS, FOLDERS_TO_CHECK_IN_ORDER

from mppsteel.config.model_scenarios import (
    MAIN_SCENARIO_RUNS,
    DEFAULT_SCENARIO,
    SCENARIO_SETTINGS,
    SCENARIO_OPTIONS,
    ABATEMENT_SCENARIO,
    BAU_SCENARIO,
    CARBON_COST,
    TECH_MORATORIUM,
)

from mppsteel.config.model_grouping import *

logger = get_logger(__name__)

if __name__ == "__main__":

    args = parser.parse_args()

    # INITAL SCENARIO ARGUMENTS
    scenario_args = DEFAULT_SCENARIO
    number_of_runs = DEFAULT_NUMBER_OF_RUNS
    if args.number_of_runs:
        number_of_runs = int(args.number_of_runs)

    if args.choose_scenario:
        if args.choose_scenario in SCENARIO_OPTIONS.keys():
            logger.info(f"CORRECT SCENARIO CHOSEN: {args.choose_scenario}")
            scenario_args = SCENARIO_OPTIONS[args.choose_scenario]
        else:
            scenario_options = list(SCENARIO_OPTIONS.keys())
            logger.info(
                f"INVALID SCENARIO INPUT: {args.choose_scenario}, please choose from {scenario_options}"
            )

    if args.custom_scenario:
        logger.info("Including custom parameter inputs.")
        scenario_args = get_inputted_scenarios(
            scenario_options=SCENARIO_SETTINGS, default_scenario=scenario_args
        )

    # SCENARIO CUSTOMIZATION
    scenario_args = add_currency_rates_to_scenarios(scenario_args)

    timestamp = datetime.today().strftime(DATETIME_FORMAT)
    logger.info(f"Model running at {timestamp}")
    model_output_folder = f"{scenario_args['scenario_name']} {timestamp}"

    intermediate_path = get_scenario_pkl_path(
        scenario_args["scenario_name"], "intermediate"
    )
    final_path = get_scenario_pkl_path(scenario_args["scenario_name"], "final")
    create_folders_if_nonexistant(FOLDERS_TO_CHECK_IN_ORDER)
    create_folders_if_nonexistant([intermediate_path, final_path])

    # SCENARIO Flows
    if args.main_scenarios:

        logger.info(f"Running {MAIN_SCENARIO_RUNS} scenario options")
        data_import_and_preprocessing_refresh()
        multiprocessing_scenarios_single_run(
            scenario_options=MAIN_SCENARIO_RUNS, func=scenario_batch_run
        )
        join_scenario_data(
            scenario_options=MAIN_SCENARIO_RUNS,
            timestamp=timestamp,
            final_outputs_only=True,
        )

    logger.info(
        f"""Running model with the following parameters:  
    {scenario_args}"""
    )

    if args.full_model:
        full_flow(
            scenario_dict=scenario_args,
            dated_output_folder=True,
            model_output_folder=model_output_folder,
        )

    if args.multi_run_full:
        logger.info(f"Running model in full {number_of_runs} times for {scenario_args['scenario_name']} scenario")
        data_import_and_preprocessing_refresh(scenario_dict=scenario_args)
        scenario_preprocessing_phase(scenario_dict=scenario_args)
        make_multiple_model_runs(
            scenario_dict=scenario_args,
            files_to_aggregate=["production_resource_usage", "production_emissions"],
            dated_output_folder=True,
            timestamp=timestamp,
            number_of_runs=number_of_runs,
            aggregate_only=False
        )

    if args.multi_run_half:
        logger.info(f"Running half-model {number_of_runs} times for {scenario_args['scenario_name']} scenario")
        make_multiple_model_runs(
            scenario_dict=scenario_args,
            files_to_aggregate=["production_resource_usage", "production_emissions"],
            dated_output_folder=True,
            timestamp=timestamp,
            number_of_runs=number_of_runs,
            aggregate_only=False
        )

    if args.multi_run_multi_scenario:
        logger.info(f"Running the model {number_of_runs} times for {len(MAIN_SCENARIO_RUNS)} scenarios")
        scenario_options = {scenario: add_currency_rates_to_scenarios(SCENARIO_OPTIONS[scenario]) for scenario in MAIN_SCENARIO_RUNS}
        data_import_and_preprocessing_refresh(scenario_options["baseline"])
        multiprocessing_scenarios_preprocessing(
            scenario_options, scenario_preprocessing_phase
        )
        multiprocessing_scenarios_solver_flow(
            scenario_options=scenario_options,
            repeating_function=make_multiple_model_runs,
            timestamp=timestamp,
            number_of_runs=number_of_runs,
            files_to_aggregate=["production_resource_usage", "production_emissions", "full_trade_summary"]
        )

    if args.solver:
        main_solver_flow(scenario_dict=scenario_args, serialize=True)

    if args.output:
        outputs_only(
            scenario_dict=scenario_args,
            dated_output_folder=True,
            model_output_folder=model_output_folder,
        )

    if args.scenario_model_run:
        scenario_model_run(
            scenario_dict=scenario_args,
            dated_output_folder=True,
            model_output_folder=model_output_folder,
        )

    if args.half_model_run:
        main_solver_flow(scenario_dict=scenario_args, serialize=True)
        results_and_output(
            scenario_dict=scenario_args,
            dated_output_folder=True,
            model_output_folder=model_output_folder,
        )

    if args.data_import:
        data_import_refresh()

    if args.preprocessing:
        data_preprocessing_scenarios(scenario_dict=scenario_args)

    if args.presolver:
        model_presolver(scenario_dict=scenario_args)

    if args.results:
        model_results_phase(scenario_dict=scenario_args)

    if args.generic_preprocessing:
        data_preprocessing_generic_1()
        data_preprocessing_generic_2(scenario_dict=scenario_args)

    if args.variable_costs:
        generate_variable_plant_summary(scenario_dict=scenario_args, serialize=True)

    if args.levelized_cost:
        lcost_flow(scenario_dict=scenario_args)

    if args.results_and_output:
        results_and_output(
            scenario_dict=scenario_args,
            dated_output_folder=True,
            model_output_folder=model_output_folder,
        )

    if args.graphs:
        graphs_only(
            scenario_dict=scenario_args,
            model_output_folder=model_output_folder,
            dated_output_folder=True,
        )

    if args.total_opex:
        total_opex_calculations(scenario_dict=scenario_args)

    if args.production:
        production_flow(scenario_dict=scenario_args)

    if args.cos:
        cos_flow(scenario_dict=scenario_args)

    if args.metaresults:
        global_metaresults_flow(scenario_dict=scenario_args)

    if args.investment:
        investment_flow(scenario_dict=scenario_args)

    if args.join_final_data:
        join_scenario_data(
            scenario_options=MAIN_SCENARIO_RUNS,
            new_folder=True,
            timestamp=timestamp,
            final_outputs_only=True,
        )

    if args.tco:
        tco_switch_reference(scenario_dict=scenario_args)

    if args.abatement:
        abatement_switch_reference(scenario_dict=scenario_args)

    if args.emissivity:
        generate_emissions_flow(scenario_dict=scenario_args, serialize=True)

    if args.investment_cycles:
        investment_cycles()

    if args.pe_models:
        format_pe_data(scenario_dict=scenario_args)

    if args.steel_plants:
        steel_plant_processor(scenario_dict=scenario_args)

    TIME_CONTAINER.return_time_container()
