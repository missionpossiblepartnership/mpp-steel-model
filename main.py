"""Runs the data loading scripts"""

import math
from datetime import datetime
from distributed import Client
from mppsteel.multi_run_module.multiple_runs import (
    aggregate_multi_run_scenarios,
    join_scenario_data, 
    make_multiple_model_runs,
    multiprocessing_scenarios_multiple_scenarios_multiple_runs
)
from mppsteel.multi_run_module.multiprocessing_functions import (
    multiprocessing_scenarios_single_run,
    multiprocessing_scenarios_preprocessing
)
from mppsteel.multi_run_module.iteration_runs import (
    generate_scenario_iterations_reference,
    make_scenario_iterations,
    combine_files_iteration_run
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import (
    create_folders_if_nonexistant, create_scenario_paths
)
from mppsteel.utility.function_timer_utility import TIME_CONTAINER
from mppsteel.config.model_config import (
    DATETIME_FORMAT, DEFAULT_NUMBER_OF_RUNS, FOLDERS_TO_CHECK_IN_ORDER
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
from mppsteel.config.reference_lists import PKL_FILE_RESULTS_REFERENCE
from mppsteel.utility.utils import split_list_into_chunks

logger = get_logger(__name__)

if __name__ == "__main__":

    args = parser.parse_args()

    # INITAL SCENARIO ARGUMENTS
    scenario_args = DEFAULT_SCENARIO
    number_of_runs = DEFAULT_NUMBER_OF_RUNS
    if args.number_of_runs:
        number_of_runs = int(args.number_of_runs)

    if args.multi_run_half or args.multi_run_full or args.multi_run_multi_scenario or args.model_iterations_run:
        client = Client() # manages localhost app for modin operations

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

    timestamp = datetime.now().strftime(DATETIME_FORMAT)
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
        for scenario_name in MAIN_SCENARIO_RUNS:
            create_scenario_paths(scenario_name)
        data_import_and_preprocessing_refresh(SCENARIO_OPTIONS[MAIN_SCENARIO_RUNS[0]])
        scenario_options = [SCENARIO_OPTIONS[scenario_name] for scenario_name in MAIN_SCENARIO_RUNS]
        multiprocessing_scenarios_single_run(
            scenario_options=scenario_options, 
            func=scenario_batch_run, 
            dated_output_folder=True, 
            iteration_run=False, 
            include_outputs=True,
        )
        join_scenario_data(
            scenario_options=MAIN_SCENARIO_RUNS,
            timestamp=timestamp,
            final_outputs_only=True,
        )

    if args.multi_run_multi_scenario:
        logger.info(f"Running the model {number_of_runs} times for {len(MAIN_SCENARIO_RUNS)} scenarios")
        scenario_options = {scenario: add_currency_rates_to_scenarios(SCENARIO_OPTIONS[scenario]) for scenario in MAIN_SCENARIO_RUNS}
        """
        for scenario_name in MAIN_SCENARIO_RUNS:
            create_scenario_paths(scenario_name)
        data_import_and_preprocessing_refresh(scenario_options["baseline"])
        multiprocessing_scenarios_preprocessing(
            scenario_options=scenario_options, 
            preprocessing_function=scenario_preprocessing_phase,
        )
        multiprocessing_scenarios_multiple_scenarios_multiple_runs(
            scenario_options=scenario_options,
            number_of_runs=number_of_runs,
            remove_run_folders=True,
        )
        """
        aggregate_multi_run_scenarios(
            scenario_options=scenario_options,
            single_scenario=False,
            dated_output_folder=True,
            timestamp=timestamp
        )

    if args.model_iterations_run:
        logger.info(f"Running model iterations for {BATCH_ITERATION_SCENARIOS}")

        output_iteration_path = f"{OUTPUT_FOLDER}/iteration_run {timestamp}"
        create_folders_if_nonexistant([output_iteration_path,])
        """
        scenario_iteration_reference = generate_scenario_iterations_reference(
            BATCH_ITERATION_SCENARIOS, SCENARIO_OPTIONS, SCENARIO_SETTINGS
        )
        scenario_iteration_reference.to_csv(
            f"{output_iteration_path}/scenario_iteration_reference.csv", index=False
        )
        
        for base_scenario in BATCH_ITERATION_SCENARIOS:
            create_scenario_paths(base_scenario)
            scenario_list = make_scenario_iterations(SCENARIO_OPTIONS[base_scenario], SCENARIO_SETTINGS)
            logger.info(f"Running {len(scenario_list)} iterations for {base_scenario}")
            data_import_and_preprocessing_refresh(scenario_list[0])
            chunks = math.ceil(len(scenario_list) / len(BATCH_ITERATION_SCENARIOS))
            scenario_list_chunks = split_list_into_chunks(scenario_list, chunks)
            for scenario_list_chunk in scenario_list_chunks:
                multiprocessing_scenarios_single_run(
                    scenario_options=scenario_list_chunk,
                    func=scenario_batch_run,
                    dated_output_folder=True,
                    iteration_run=True,
                    include_outputs=False
                )
        """
        files_to_aggregate = [
            "production_resource_usage", "production_emissions", 
            "investment_results", "cost_of_steelmaking", 
            "calculated_emissivity_combined", "tco_summary_data"
        ]
        combine_files_iteration_run(
            scenarios_to_iterate=BATCH_ITERATION_SCENARIOS,
            filenames=files_to_aggregate,
            output_path=output_iteration_path,
            to_feather=True
        )

    logger.info(
        f"""Running model with the following parameters:  
    {scenario_args}"""
    )

    if args.full_model:
        full_flow(
            scenario_dict=scenario_args,
            pkl_paths=None,
            dated_output_folder=True,
            model_output_folder=model_output_folder,
        )

    if args.multi_run_full:
        scenario_name = scenario_args['scenario_name']
        logger.info(f"Running model in full {number_of_runs} times for {scenario_name} scenario")
        data_import_and_preprocessing_refresh(scenario_dict=scenario_args)
        scenario_preprocessing_phase(scenario_dict=scenario_args)
        make_multiple_model_runs(
            scenario_dict=scenario_args,
            number_of_runs=number_of_runs,
            remove_run_folders=False
        )
        aggregate_multi_run_scenarios(
            scenario_options={scenario_name: scenario_args},
            single_scenario=True,
            dated_output_folder=True,
            timestamp=timestamp
        )

    if args.multi_run_half:
        scenario_name=scenario_args['scenario_name']
        scenario_options={scenario_name: scenario_args}
        logger.info(f"Running half-model {number_of_runs} times for {scenario_name} scenario")
        make_multiple_model_runs(
            scenario_dict=scenario_args,
            number_of_runs=number_of_runs,
            remove_run_folders=True
        )
        aggregate_multi_run_scenarios(
            scenario_options={scenario_name: scenario_args},
            single_scenario=True,
            dated_output_folder=True,
            timestamp=timestamp
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
            pkl_paths=None,
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
