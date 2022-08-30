"""Model flow functions for the main script"""

import math
from typing import Any, Dict, List, MutableMapping, Union
from datetime import datetime
from mppsteel.multi_run_module.iteration_runs import (
    combine_files_iteration_run,
    generate_scenario_iterations_reference,
    make_scenario_iterations
)

from mppsteel.utility.utils import (
    split_list_into_chunks,
    stdout_query,
    get_currency_rate
)
from mppsteel.utility.file_handling_utility import (
    generate_files_to_path_dict,
    pickle_to_csv,
    create_folder_if_nonexist,
    get_scenario_pkl_path,
    create_folders_if_nonexistant,
    return_pkl_paths,
)

from mppsteel.utility.log_utility import get_logger

from mppsteel.data_load_and_format.data_import import load_import_data
from mppsteel.data_load_and_format.reg_steel_demand_formatter import get_steel_demand
from mppsteel.data_preprocessing.timeseries_generator import generate_timeseries
from mppsteel.data_load_and_format.pe_model_formatter import format_pe_data
from mppsteel.data_load_and_format.steel_plant_formatter import steel_plant_processor
from mppsteel.data_load_and_format.data_interface import (
    create_capex_opex_dict,
    create_business_case_reference,
    generate_preprocessed_emissions_data,
)
from mppsteel.data_preprocessing.emissions_reference_tables import (
    generate_emissions_flow,
)
from mppsteel.data_preprocessing.capex_switching import create_capex_timeseries
from mppsteel.data_preprocessing.investment_cycles import investment_cycle_flow
from mppsteel.data_preprocessing.variable_plant_cost_archetypes import (
    generate_variable_plant_summary,
)
from mppsteel.data_preprocessing.carbon_tax_reference import (
    generate_carbon_tax_reference,
)
from mppsteel.data_preprocessing.total_opex_reference import (
    generate_total_opex_cost_reference,
)
from mppsteel.data_preprocessing.levelized_cost import generate_levelized_cost_results
from mppsteel.model_solver.solver_flow import main_solver_flow
from mppsteel.data_preprocessing.tco_abatement_switch import (
    tco_presolver_reference,
    abatement_presolver_reference,
)
from mppsteel.model_results.production import production_results_flow
from mppsteel.model_results.cost_of_steelmaking import (
    generate_cost_of_steelmaking_results,
)
from mppsteel.model_results.global_metaresults import metaresults_flow
from mppsteel.model_results.investments import investment_results
from mppsteel.model_results.green_capacity_ratio import generate_gcr_df
from mppsteel.model_graphs.graph_production import (
    create_graphs,
)

from mppsteel.config.model_config import (
    DATETIME_FORMAT,
    PKL_DATA_FORMATTED,
    USD_TO_EUR_CONVERSION_DEFAULT,
    OUTPUT_FOLDER
)
from mppsteel.config.model_scenarios import SCENARIO_OPTIONS, SCENARIO_SETTINGS
from mppsteel.config.reference_lists import INTERMEDIATE_RESULT_PKL_FILES, FINAL_RESULT_PKL_FILES, SCENARIO_SETTINGS_TO_ITERATE
from mppsteel.config.mypy_config_settings import (
    MYPY_PKL_PATH_OPTIONAL, 
    MYPY_SCENARIO_ENTRY_TYPE, 
    MYPY_SCENARIO_SETTINGS_SEQUENCE, 
    MYPY_SCENARIO_TYPE
)
from mppsteel.utility.file_handling_utility import (
    create_folder_if_nonexist,
    create_scenario_paths,
)

from mppsteel.multi_run_module.multiprocessing_functions import (
    multiprocessing_scenarios_preprocessing,
    multiprocessing_scenarios_single_run
)
from mppsteel.multi_run_module.multiple_runs import (
    aggregate_multi_run_scenarios, 
    join_scenario_data, 
    make_multiple_model_runs, 
    multiprocessing_scenarios_multiple_scenarios_multiple_runs
)

logger = get_logger(__name__)

# SCENARIO SET-UP
def stdout_question(
    count_iter: int, scenario_type: str, scenario_options: MYPY_SCENARIO_SETTINGS_SEQUENCE, default_dict: MYPY_SCENARIO_TYPE
) -> str:
    return f"""
    Scenario Option {count_iter+1}/{len(scenario_options)}: {scenario_type}
    Default value: {default_dict[scenario_type]}.
    To keep default, leave blank and press ENTER, else enter a different value from the options presented.
    ---> Options {scenario_options[scenario_type]}
    """


def get_inputted_scenarios(scenario_options: MYPY_SCENARIO_SETTINGS_SEQUENCE, default_scenario: MYPY_SCENARIO_TYPE) -> Dict[str, MYPY_SCENARIO_ENTRY_TYPE]:
    inputted_scenario_args = {}
    for count, scenario_option in enumerate(scenario_options.keys()):
        question = stdout_question(count, scenario_option, scenario_options, default_scenario)
        inputted_scenario_args[scenario_option] = stdout_query(
            question, default_scenario[scenario_option], scenario_options[scenario_option]
        )
    return inputted_scenario_args


def add_currency_rates_to_scenarios(scenario_dict: MYPY_SCENARIO_TYPE, live: bool = False) -> MYPY_SCENARIO_TYPE:
    eur_to_usd = 1 / USD_TO_EUR_CONVERSION_DEFAULT
    usd_to_eur = USD_TO_EUR_CONVERSION_DEFAULT
    if live:
        eur_to_usd = get_currency_rate("eur", "usd")
        usd_to_eur = get_currency_rate("usd", "eur")

    scenario_dict["eur_to_usd"] = eur_to_usd
    scenario_dict["usd_to_eur"] = usd_to_eur

    return scenario_dict

# MULTI-RUN / MULTI-SCENARIO
def data_preprocessing_scenarios(
    scenario_dict: MYPY_SCENARIO_TYPE, pkl_paths: MYPY_PKL_PATH_OPTIONAL = None
) -> None:
    get_steel_demand(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_timeseries(
        scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True
    )
    format_pe_data(
        scenario_dict=scenario_dict,
        pkl_paths=pkl_paths,
        serialize=True,
        standardize_units=True,
    )
    generate_emissions_flow(
        scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True
    )
    generate_variable_plant_summary(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_carbon_tax_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_total_opex_cost_reference(
        scenario_dict, pkl_paths=pkl_paths, serialize=True
    )
    generate_levelized_cost_results(
        scenario_dict=scenario_dict,
        pkl_paths=pkl_paths,
        serialize=True,
        standard_plant_ref=True,
    )
    tco_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    abatement_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)

def scenario_batch_run(
    scenario_dict: MYPY_SCENARIO_TYPE,
    dated_output_folder: bool = False,
    iteration_run: bool = False,
    include_outputs: bool = True,
) -> None:
    scenario_name = str(scenario_dict["scenario_name"])
    # create new folders for path
    intermediate_path = get_scenario_pkl_path(
        scenario=scenario_name,
        pkl_folder_type="intermediate",
        iteration_run=iteration_run,
    )
    final_path = get_scenario_pkl_path(
        scenario=scenario_name, pkl_folder_type="final", iteration_run=iteration_run
    )
    pkl_paths = {"intermediate_path": intermediate_path, "final_path": final_path}
    create_folders_if_nonexistant([intermediate_path, final_path])
    # Set up scenario and metadata
    scenario_args = scenario_dict
    scenario_args = add_currency_rates_to_scenarios(scenario_args)
    timestamp = datetime.now().strftime(DATETIME_FORMAT)
    model_output_folder = f"{scenario_name} {timestamp}"

    # Model run
    scenario_model_run(
        scenario_dict=scenario_args,
        pkl_paths=pkl_paths,
        new_folder=dated_output_folder,
        output_folder=model_output_folder,
        include_outputs=include_outputs,
    )


def scenario_model_run(
    scenario_dict: MYPY_SCENARIO_TYPE,
    new_folder: bool,
    output_folder: str,
    include_outputs: bool = True,
    pkl_paths: MYPY_PKL_PATH_OPTIONAL = None,
) -> None:
    data_preprocessing_scenarios(scenario_dict, pkl_paths=pkl_paths)
    main_solver_flow(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    model_results_phase(scenario_dict, pkl_paths=pkl_paths)
    if include_outputs:
        model_outputs_phase(
            scenario_dict,
            pkl_paths=pkl_paths,
            new_folder=new_folder,
            output_folder=output_folder,
        )
        model_graphs_phase(
            scenario_dict,
            pkl_paths=pkl_paths,
            new_folder=new_folder,
            output_folder=output_folder,
        )


# PHASING / GROUPING
def data_preprocessing_generic_1() -> None:
    create_capex_opex_dict(serialize=True)
    create_capex_timeseries(serialize=True)
    create_business_case_reference(serialize=True)


def data_preprocessing_generic_2(scenario_dict):
    steel_plant_processor(scenario_dict=scenario_dict, serialize=True)
    investment_cycle_flow(scenario_dict=scenario_dict, serialize=True)
    generate_preprocessed_emissions_data(serialize=True)


def data_preprocessing_refresh(
    scenario_dict: MYPY_SCENARIO_TYPE, pkl_paths: MYPY_PKL_PATH_OPTIONAL = None
) -> None:
    data_preprocessing_generic_1()
    data_preprocessing_generic_2(scenario_dict)
    data_preprocessing_scenarios(scenario_dict, pkl_paths=pkl_paths)


def data_import_and_preprocessing_refresh(
    scenario_dict: MYPY_SCENARIO_TYPE
) -> None:
    load_import_data(serialize=True)
    data_preprocessing_generic_1()
    data_preprocessing_generic_2(scenario_dict)


def generic_data_preprocessing_only(scenario_dict: MYPY_SCENARIO_TYPE) -> None:
    data_preprocessing_generic_1()
    data_preprocessing_generic_2(scenario_dict)


def total_opex_calculations(
    scenario_dict: MYPY_SCENARIO_TYPE, pkl_paths: MYPY_PKL_PATH_OPTIONAL = None
) -> None:
    generate_variable_plant_summary(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_carbon_tax_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_total_opex_cost_reference(
        scenario_dict, pkl_paths=pkl_paths, serialize=True
    )
    generate_levelized_cost_results(
        scenario_dict=scenario_dict,
        pkl_paths=pkl_paths,
        serialize=True,
        standard_plant_ref=True,
    )
    tco_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def model_presolver(scenario_dict: MYPY_SCENARIO_TYPE, pkl_paths: MYPY_PKL_PATH_OPTIONAL = None) -> None:
    tco_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    abatement_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def model_results_phase(
    scenario_dict: MYPY_SCENARIO_TYPE, pkl_paths: MYPY_PKL_PATH_OPTIONAL = None, model_run: str = ""
) -> None:
    production_results_flow(
        scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run
    )
    investment_results(
        scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run
    )
    metaresults_flow(
        scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run
    )
    generate_cost_of_steelmaking_results(
        scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run
    )
    generate_gcr_df(
        scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run
    )


def model_outputs_phase(
    scenario_dict: MYPY_SCENARIO_TYPE,
    pkl_paths: MYPY_PKL_PATH_OPTIONAL = None,
    new_folder: bool = False,
    output_folder: str = "",
) -> None:
    scenario_name = str(scenario_dict["scenario_name"])
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f"{OUTPUT_FOLDER}/{output_folder}"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath

    # Save Intermediate Pickle Files
    _, intermediate_path, final_path = return_pkl_paths(
        scenario_name=scenario_name, paths=pkl_paths
    )
    pickle_to_csv(save_path, PKL_DATA_FORMATTED, "capex_switching_df", reset_index=True)
    # Save Final Pickle Files
    for pkl_file in INTERMEDIATE_RESULT_PKL_FILES:
        pickle_to_csv(save_path, intermediate_path, pkl_file)

    for pkl_file in FINAL_RESULT_PKL_FILES:
        pickle_to_csv(save_path, final_path, pkl_file)


def model_graphs_phase(
    scenario_dict: MYPY_SCENARIO_TYPE,
    pkl_paths: MYPY_PKL_PATH_OPTIONAL = None,
    new_folder: bool = False,
    output_folder: str = "",
) -> None:
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f"{OUTPUT_FOLDER}/{output_folder}/graphs"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    create_graphs(filepath=save_path, scenario_dict=scenario_dict, pkl_paths=pkl_paths)


def results_and_output(
    scenario_dict: MYPY_SCENARIO_TYPE,
    new_folder: bool,
    output_folder: str,
    pkl_paths: MYPY_PKL_PATH_OPTIONAL = None
) -> None:
    model_results_phase(scenario_dict, pkl_paths=pkl_paths)
    model_outputs_phase(
        scenario_dict,
        pkl_paths=pkl_paths,
        new_folder=new_folder,
        output_folder=output_folder,
    )
    model_graphs_phase(
        scenario_dict,
        pkl_paths=pkl_paths,
        new_folder=new_folder,
        output_folder=output_folder,
    )


def outputs_only(
    scenario_dict: MYPY_SCENARIO_TYPE,
    new_folder: bool,
    output_folder: str,
    pkl_paths: MYPY_PKL_PATH_OPTIONAL = None
) -> None:
    model_outputs_phase(
        scenario_dict,
        pkl_paths=pkl_paths,
        new_folder=new_folder,
        output_folder=output_folder,
    )
    model_graphs_phase(
        scenario_dict,
        pkl_paths=pkl_paths,
        new_folder=new_folder,
        output_folder=output_folder,
    )


def graphs_only(
    scenario_dict: MYPY_SCENARIO_TYPE,
    output_folder: str,
    new_folder: bool,
    pkl_paths: MYPY_PKL_PATH_OPTIONAL = None
) -> None:
    model_graphs_phase(
        scenario_dict,
        pkl_paths=pkl_paths,
        new_folder=new_folder,
        output_folder=output_folder,
    )

# FULL FLOW FUNCTIONS
def full_flow(
    scenario_dict: MYPY_SCENARIO_TYPE,
    new_folder: bool,
    output_folder: str,
    pkl_paths: MYPY_PKL_PATH_OPTIONAL = None
) -> None:
    data_import_and_preprocessing_refresh(scenario_dict)
    scenario_model_run(
        scenario_dict,
        pkl_paths=pkl_paths,
        new_folder=new_folder,
        output_folder=output_folder,
    )


def half_model_run(
    scenario_dict: MYPY_SCENARIO_TYPE, output_folder: str
) -> None:
    main_solver_flow(scenario_dict=scenario_dict, serialize=True)
    results_and_output(
        scenario_dict=scenario_dict,
        new_folder=True,
        output_folder=output_folder,
    )


def core_run_function(
    scenario_dict: MutableMapping[str, Any], model_run: str, pkl_paths: Union[dict, None] = None
) -> None:
    """Function to run the section of the model that runs multiple times.

    Args:
        scenario_dict (MutableMapping): The scenario to run.
        model_run (str): The number of the scenario run.
        pkl_paths (Union[dict, None], optional): The path where the multiple runs will be stored. Defaults to None.
    """
    # Create folders where the multiple runs will be saved.
    generate_files_to_path_dict(
        scenarios=[
            scenario_dict["scenario_name"],
        ],
        pkl_paths=pkl_paths,
        model_run=model_run,
        create_path=True,
    )
    main_solver_flow(
        scenario_dict=scenario_dict,
        pkl_paths=pkl_paths,
        serialize=True,
        model_run=model_run,
    )
    model_results_phase(scenario_dict, pkl_paths=pkl_paths, model_run=model_run)


# MULTI RUN / MULTI SCENARIO FUNCTIONS
def full_multiple_run_flow(
    scenario_name: str, main_scenario_runs: List[str], timestamp: str
) -> None:
    for scenario_name in main_scenario_runs:
        create_scenario_paths(scenario_name)
    data_import_and_preprocessing_refresh(SCENARIO_OPTIONS[main_scenario_runs[0]])
    scenario_options_main_scenario = [
        SCENARIO_OPTIONS[scenario_name] for scenario_name in main_scenario_runs
    ]
    multiprocessing_scenarios_single_run(
        scenario_options=scenario_options_main_scenario,
        func=scenario_batch_run,
        dated_output_folder=True,
        iteration_run=False,
        include_outputs=True,
    )
    join_scenario_data(
        scenario_options=main_scenario_runs,
        timestamp=timestamp,
        final_outputs_only=True,
    )


def multi_run_full_function(
    scenario_dict: MYPY_SCENARIO_TYPE, number_of_runs: int, timestamp: str
) -> None:
    scenario_name_multi_run_full = str(scenario_dict["scenario_name"])
    logger.info(
        f"Running model in full {number_of_runs} times for {scenario_name_multi_run_full} scenario"
    )
    data_import_and_preprocessing_refresh(scenario_dict=scenario_dict)
    data_preprocessing_scenarios(scenario_dict=scenario_dict)
    make_multiple_model_runs(
        core_run_function,
        scenario_dict=scenario_dict,
        number_of_runs=number_of_runs,
        remove_run_folders=False,
    )
    aggregate_multi_run_scenarios(
        scenario_options={scenario_name_multi_run_full: scenario_dict},
        single_scenario=True,
        new_folder=True,
        timestamp=timestamp,
    )


def multi_run_half_function(
    scenario_dict: MYPY_SCENARIO_TYPE, number_of_runs: int, timestamp: str
) -> None:
    scenario_name_multi_run_half = str(scenario_dict["scenario_name"])
    logger.info(
        f"Running half-model {number_of_runs} times for {scenario_name_multi_run_half} scenario"
    )
    make_multiple_model_runs(
        core_run_function,
        scenario_dict=scenario_dict,
        number_of_runs=number_of_runs,
        remove_run_folders=True,
    )
    aggregate_multi_run_scenarios(
        scenario_options={scenario_name_multi_run_half: scenario_dict},
        single_scenario=True,
        new_folder=True,
        timestamp=timestamp,
    )


def multi_run_multi_scenario(
    main_scenario_runs: List[str], number_of_runs: int, timestamp: str
) -> None:
    logger.info(
        f"Running the model {number_of_runs} times for {len(main_scenario_runs)} scenarios"
    )
    scenario_options: Dict[str, MYPY_SCENARIO_TYPE] = {
        scenario: add_currency_rates_to_scenarios(SCENARIO_OPTIONS[scenario])
        for scenario in main_scenario_runs
    }
    for scenario_name in main_scenario_runs:
        create_scenario_paths(scenario_name)
    data_import_and_preprocessing_refresh(scenario_options["baseline"])
    multiprocessing_scenarios_preprocessing(
        scenario_options=scenario_options,
        preprocessing_function=data_preprocessing_scenarios,
    )
    multiprocessing_scenarios_multiple_scenarios_multiple_runs(
        core_run_function,
        scenario_options=scenario_options,
        number_of_runs=number_of_runs,
        remove_run_folders=True,
    )
    aggregate_multi_run_scenarios(
        scenario_options=scenario_options,
        single_scenario=False,
        new_folder=True,
        timestamp=timestamp,
    )


def full_model_iteration_run(
    batch_iteration_scenarios: list, files_to_aggregate: list, scenario_setting_to_iterate: list, timestamp: str
) -> None:
    logger.info(f"Running model iterations for {batch_iteration_scenarios}")
    output_iteration_path = f"{OUTPUT_FOLDER}/iteration_run {timestamp}"
    create_folders_if_nonexistant(
        [
            output_iteration_path,
        ]
    )
    scenario_iteration_reference = generate_scenario_iterations_reference(
        batch_iteration_scenarios, SCENARIO_OPTIONS, SCENARIO_SETTINGS, scenario_setting_to_iterate
    )
    scenario_iteration_reference.to_csv(
        f"{output_iteration_path}/scenario_iteration_reference.csv", index=False
    )
    for base_scenario in batch_iteration_scenarios:
        create_scenario_paths(base_scenario)
        scenario_list = make_scenario_iterations(
            SCENARIO_OPTIONS[base_scenario], SCENARIO_SETTINGS, scenario_setting_to_iterate
        )
        logger.info(f"Running {len(scenario_list)} iterations for {base_scenario}")
        data_import_and_preprocessing_refresh(scenario_list[0])
        chunks = math.ceil(len(scenario_list) / len(batch_iteration_scenarios))
        scenario_list_chunks = split_list_into_chunks(scenario_list, chunks)
        for scenario_list_chunk in scenario_list_chunks:
            multiprocessing_scenarios_single_run(
                scenario_options=scenario_list_chunk,
                func=scenario_batch_run,
                dated_output_folder=True,
                iteration_run=True,
                include_outputs=False,
            )
    combine_files_iteration_run(
        scenarios_to_iterate=batch_iteration_scenarios,
        filenames=files_to_aggregate,
        output_path=output_iteration_path,
        serialize=True,
    )
