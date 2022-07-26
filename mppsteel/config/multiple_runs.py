"""Script to manage making multiple model_runs"""

import multiprocessing as mp
from typing import Callable

import pandas as pd

from mppsteel.config.model_scenarios import SCENARIO_OPTIONS

from mppsteel.config.model_config import (
    COMBINED_OUTPUT_FOLDER_NAME,
    DEFAULT_NUMBER_OF_RUNS,
    FINAL_RESULT_PKL_FILES,
    INTERMEDIATE_RESULT_PKL_FILES,
    MULTIPLE_MODEL_RUN_EVALUATION_YEARS,
    MULTIPLE_RUN_SCENARIO_FOLDER_NAME,
    OUTPUT_FOLDER,
    PKL_DATA_COMBINED,
    PKL_FOLDER,
)

from mppsteel.model_graphs.graph_production import create_combined_scenario_graphs
from mppsteel.model_results.multiple_model_run_summary import create_emissions_summary_stack, create_production_summary_stack
from mppsteel.model_results.resource_demand_summary import create_resource_demand_summary
from mppsteel.model_solver.solver_flow import main_solver_flow
from mppsteel.config.model_grouping import add_currency_rates_to_scenarios, model_results_phase

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    create_folder_if_nonexist,
    pickle_to_csv,
    read_pickle_folder,
    serialize_file,
    get_scenario_pkl_path,
)
from mppsteel.utility.log_utility import get_logger
# Create logger
logger = get_logger(__name__)

def create_pool(scenario_options: list):
    virtual_cores = len(scenario_options)
    n_cores = mp.cpu_count()
    logger.info(f"{n_cores} cores detected, creating {virtual_cores} virtual cores")
    return mp.Pool(processes=virtual_cores)


def multiprocessing_scenarios_single_run(scenario_options: list, func: Callable) -> None:
    pool = create_pool(scenario_options)

    for scenario in scenario_options:
        pool_async = pool.apply_async(func, args=(scenario, True))
        pool_async.get()
    pool.close()
    pool.join()


def multiprocessing_scenarios_preprocessing(
    scenario_options: dict,
    preprocessing_function: Callable
) -> None:
    # POOL 1: PREPROCESSING
    pool = create_pool(scenario_options.keys())
    for scenario in scenario_options:
        pool_async = pool.apply_async(
            preprocessing_function, 
            kwds=dict(scenario_dict=scenario_options[scenario])
        )
        pool_async.get()
    pool.close()
    pool.join()

def multiprocessing_scenarios_solver_flow(
    scenario_options: dict,
    repeating_function: Callable,
    timestamp: str,
    number_of_runs: int,
    files_to_aggregate: list
) -> None:

    pool = create_pool(scenario_options.keys())
    for scenario in scenario_options:
        pool_async = pool.apply_async(
            repeating_function, 
            kwds=dict(
                scenario_dict=scenario_options[scenario],
                files_to_aggregate=files_to_aggregate,
                dated_output_folder=True,
                timestamp=timestamp,
                number_of_runs=number_of_runs,
                aggregate_only=True
            )
        )
        pool_async.get()
    pool.close()
    pool.join()


@timer_func
def join_scenario_data(
    scenario_options: list,
    new_folder: bool = True,
    timestamp: str = "",
    final_outputs_only: bool = True,
):
    logger.info(f"Joining the Following Scenario Data {scenario_options}")
    combined_output_pkl_folder = f"{PKL_FOLDER}/{COMBINED_OUTPUT_FOLDER_NAME}"
    create_folder_if_nonexist(combined_output_pkl_folder)
    output_save_path = OUTPUT_FOLDER
    output_folder_graphs = f"{output_save_path}/graphs"
    output_folder_name = f"{COMBINED_OUTPUT_FOLDER_NAME} {timestamp}"
    output_folder_filepath = "/"
    if new_folder:
        output_folder_filepath = f"{OUTPUT_FOLDER}/{output_folder_name}"
        output_folder_graphs = f"{output_folder_filepath}/graphs"
        create_folder_if_nonexist(output_folder_filepath)
        create_folder_if_nonexist(output_folder_graphs)
        output_save_path = output_folder_filepath
        output_save_path_graphs = output_folder_graphs

    if not final_outputs_only:
        for output_file in INTERMEDIATE_RESULT_PKL_FILES:
            output_container = []
            for scenario_name in scenario_options:
                path = get_scenario_pkl_path(scenario_name, "intermediate")
                output_container.append(read_pickle_folder(path, output_file, "df"))

            combined_output = pd.concat(output_container).reset_index(drop=True)
            serialize_file(combined_output, combined_output_pkl_folder, output_file)
            combined_output.to_csv(f"{output_save_path}/{output_file}.csv", index=False)

    for output_file in FINAL_RESULT_PKL_FILES:
        output_container = []
        for scenario_name in scenario_options:
            path = get_scenario_pkl_path(scenario_name, "final")
            output_container.append(read_pickle_folder(path, output_file, "df"))

        combined_output = pd.concat(output_container).reset_index(drop=True)
        serialize_file(combined_output, combined_output_pkl_folder, output_file)
        combined_output.to_csv(f"{output_save_path}/{output_file}.csv", index=False)

    resource_demand_summary = create_resource_demand_summary(
        output_folder_path=PKL_DATA_COMBINED,
        serialize=True
    )
    resource_demand_summary.to_csv(f"{output_save_path}/resource_demand_summary.csv", index=False)

    create_combined_scenario_graphs(filepath=output_save_path_graphs)

def add_model_run_metadata_columns(
    df: pd.DataFrame, order_of_run: int, total_runs: int) -> pd.DataFrame:
    df_c = df.copy()
    df_c["order_of_run"] = order_of_run
    df_c["total_runs"] = total_runs
    return df_c


def store_result_to_container(
    run_container: dict, 
    filename: str, 
    pkl_path: str, 
    model_run: int, 
    number_of_runs: int
) -> dict:
    df = read_pickle_folder(pkl_path, filename, "df")
    df = add_model_run_metadata_columns(df, model_run, number_of_runs)
    run_container[filename].append(df)
    return run_container


def store_run_container_to_pkl(
    run_container: dict,
    pkl_path: str,
) -> None:
    for filename in run_container:
        df = pd.concat(run_container[filename]).reset_index(drop=True)
        serialize_file(df, pkl_path, filename)

def folder_filepath_creation(
    scenario_name: str,
    dated_output_folder: bool = True,
    timestamp: str = "",
) -> str:

    # Create Folders
    logger.info(f"Creating Folders for {scenario_name}")
    pkl_output_folder = f"{PKL_FOLDER}/{MULTIPLE_RUN_SCENARIO_FOLDER_NAME}/{scenario_name}"
    create_folder_if_nonexist(pkl_output_folder)
    output_save_path = OUTPUT_FOLDER
    output_folder_name = f"{MULTIPLE_RUN_SCENARIO_FOLDER_NAME} {scenario_name} {timestamp}"
    output_folder_filepath = "/"
    if dated_output_folder:
        output_folder_filepath = f"{OUTPUT_FOLDER}/{output_folder_name}"
        create_folder_if_nonexist(output_folder_filepath)
        output_save_path = output_folder_filepath

    return output_save_path, pkl_output_folder

def multi_model_run(
    scenario_dict: dict, files_to_path_dict: dict, pkl_output_folder: str, number_of_runs: int
) -> None:

    scenario_name = scenario_dict["scenario_name"]
    logger.info(f"Generating the scenario data for {scenario_name}")

    run_container = {filename: [] for filename in files_to_path_dict}

    for model_run in range(1, number_of_runs + 1):
        main_solver_flow(scenario_dict=scenario_dict, serialize=True)
        model_results_phase(scenario_dict)
        for filename in files_to_path_dict:
            store_result_to_container(run_container, filename, files_to_path_dict[filename], model_run, number_of_runs)

    store_run_container_to_pkl(run_container, pkl_output_folder)

def aggregate_files(df_dict: dict) -> pd.DataFrame:
    # AGGREGATE MODEL RUNS
    logger.info("Producing combined run summary DataFrame")
    production_resource_usage = df_dict["production_resource_usage"]
    production_emissions = df_dict["production_emissions"]
    # CREATE SUMMARY DATAFRAMES
    emissions_summary = create_emissions_summary_stack(
        production_emissions, years=MULTIPLE_MODEL_RUN_EVALUATION_YEARS)
    production_summary = create_production_summary_stack(
        production_resource_usage, material_unit="mt", energy_unit="gj", years=MULTIPLE_MODEL_RUN_EVALUATION_YEARS)
    return pd.concat([emissions_summary, production_summary]).reset_index(drop=True)


def write_files(
    combined_summary_df: pd.DataFrame, files_to_aggregate: list, 
    pkl_output_folder: str, output_save_path: str, summary_csv_filename: str
):
    logger.info("Writing results to file")
    # WRITE FILES TO PKL
    serialize_file(combined_summary_df, pkl_output_folder, summary_csv_filename)

    # WRITE FILES TO CSV
    for filename in files_to_aggregate:
        pickle_to_csv(output_save_path, pkl_output_folder, filename)
    combined_summary_df.to_csv(f"{output_save_path}/{summary_csv_filename}.csv", index=False)


def make_multiple_model_runs(
    scenario_dict: dict,
    files_to_aggregate: list,
    dated_output_folder: bool = True,
    timestamp: str = "",
    number_of_runs: int = DEFAULT_NUMBER_OF_RUNS,
    aggregate_only: bool = False
) -> None:

    scenario_name = scenario_dict["scenario_name"]
    logger.info("Running model")
    output_save_path, pkl_output_folder = folder_filepath_creation(
        scenario_name, dated_output_folder, timestamp
    )
    final_path = get_scenario_pkl_path(scenario_name, "final")
    intermediate_path = get_scenario_pkl_path(scenario_name, "intermediate")
    files_to_path = {
        "production_resource_usage": final_path,
        "production_emissions": final_path,
        "full_trade_summary": intermediate_path
    }

    multi_model_run(scenario_dict, files_to_path, pkl_output_folder, number_of_runs)
    if not aggregate_only:
        df_dict = {filename: read_pickle_folder(pkl_output_folder, filename, "df") for filename in files_to_aggregate}
        combined_summary = aggregate_files(df_dict)

        write_files(
            combined_summary, 
            files_to_aggregate, 
            pkl_output_folder, 
            output_save_path, 
            "multi_run_summary"
        )
