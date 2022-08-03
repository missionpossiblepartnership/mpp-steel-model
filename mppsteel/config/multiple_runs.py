"""Script to manage making multiple model_runs"""

import itertools
import multiprocessing as mp
import shutil
from typing import Callable, List, Union

import pandas as pd
import modin.pandas as mpd
from tqdm import tqdm

from mppsteel.config.model_config import (
    COMBINED_OUTPUT_FOLDER_NAME,
    DEFAULT_NUMBER_OF_RUNS,
    FINAL_RESULT_PKL_FILES,
    INTERMEDIATE_RESULT_PKL_FILES,
    MULTIPLE_RUN_SCENARIO_FOLDER_NAME,
    OUTPUT_FOLDER,
    PKL_DATA_COMBINED,
    PKL_FOLDER,
)

from mppsteel.model_results.multiple_model_run_summary import summarise_combined_data
from mppsteel.model_graphs.graph_production import create_combined_scenario_graphs
from mppsteel.model_results.resource_demand_summary import create_resource_demand_summary
from mppsteel.model_solver.solver_flow import main_solver_flow
from mppsteel.config.model_grouping import model_results_phase

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    create_folder_if_nonexist,
    create_folders_if_nonexistant,
    pickle_to_csv,
    read_pickle_folder,
    return_pkl_paths,
    serialize_file,
    get_scenario_pkl_path
)

from mppsteel.utility.log_utility import get_logger
# Create logger
logger = get_logger(__name__)

def create_pool(processes_to_run: list):
    virtual_cores = len(processes_to_run)
    n_cores = mp.cpu_count()
    logger.info(f"{n_cores} cores detected, creating {virtual_cores} virtual cores")
    return mp.Pool(processes=virtual_cores)


def multiprocessing_scenarios_single_run(
    scenario_options: List[dict], func: Callable, dated_output_folder: bool = False, 
    iteration_run: bool = False, include_outputs: bool = True
) -> None:
    pool = create_pool(scenario_options)
    workers = [
        pool.apply_async(
            func, 
            args=(scenario_dict, dated_output_folder, iteration_run, include_outputs)
        ) for scenario_dict in scenario_options
    ]
    for async_pool in workers:
        async_pool.get()
    pool.close()
    pool.join()


def multiprocessing_scenarios_preprocessing(
    scenario_options: dict,
    preprocessing_function: Callable
) -> None:
    # POOL 1: PREPROCESSING
    pool = create_pool(scenario_options.keys())
    workers = [
        pool.apply_async(
            preprocessing_function, 
            kwds=dict(scenario_dict=scenario_options[scenario])
        ) for scenario in scenario_options
    ]
    for async_pool in workers:
        async_pool.get()
    pool.close()
    pool.join()

def multiprocessing_scenarios_multiple_scenarios_multiple_runs(
    repeating_function: Callable, scenario_options: list,  
    files_to_path: dict, number_of_runs: int, remove_run_folders: bool = False
):
    pool = create_pool(scenario_options.keys())
    workers = [
        pool.apply_async(
            repeating_function, 
            kwds=dict(
                scenario_dict=scenario_options[scenario],
                files_to_path=files_to_path,
                number_of_runs=number_of_runs,
                remove_run_folders=remove_run_folders
            )   
        ) for scenario in scenario_options
    ]
    for async_pool in workers:
        async_pool.get()
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
    df: pd.DataFrame, scenario_name: str, order_of_run: int, total_runs: int) -> pd.DataFrame:
    df_c = df.copy()
    if "scenario" not in df_c.columns:
        df_c["scenario"] = scenario_name
    df_c["order_of_run"] = order_of_run
    df_c["total_runs"] = total_runs
    return df_c


def store_result_to_container(
    run_container: dict, 
    scenario_name: str,
    filename: str, 
    pkl_path: str,
    model_run: int, 
    number_of_runs: int
) -> dict:
    df = read_pickle_folder(pkl_path, filename, "df")
    df = add_model_run_metadata_columns(df, scenario_name, model_run, number_of_runs)
    run_container[filename].append(df)


def store_run_container_to_pkl(
    run_container: dict,
    pkl_path: str,
) -> None:
    for filename in run_container:
        df = mpd.concat(run_container[filename]).reset_index(drop=True)
        serialize_file(df._to_pandas(), pkl_path, filename)

def pkl_folder_filepath_creation(scenario_name: str, create_folder: bool = False) -> str:
    pkl_output_folder = f"{PKL_FOLDER}/{MULTIPLE_RUN_SCENARIO_FOLDER_NAME}/{scenario_name}"
    if create_folder:
        create_folder_if_nonexist(pkl_output_folder)
    return pkl_output_folder

def output_folder_path_creation(
    dated_output_folder: bool = True,
    timestamp: str = "",
    single_scenario: str = ""
):
    output_save_path = OUTPUT_FOLDER
    output_folder_name = f"{MULTIPLE_RUN_SCENARIO_FOLDER_NAME} {single_scenario} {timestamp}" if single_scenario else f"{MULTIPLE_RUN_SCENARIO_FOLDER_NAME} {timestamp}"
    output_folder_filepath = "/"
    if dated_output_folder:
        output_folder_filepath = f"{OUTPUT_FOLDER}/{output_folder_name}"
        create_folder_if_nonexist(output_folder_filepath)
        output_save_path = output_folder_filepath
    return output_save_path

def core_run_function(scenario_dict: dict, model_run: int, pkl_paths: Union[dict, None] = None):
    generate_files_to_path_dict([scenario_dict["scenario_name"],], pkl_paths, model_run, create_path=True)
    main_solver_flow(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=str(model_run))
    model_results_phase(scenario_dict, pkl_paths=pkl_paths, model_run=str(model_run))

def make_multiple_model_runs(
    scenario_dict: dict, number_of_runs: int = DEFAULT_NUMBER_OF_RUNS, remove_run_folders: bool = False
) -> None:

    scenario_name = scenario_dict["scenario_name"]
    logger.info(f"Generating the scenario data for {scenario_name}")
    pkl_output_folder = pkl_folder_filepath_creation(scenario_name, create_folder=True)
    run_range = range(1, number_of_runs + 1)

    pool = create_pool(run_range)
    workers = [
        pool.apply_async(
            core_run_function, 
            kwds=dict(
                scenario_dict=scenario_dict,
                model_run=model_run,
                pkl_paths=None,
            )
        ) for model_run in run_range
    ]
    for async_pool in workers:
        async_pool.get()
    pool.close()
    pool.join()

    run_container = aggregate_results(
        scenario_name, run_range, number_of_runs, remove_run_folders=remove_run_folders
    )

    store_run_container_to_pkl(run_container, pkl_output_folder)

def aggregate_results(scenario_name: str, run_range: range, number_of_runs: int, remove_run_folders: bool = False):
    generic_files_to_path_dict = generate_files_to_path_dict([scenario_name,])[scenario_name]
    run_container = {filename: [] for filename in generic_files_to_path_dict}
    for model_run in run_range:
        model_run_files_to_path = generate_files_to_path_dict(scenarios=[scenario_name,], model_run=model_run)
        for filename in run_container:
            store_result_to_container(
                run_container=run_container, 
                scenario_name=scenario_name, 
                filename=filename, 
                pkl_path=model_run_files_to_path[scenario_name][filename], 
                model_run=model_run, 
                number_of_runs=number_of_runs
            )

    if remove_run_folders:
        for model_run in run_range:
            intermediate_path = get_scenario_pkl_path(
                scenario=scenario_name, pkl_folder_type="intermediate", model_run=model_run
            )
            final_path = get_scenario_pkl_path(
                scenario=scenario_name, pkl_folder_type="final", model_run=model_run
            )
            shutil.rmtree(intermediate_path)
            shutil.rmtree(final_path)
    
    return run_container


def generate_files_to_path_dict(scenarios: list, pkl_paths: Union[dict, None] = None, model_run: str = "", create_path: bool = False):
    files_to_path = {scenario: {} for scenario in scenarios}
    for scenario_name in scenarios:
        intermediate_path_preprocessing, intermediate_path, final_path = return_pkl_paths(scenario_name, pkl_paths, model_run)
        if create_path:
            create_folders_if_nonexistant([intermediate_path, final_path])
        files_to_path[scenario_name] = {
            "production_resource_usage": final_path,
            "production_emissions": final_path,
            "investment_results": final_path,
            "cost_of_steelmaking": final_path,
            "full_trade_summary": intermediate_path,
            "plant_result_df": intermediate_path,
            "levelized_cost_standardized": intermediate_path_preprocessing,
            "calculated_emissivity_combined": intermediate_path_preprocessing
        }

    return files_to_path

def append_to_list(appending_list, filename: str, scenario: str):
    appending_list.append(read_pickle_folder(pkl_folder_filepath_creation(scenario), filename, "df"))

def create_process_function_kwargs_scenario_agg(appending_list, filename, scenario):
    return dict(
        appending_list=appending_list,
        filename=filename,
        scenario=scenario,
    )

def manager_run(
    process_function: Callable, process_kwargs_function: Callable, 
    process_iterable_dict: dict, container: dict, filename: str = "", 
):
    with mp.Manager() as manager:
        L = manager.list()
        processes = []
        for iter in process_iterable_dict:
            p = mp.Process(
                target=process_function, 
                kwargs=process_kwargs_function(L, filename, iter),   
            )
            p.start()
            processes.append(p)
        
        for p in processes:
            p.join()
        if filename:
            container[filename] = list(L)

    for object in [L, p, processes, manager]:
        del object

def return_single_scenario(scenario_options: dict):
    return list(scenario_options.keys())[0]

def aggregate_multi_run_scenarios(
    scenario_options: dict,
    single_scenario: bool = False,
    dated_output_folder: bool = True,
    timestamp: str = "",
):
    combined_pkl_path = pkl_folder_filepath_creation("combined", create_folder=True)
    output_save_path = output_folder_path_creation(dated_output_folder, timestamp)
    if single_scenario:
        output_save_path = output_folder_path_creation(dated_output_folder, timestamp, return_single_scenario(scenario_options))
    else:
        output_save_path = output_folder_path_creation(dated_output_folder, timestamp)
    files_to_aggregate =  list(generate_files_to_path_dict(scenario_options.keys())[return_single_scenario(scenario_options)].keys())

    agg_dict = {filename: [] for filename in files_to_aggregate}

    for filename in tqdm(files_to_aggregate, total=len(files_to_aggregate), desc="Running pkl scenario data merge"):
        manager_run(
            process_function=append_to_list,
            process_kwargs_function=create_process_function_kwargs_scenario_agg,
            process_iterable_dict=scenario_options,
            container=agg_dict,
            filename=filename,
        )

    for filename in files_to_aggregate:
        agg_dict[filename] = mpd.concat(agg_dict[filename]).reset_index(drop=True)

    summarised_data_dict = summarise_combined_data(agg_dict)

    for filename in summarised_data_dict:
        serialize_file(summarised_data_dict[filename], combined_pkl_path, filename)
        pickle_to_csv(output_save_path, combined_pkl_path, filename, reset_index=False)


def make_scenario_iterations(base_scenario: dict, scenario_settings: dict):
    scenario_list = []
    product_iteration = list(itertools.product(
            scenario_settings["carbon_tax_scenario"], 
            scenario_settings["hydrogen_cost_scenario"], 
            scenario_settings["electricity_cost_scenario"], 
            scenario_settings["steel_demand_scenario"], 
            scenario_settings["grid_scenario"]
        )
    )

    for iteration, (
        carbon_cost_scenario, hydrogen_cost_scenario, electricity_cost_scenario, 
        steel_demand_scenario, grid_scenario) in enumerate(product_iteration
    ):
        new_scenario_dict = dict(base_scenario)
        new_scenario_dict["scenario_name"] = f"{base_scenario['scenario_name']}_{iteration + 1}"
        new_scenario_dict["carbon_tax_scenario"] = carbon_cost_scenario
        new_scenario_dict["electricity_cost_scenario"] = electricity_cost_scenario
        new_scenario_dict["hydrogen_cost_scenario"] = hydrogen_cost_scenario
        new_scenario_dict["steel_demand_scenario"] = steel_demand_scenario
        new_scenario_dict["grid_scenario"] = grid_scenario
        scenario_list.append(new_scenario_dict)
    return scenario_list

def create_scenario_paths(scenario_name: str):
    intermediate_path = get_scenario_pkl_path(
        scenario_name, "intermediate"
    )
    final_path = get_scenario_pkl_path(
        scenario_name, "final"
    )
    create_folders_if_nonexistant([intermediate_path, final_path])
