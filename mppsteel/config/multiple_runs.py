"""Script to manage making multiple model_runs"""

import itertools
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
from mppsteel.config.model_grouping import model_results_phase

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

def create_pool(processes_to_run: list):
    virtual_cores = len(processes_to_run)
    n_cores = mp.cpu_count()
    logger.info(f"{n_cores} cores detected, creating {virtual_cores} virtual cores")
    return mp.Pool(processes=virtual_cores)


def multiprocessing_scenarios_single_run(scenario_options: list, func: Callable) -> None:
    pool = create_pool(scenario_options)
    workers = [pool.apply_async(func, args=(scenario, True)) for scenario in scenario_options]
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
    files_to_path: dict, number_of_runs: int, 
):
    pool = create_pool(scenario_options.keys())
    workers = [
        pool.apply_async(
            repeating_function, 
            kwds=dict(
                scenario_dict=scenario_options[scenario],
                files_to_path=files_to_path,
                number_of_runs=number_of_runs,
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
    filename: str, 
    scenario_name: str,
    pkl_path: str, 
    model_run: int, 
    number_of_runs: int
) -> dict:
    df = read_pickle_folder(pkl_path, filename, "df")
    df = add_model_run_metadata_columns(df, scenario_name, model_run, number_of_runs)
    run_container[filename].append(df)
    return run_container


def store_run_container_to_pkl(
    run_container: dict,
    pkl_path: str,
) -> None:
    for filename in run_container:
        df = pd.concat(run_container[filename]).reset_index(drop=True)
        serialize_file(df, pkl_path, filename)

def pkl_folder_filepath_creation(scenario_name: str) -> str:
    # Create Folders
    logger.info(f"Creating Folders for {scenario_name}")
    pkl_output_folder = f"{PKL_FOLDER}/{MULTIPLE_RUN_SCENARIO_FOLDER_NAME}/{scenario_name}"
    create_folder_if_nonexist(pkl_output_folder)
    return pkl_output_folder

def output_folder_path_creation(
    dated_output_folder: bool = True,
    timestamp: str = "",
):
    output_save_path = OUTPUT_FOLDER
    output_folder_name = f"{MULTIPLE_RUN_SCENARIO_FOLDER_NAME} {timestamp}"
    output_folder_filepath = "/"
    if dated_output_folder:
        output_folder_filepath = f"{OUTPUT_FOLDER}/{output_folder_name}"
        create_folder_if_nonexist(output_folder_filepath)
        output_save_path = output_folder_filepath
    return output_save_path

def core_run_function(scenario_dict: dict, files_to_path_dict: dict, run_container: dict, model_run: int, number_of_runs: int):
        main_solver_flow(scenario_dict=scenario_dict, serialize=True)
        model_results_phase(scenario_dict)
        for filename in files_to_path_dict:
            store_result_to_container(run_container, filename, scenario_dict["scenario_name"], files_to_path_dict[filename], model_run, number_of_runs)

def multi_model_run(
    scenario_dict: dict, files_to_path_dict: dict, pkl_output_folder: str, number_of_runs: int
) -> None:

    scenario_name = scenario_dict["scenario_name"]
    logger.info(f"Generating the scenario data for {scenario_name}")

    run_container = {filename: [] for filename in files_to_path_dict}
    run_range = range(1, number_of_runs + 1)
    for model_run in run_range:
        core_run_function(
            scenario_dict=scenario_dict,
            files_to_path_dict=files_to_path_dict,
            run_container=run_container,
            model_run=model_run,
            number_of_runs=number_of_runs
        )
    store_run_container_to_pkl(run_container, pkl_output_folder)

def generate_files_to_path_dict(scenarios: list):
    files_to_path = {scenario: {} for scenario in scenarios}
    for scenario in scenarios:
        final_path = get_scenario_pkl_path(scenario, "final")
        intermediate_path = get_scenario_pkl_path(scenario, "intermediate")
        files_to_path[scenario] = {
            "production_resource_usage": final_path,
            "production_emissions": final_path,
            "investment_results": final_path,
            "cost_of_steelmaking": final_path,
            "full_trade_summary": intermediate_path,
            "levelized_cost_standardized": intermediate_path,
            "calculated_emissivity_combined": intermediate_path,
            "plant_result_df": intermediate_path
        }
    return files_to_path


def consumption_summary(df, grouping_cols: list, unit_list: list):
    total_runs = df["total_runs"].unique()[0]
    resource_cols = [col for col in df.columns if any(ext in col for ext in unit_list)]
    combined_cols = grouping_cols + resource_cols
    df_c = df[combined_cols]
    grouped_df = df_c.groupby(
        by=grouping_cols,
    ).sum()
    grouped_df = grouped_df / total_runs
    return grouped_df.reset_index()

def generic_summary(df: pd.DataFrame, grouping_cols: list, value_cols: list, agg_dict: dict, include_plant_count: bool = False):
    total_runs = df["total_runs"].unique()[0]
    combined_cols = grouping_cols + value_cols
    df_c = df[combined_cols]
    if include_plant_count:
        df_c.loc[:,"number_of_plants"] = 0
        agg_dict["number_of_plants"] = "size"
    grouped_df = df_c.groupby(by=grouping_cols).agg(agg_dict)
    grouped_df = grouped_df / total_runs
    return grouped_df.reset_index()

def summarise_combined_data(data_dict: dict) -> dict:

    production_emissions_summary = consumption_summary(
        df=data_dict["production_emissions"], 
        grouping_cols=["scenario", "year", "region_rmi", "technology"], 
        unit_list=["_gt", "_mt"]
    )

    production_resource_usage_summary = consumption_summary(
        df=data_dict["production_resource_usage"], 
        grouping_cols=["scenario", "year", "region_rmi", "technology"], 
        unit_list=["_gt", "_mt", "_gj", "_pj"]
    )

    plant_capacity_summary = generic_summary(
        df=data_dict["production_resource_usage"],
        grouping_cols=["scenario", "year", "region_rmi", "technology"],
        value_cols=["capacity"],
        agg_dict={"capacity": "sum"},
        include_plant_count=True
    )

    cost_of_steelmaking_summary = generic_summary(
        df=data_dict["cost_of_steelmaking"],
        grouping_cols=["scenario", "year", "region_rmi"],
        value_cols=["cost_of_steelmaking"],
        agg_dict={"cost_of_steelmaking": "sum"}
    )

    investment_results_summary = generic_summary(
        df=data_dict["investment_results"],
        grouping_cols=["scenario", "year", "region_rmi", "switch_type", "start_tech", "end_tech"],
        value_cols=["capital_cost"],
        agg_dict={"capital_cost": "sum"},
        include_plant_count=True
    )

    levelized_cost_standardized_summary = generic_summary(
        df=data_dict["levelized_cost_standardized"],
        grouping_cols=["scenario", "year", "region", "country_code", "technology"],
        value_cols=["levelized_cost"],
        agg_dict={"levelized_cost": "sum"}
    )

    calc_em_value_cols = ["s1_emissivity", "s2_emissivity", "s3_emissivity", "combined_emissivity"]
    calc_em_agg_dict = {val_col: "sum" for val_col in calc_em_value_cols}
    calculated_emissivity_combined_summary = generic_summary(
        df=data_dict["calculated_emissivity_combined"],
        grouping_cols=["scenario", "year", "region", "country_code", "technology"],
        value_cols=["s1_emissivity", "s2_emissivity", "s3_emissivity", "combined_emissivity"],
        agg_dict=calc_em_agg_dict,
    )

    return {
        "production_emissions_summary": production_emissions_summary,
        "production_resource_usage_summary": production_resource_usage_summary,
        "plant_capacity_summary": plant_capacity_summary,
        "cost_of_steelmaking_summary": cost_of_steelmaking_summary,
        "investment_results_summary": investment_results_summary,
        "levelized_cost_standardized_summary": levelized_cost_standardized_summary,
        "calculated_emissivity_combined_summary": calculated_emissivity_combined_summary,
        "full_trade_summary": data_dict["full_trade_summary"],
        "plant_result_df": data_dict["plant_result_df"]
    }


def make_multiple_model_runs(
    scenario_dict: dict,
    files_to_path: dict,
    number_of_runs: int = DEFAULT_NUMBER_OF_RUNS,
) -> None:

    scenario_name = scenario_dict["scenario_name"]
    logger.info("Running model")
    pkl_output_folder = pkl_folder_filepath_creation(scenario_name)

    multi_model_run(scenario_dict, files_to_path[scenario_name], pkl_output_folder, number_of_runs)

def aggregate_multi_run_scenarios(
    scenario_options: dict,
    files_to_path: dict,
    dated_output_folder: bool = True,
    timestamp: str = "",
):
    combined_pkl_path = pkl_folder_filepath_creation("combined")
    output_save_path = output_folder_path_creation(dated_output_folder, timestamp)
    files_to_aggregate = list(files_to_path[list(scenario_options.keys())[0]].keys())

    agg_dict = {filename: [] for filename in files_to_aggregate}
    for filename, scenario in itertools.product(files_to_aggregate, scenario_options.keys()) :
        agg_dict[filename].append(read_pickle_folder(pkl_folder_filepath_creation(scenario), filename, "df"))
    for filename in files_to_aggregate:
        df = pd.concat(agg_dict[filename]).reset_index(drop=True)
        agg_dict[filename] = df
        serialize_file(df, combined_pkl_path, filename)

    summarised_data_dict = summarise_combined_data(agg_dict)

    for filename in summarised_data_dict:
        serialize_file(summarised_data_dict[filename], combined_pkl_path, filename)
        pickle_to_csv(output_save_path, combined_pkl_path, filename, reset_index=False)
