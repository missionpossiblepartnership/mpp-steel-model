"""Script to manage making multiple model_runs"""

import math
import shutil

import multiprocessing as mp
from typing import Dict, Mapping

import pandas as pd
import modin.pandas as mpd
from tqdm import tqdm

from mppsteel.config.model_config import (
    COMBINED_OUTPUT_FOLDER_NAME,
    DEFAULT_NUMBER_OF_RUNS,
    MULTIPLE_RUN_SCENARIO_FOLDER_NAME,
    OUTPUT_FOLDER,
    PKL_DATA_COMBINED,
    PKL_FOLDER,
)
from mppsteel.config.reference_lists import (
    MULTI_RUN_MULTI_SCENARIO_SUMMARY_FILENAMES,
    FINAL_RESULT_PKL_FILES,
    INTERMEDIATE_RESULT_PKL_FILES,
)
from mppsteel.config.mypy_config_settings import MYPY_DICT_STR_LIST, MYPY_SCENARIO_TYPE, MYPY_SCENARIO_TYPE_DICT
from mppsteel.multi_run_module.multiprocessing_functions import multi_run_function
from mppsteel.model_results.multiple_model_run_summary import summarise_combined_data
from mppsteel.model_graphs.graph_production import create_combined_scenario_graphs
from mppsteel.model_results.resource_demand_summary import (
    create_resource_demand_summary,
)
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    create_folder_if_nonexist,
    generate_files_to_path_dict,
    pickle_to_csv,
    read_pickle_folder,
    serialize_file,
    get_scenario_pkl_path,
)

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.utils import split_list_into_chunks

logger = get_logger(__name__)


def make_multiple_model_runs(
    scenario_dict: MYPY_SCENARIO_TYPE,
    number_of_runs: int = DEFAULT_NUMBER_OF_RUNS,
    remove_run_folders: bool = False,
) -> None:

    scenario_name = str(scenario_dict["scenario_name"])
    logger.info(f"Generating the scenario data for {scenario_name}")
    pkl_output_folder = pkl_folder_filepath_creation(scenario_name, create_folder=True)
    run_range = range(1, number_of_runs + 1)
    run_range_chunks = split_list_into_chunks(
        run_range, math.ceil(len(run_range) / mp.cpu_count())
    )
    for run_range_chunk in run_range_chunks:
        multi_run_function(run_range_chunk, scenario_dict)
    run_container = aggregate_results(
        scenario_name, run_range, number_of_runs, remove_run_folders=remove_run_folders
    )

    store_run_container_to_pkl(run_container, pkl_output_folder)


def multiprocessing_scenarios_multiple_scenarios_multiple_runs(
    scenario_options: MYPY_SCENARIO_TYPE_DICT,
    number_of_runs: int,
    remove_run_folders: bool = False,
) -> None:
    for scenario in scenario_options:
        make_multiple_model_runs(
            scenario_dict=scenario_options[scenario],
            number_of_runs=number_of_runs,
            remove_run_folders=remove_run_folders,
        )


@timer_func
def join_scenario_data(
    scenario_options: list,
    new_folder: bool = True,
    timestamp: str = "",
    final_outputs_only: bool = True,
) -> None:
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
        output_folder_path=PKL_DATA_COMBINED, serialize=True
    )
    resource_demand_summary.to_csv(
        f"{output_save_path}/resource_demand_summary.csv", index=False
    )

    create_combined_scenario_graphs(filepath=output_save_path_graphs)


def aggregate_results(
    scenario_name: str,
    run_range: range,
    number_of_runs: int,
    remove_run_folders: bool = False,
) -> dict:
    generic_files_to_path_dict = generate_files_to_path_dict(
        [
            scenario_name,
        ]
    )[scenario_name]
    run_container: MYPY_DICT_STR_LIST = {filename: [] for filename in generic_files_to_path_dict}
    for model_run in run_range:
        model_run_files_to_path = generate_files_to_path_dict(
            scenarios=[
                scenario_name,
            ],
            model_run=str(model_run),
        )
        for filename in run_container:
            store_result_to_container(
                run_container=run_container,
                scenario_name=scenario_name,
                filename=filename,
                pkl_path=model_run_files_to_path[scenario_name][filename],
                model_run=model_run,
                number_of_runs=number_of_runs,
            )

    if remove_run_folders:
        for model_run in run_range:
            intermediate_path = get_scenario_pkl_path(
                scenario=scenario_name,
                pkl_folder_type="intermediate",
                model_run=str(model_run),
            )
            final_path = get_scenario_pkl_path(
                scenario=scenario_name, pkl_folder_type="final", model_run=str(model_run)
            )
            shutil.rmtree(intermediate_path)
            shutil.rmtree(final_path)

    return run_container


def aggregate_multi_run_scenarios(
    scenario_options: MYPY_SCENARIO_TYPE_DICT,
    single_scenario: bool = False,
    new_folder: bool = True,
    timestamp: str = "",
) -> None:
    combined_pkl_path = pkl_folder_filepath_creation("combined", create_folder=True)
    output_save_path = output_folder_path_creation(new_folder, timestamp)
    if single_scenario:
        output_save_path = output_folder_path_creation(
            new_folder, timestamp, return_single_scenario(scenario_options)
        )
    files_to_aggregate = list(
        generate_files_to_path_dict(list(scenario_options.keys()))[
            return_single_scenario(scenario_options)
        ].keys()
    )
    scenario_set = set(scenario_options.keys())

    for filename in tqdm(
        files_to_aggregate,
        total=len(files_to_aggregate),
        desc="Running pkl scenario data merge",
    ):
        logger.info(f"Running through summary flow for {filename}")
        filename_dict: Dict[str, pd.DataFrame] = {}
        for scenario in tqdm(scenario_set, total=len(scenario_set), desc="Scenario loop"):
            assign_to_dict(filename_dict, filename, scenario)
        logger.info(f"Creating summary data for {filename}")
        results_dict: MYPY_DICT_STR_LIST = {file: [] for file in MULTI_RUN_MULTI_SCENARIO_SUMMARY_FILENAMES}
        scenarios_present = filename_dict.keys()
        for scenario in scenarios_present:
            subset = filename_dict[scenario]
            print(f"{scenario} -> rows: {len(subset)}")
            summarise_combined_data(subset, results_dict, filename)

        scenarios_present_set = set(scenarios_present)
        assert scenarios_present_set == scenario_set, f"Not all scenarios in df. Difference: {scenario_set.difference(scenarios_present_set)}."

        for summary_filename in results_dict:
            if results_dict[summary_filename]:
                summary_df = pd.concat(list(results_dict[summary_filename])).reset_index(drop=True)
                serialize_file(summary_df, combined_pkl_path, summary_filename)
                pickle_to_csv(output_save_path, combined_pkl_path, summary_filename, reset_index=False)


def add_model_run_metadata_columns(
    df: pd.DataFrame, scenario_name: str, order_of_run: int, total_runs: int
) -> pd.DataFrame:
    df_c = df.copy()
    if "scenario" not in df_c.columns:
        df_c["scenario"] = scenario_name
    df_c["order_of_run"] = order_of_run
    df_c["total_runs"] = total_runs
    return df_c


def store_result_to_container(
    run_container: Mapping,
    scenario_name: str,
    filename: str,
    pkl_path: str,
    model_run: int,
    number_of_runs: int,
) -> None:
    df = read_pickle_folder(pkl_path, filename, "df")
    df = add_model_run_metadata_columns(df, scenario_name, model_run, number_of_runs)
    run_container[filename].append(df)


def store_run_container_to_pkl(
    run_container: Mapping,
    pkl_path: str,
) -> None:
    for filename in run_container:
        df = mpd.concat(run_container[filename]).reset_index(drop=True)
        serialize_file(df._to_pandas(), pkl_path, filename)


def pkl_folder_filepath_creation(
    scenario_name: str, create_folder: bool = False
) -> str:
    pkl_output_folder = (
        f"{PKL_FOLDER}/{MULTIPLE_RUN_SCENARIO_FOLDER_NAME}/{scenario_name}"
    )
    if create_folder:
        create_folder_if_nonexist(pkl_output_folder)
    return pkl_output_folder


def output_folder_path_creation(
    dated_output_folder: bool = True, timestamp: str = "", single_scenario: str = ""
) -> str:
    output_save_path = OUTPUT_FOLDER
    output_folder_name = (
        f"{MULTIPLE_RUN_SCENARIO_FOLDER_NAME} {single_scenario} {timestamp}"
        if single_scenario
        else f"{MULTIPLE_RUN_SCENARIO_FOLDER_NAME} {timestamp}"
    )
    output_folder_filepath = "/"
    if dated_output_folder:
        output_folder_filepath = f"{OUTPUT_FOLDER}/{output_folder_name}"
        create_folder_if_nonexist(output_folder_filepath)
        output_save_path = output_folder_filepath
    return output_save_path


def assign_to_dict(assigning_dict, filename: str, scenario: str) -> None:
    assigning_dict[scenario] = read_pickle_folder(pkl_folder_filepath_creation(scenario), filename, "df")


def return_single_scenario(scenario_options: Mapping) -> str:
    return list(scenario_options.keys())[0]
