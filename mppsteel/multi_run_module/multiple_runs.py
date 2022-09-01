"""Script to manage making multiple model_runs"""

import math
import shutil

import multiprocessing as mp
from typing import Callable, Dict, MutableMapping

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
from mppsteel.config.mypy_config_settings import (
    MYPY_DICT_STR_LIST,
    MYPY_SCENARIO_TYPE,
    MYPY_SCENARIO_TYPE_DICT
)
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
    function_to_run: Callable,
    scenario_dict: MYPY_SCENARIO_TYPE,
    number_of_runs: int = DEFAULT_NUMBER_OF_RUNS,
    remove_run_folders: bool = False,
) -> None:
    """Function used to make multiple model runs of a single scenario. 
    Splits multiple model into chunks of predetermined length and passes list to multiprocessing function.
    The chunk lenght is determined by the number_of_runs variable and the number of cpu's.

    The function runs all of the model run's store the files to pkl, and then aggregates all of the runs 
    (deleting the singular files if requested with remove_run_folders, and saves the file to pkl.


    Args:
        function_to_run: (Callable): The function to run in the multiprocessing function multi_run_function.
        scenario_dict (MYPY_SCENARIO_TYPE): A mapping object with scenario settings.
        number_of_runs (int, optional): The number of times to run the scenario. Defaults to DEFAULT_NUMBER_OF_RUNS.
        remove_run_folders (bool, optional): Flag to determine whether tho remove the singular files once they have run. Defaults to False.
    """
    scenario_name = str(scenario_dict["scenario_name"])
    logger.info(f"Generating the scenario data for {scenario_name}")
    pkl_output_folder = pkl_folder_filepath_creation(scenario_name, create_folder=True)
    run_range = range(1, number_of_runs + 1)
    if number_of_runs > 50:
        run_range_chunks = split_list_into_chunks(
            run_range, math.ceil(len(run_range) / mp.cpu_count())
        )
        for run_range_chunk in run_range_chunks:
            multi_run_function(run_range_chunk, scenario_dict, function_to_run)
    else:
        multi_run_function(run_range, scenario_dict, function_to_run)
    run_container = aggregate_results(
        scenario_name, run_range, number_of_runs, remove_run_folders=remove_run_folders
    )
    store_run_container_to_pkl(run_container, pkl_output_folder)


def multiprocessing_scenarios_multiple_scenarios_multiple_runs(
    function_to_run: Callable,
    scenario_options: MYPY_SCENARIO_TYPE_DICT,
    number_of_runs: int,
    remove_run_folders: bool = False,
) -> None:
    """Function used to make multiple model runs of multiple scenarios by passing each scenario to make_multiple_model_runs.

    Args:
        function_to_run (Callable): The function to run in the multiprocessing function make_multiple_model_runs.
        scenario_dict (MYPY_SCENARIO_TYPE): A mapping object with all the scenario settings.
        number_of_runs (int, optional): The number of times to run the scenario. Defaults to DEFAULT_NUMBER_OF_RUNS.
        remove_run_folders (bool, optional): Flag to determine whether tho remove the singular files once they have run. Defaults to False.
    """
    for scenario in scenario_options:
        make_multiple_model_runs(
            function_to_run,
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
    """Joins the scenario data together from multiple scenario single run pkl data, saving the ouput as csv and generating summary csv files and graphs.

    Args:
        scenario_options (list): The list of scenarios to join together.
        new_folder (bool, optional): Flag to determine whether output files should be saved in a new folder. Defaults to True.
        timestamp (str, optional): A timestamp for the new folder name. Defaults to "".
        final_outputs_only (bool, optional): Flag to determine whether intermdiate files should also be joined together and saved as csv. Defaults to True.
    """
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
    """A function that aggregates multiple model run pkl data together as a dictionary.

    Args:
        scenario_name (str): The name of a scenario.
        run_range (range): A range object representing the number of runs in the model.
        number_of_runs (int): The number of runs to aggregate.
        remove_run_folders (bool, optional): Flag to determine whether tho remove the singular files once they have run. Defaults to False.

    Returns:
        dict: A dictionary containing each filename and the aggregated data from each model run.
    """
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

@timer_func
def aggregate_multi_run_scenarios(
    scenario_options: MYPY_SCENARIO_TYPE_DICT,
    single_scenario: bool = False,
    new_folder: bool = True,
    timestamp: str = "",
) -> None:
    """Aggregates pkl data from multiple runs and multiple scenarios into a single file and saves combined file as a pkl.

    Args:
        scenario_options (MYPY_SCENARIO_TYPE): A mapping object with all the scenario settings.
        single_scenario (bool, optional): A boolean flag to determine if only one scenario in scenario_options should be chosen. Defaults to False.
        new_folder (bool, optional): Flag to determine whether output files should be saved in a new folder. Defaults to True.
        timestamp (str, optional): A timestamp for the new folder name. Defaults to "".
    """
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
            assign_to_mapping_container(filename_dict, filename, scenario)
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
    """Adds metadata as columns to summary DataFrame of multiple run models.

    Args:
        df (pd.DataFrame): The initial summary DataFrame.
        scenario_name (str): The name of the scenario.
        order_of_run (int): The order of the model run.
        total_runs (int): The total number of model runs.

    Returns:
        pd.DataFrame: The modified DataFrame with the new columns.
    """
    df_c = df.copy()
    if "scenario" not in df_c.columns:
        df_c["scenario"] = scenario_name
    df_c["order_of_run"] = order_of_run
    df_c["total_runs"] = total_runs
    return df_c


def store_result_to_container(
    run_container: MutableMapping,
    scenario_name: str,
    filename: str,
    pkl_path: str,
    model_run: int,
    number_of_runs: int,
) -> None:
    """Reads a DataFrame from a pkl file. Adds metadata columns to to DataFrame. Adds the modified DataFrame to a dictionary list value.

    Args:
        run_container (MutableMapping): A container with filename as key, List[DataFrame] as value.
        scenario_name (str): The name of the scenario.
        filename (str): The name of the file to load from pkl and to save as the container key.
        pkl_path (str): The pkl path whether the pkl files are stored.
        model_run (int): The order of the model run.
        number_of_runs (int): The total number of model runs.
    """
    df = read_pickle_folder(pkl_path, filename, "df")
    df = add_model_run_metadata_columns(df, scenario_name, model_run, number_of_runs)
    run_container[filename].append(df)


def store_run_container_to_pkl(
    run_container: MutableMapping,
    pkl_path: str,
) -> None:
    """Takes a container object and serializes its values as a Pandas DataFrame.

    Args:
        run_container (MutableMapping): A container with filename as key, List[modin DataFrame] as value.
        pkl_path (str): The pkl path where the concatenated DataFrames should be stored.
    """
    for filename in run_container:
        df = mpd.concat(run_container[filename]).reset_index(drop=True)
        serialize_file(df._to_pandas(), pkl_path, filename)


def pkl_folder_filepath_creation(
    scenario_name: str, create_folder: bool = False
) -> str:
    """Creates a pkl folder filepath to be used for multiple run outputs.

    Args:
        scenario_name (str): The name of the scenario to create the filepath.
        create_folder (bool, optional): Flag to determine whether a new folder at the generated path should be created. Defaults to False.

    Returns:
        str: The generated folder path.
    """
    pkl_output_folder = (
        f"{PKL_FOLDER}/{MULTIPLE_RUN_SCENARIO_FOLDER_NAME}/{scenario_name}"
    )
    if create_folder:
        create_folder_if_nonexist(pkl_output_folder)
    return pkl_output_folder


def output_folder_path_creation(
    new_folder: bool = True, timestamp: str = "", single_scenario: str = ""
) -> str:
    """Creates an output folder path where multiple run outputs will be saved.

    Args:
        new_folder (bool, optional): Flag to determine whether output files should be saved in a new folder. Defaults to True.
        timestamp (str, optional): A timestamp for the new folder name. Defaults to "".
        single_scenario (bool, optional): A boolean flag to determine if only one scenario in scenario_options should be chosen. Defaults to False.

    Returns:
        str: The output folder path.
    """
    output_save_path = OUTPUT_FOLDER
    output_folder_name = (
        f"{MULTIPLE_RUN_SCENARIO_FOLDER_NAME} {single_scenario} {timestamp}"
        if single_scenario
        else f"{MULTIPLE_RUN_SCENARIO_FOLDER_NAME} {timestamp}"
    )
    output_folder_filepath = "/"
    if new_folder:
        output_folder_filepath = f"{OUTPUT_FOLDER}/{output_folder_name}"
        create_folder_if_nonexist(output_folder_filepath)
        output_save_path = output_folder_filepath
    return output_save_path


def assign_to_mapping_container(assigning_dict: MutableMapping, filename: str, scenario: str) -> None:
    """Loads a file from a pkl location and assigns it as a value of a Mapping object with a key of scenario.

    Args:
        assigning_dict (MutableMapping): The Mapping to store the loaded pkl files in.
        filename (str): The name of the file to load.
        scenario (str): The name of the scenario.
    """
    assigning_dict[scenario] = read_pickle_folder(
        pkl_folder_filepath_creation(scenario), filename, "df")


def return_single_scenario(scenario_options: MutableMapping) -> str:
    """Return the first key in a mapping object.

    Args:
        scenario_options (MutableMapping): The mapping object.

    Returns:
        str: The str representation of the first mapping key.
    """
    return list(scenario_options.keys())[0]
