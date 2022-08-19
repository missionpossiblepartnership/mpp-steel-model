"""Script for handling files and folder"""

import os
import pickle

from pathlib import Path
import re
from typing import AnyStr, Dict, Mapping, MutableMapping, Optional, Sequence, Union

import pandas as pd
from mppsteel.config.model_config import (
    COMBINED_OUTPUT_FOLDER_NAME,
    FINAL_DATA_OUTPUT_NAME,
    INTERMEDIATE_DATA_OUTPUT_NAME,
    PKL_DATA_FINAL,
    PKL_DATA_INTERMEDIATE,
    PKL_FOLDER,
    UNDERSCORE_NUMBER_REGEX,
)

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def read_pickle_folder(
    data_path: str, pkl_file: str = "", mode: str = "dict", log: bool = False
) -> Union[pd.DataFrame, dict]:
    """Reads a path where pickle files are stores and saves them to a dictionary

    Args:
        data_path (str): A path in the repository where pickle files are stored
        pkl_file (str, optional): The file you want to unpickle. Defaults to "".
        mode (str, optional): Describes the unpickled format: A dictionary (dict) or a DataFrame (df). Defaults to "dict".
        log (bool, optional): Optional flag to log file read. Defaults to False.

    Returns:
        Union[pd.DataFrame, dict]: A DataFrame or a Dictionary object depending on `mode`.
    """

    if pkl_file:
        mode = "df"

    if mode == "df":
        if log:
            logger.info(f"||| Loading pickle file {pkl_file} from path {data_path}")
        with open(fr"{data_path}/{pkl_file}.pickle", "rb") as f:
            data: pd.DataFrame = pickle.load(f)

    elif mode == "dict":
        if log:
            logger.info(f"||| Loading pickle files from path {data_path}")
        new_data_dict: Dict[str, pd.DataFrame] = {}
        for pkl_file in os.listdir(data_path):
            if log:
                logger.info(f"|||| Loading {pkl_file}")
            with open(fr"{data_path}/{pkl_file}", "rb") as f:
                new_data_dict[pkl_file.split(".")[0]] = pickle.load(f)
        data: dict = new_data_dict
    return data


def extract_data(
    data_path: str, filename: str, ext: str, sheet: int = 0
) -> pd.DataFrame:
    """Extracts data from excel or csv files based on input parameters

    Args:
        data_path (str): path where data files are stored
        filename (str): name of file to extract (without extension)
        ext (str): extension of the file to extract
        sheet (int, optional): Number of the sheet to extract. For xlsx (workbook) files only. - . Defaults to 0.

    Returns:
        pd.DataFrame: A dataframe of the data file
    """
    # Full path of the file
    full_filename = fr"{data_path}/{filename}.{ext}"
    # If else logic that determines which pandas function to call based on the extension
    logger.info(f"|| Extracting file {filename}.{ext}")
    if ext == "xlsx":
        return pd.read_excel(full_filename, sheet_name=sheet)
    elif ext == "csv":
        return pd.read_csv(full_filename)


def serialize_file(obj, pkl_folder: str, filename: str) -> None:
    """Serializes a file using the pickle protocol.

    Args:
        obj: The object that you want to serialize.
        pkl_folder (str): The folder where you want to store the pickle file.
        filename (str): The name of the file you want to use (do not include a file extension in the string)
    """
    filename = f"{pkl_folder}/{filename}.pickle"
    with open(filename, "wb") as f:
        # Pickle the 'data' using the highest protocol available.
        logger.info(f"* Saving Pickle file {filename} to path")
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def serialize_df_dict(data_path: str, data_dict: dict) -> None:
    """Iterate through each df and store the file as pickle or feather. Does not return any object.

    Args:
        data_ref (dict): A data dictionary where the DataFrames are stored
        data_path (str): The path where the pickle files will be stored
    """
    logger.info(f"||| Serializing each df to a pickle file {data_path}")
    for df_name in data_dict:
        serialize_file(data_dict[df_name], data_path, df_name)


def create_folders_if_nonexistant(folder_list: list) -> None:
    """For each path in the `folder_list`, check if the folder already exists, if it doesn't create it.
    Args:
        folder_list (list): A list of folder paths to check.
    """
    for folder_path in folder_list:
        if os.path.isdir(folder_path):
            logger.info(f"{folder_path} already exists")
        else:
            logger.info(f"{folder_path} does not exist yet. Creating folder.")
            Path(folder_path).mkdir(parents=True, exist_ok=True)


def pickle_to_csv(
    folder_path: str,
    pkl_folder: str,
    pickle_filename: str,
    csv_filename: str = "",
    reset_index: bool = False,
) -> None:
    """Checks a folder path where a pickled DataFrame is stored. Loads the DataFrame and converts it to a .csv file.

    Args:
        folder_path (str): The path where you want to save the .csv file.
        pkl_folder (str): The path where the pickled DataFrame is stored.
        pickle_filename (str): The name of the pickle file you want to load. (No .pkl/.pickle extension necessary).
        csv_filename (str, optional): The name of the newly created csv file. (No .csv extension necessary). If none, defaults to pickle_filename. Defaults to "".
    """
    df = read_pickle_folder(pkl_folder, pickle_filename, "df")
    if reset_index:
        df.reset_index(inplace=True)

    logger.info(
        f"||| Saving {pickle_filename} pickle file as {csv_filename or pickle_filename}.csv"
    )
    if csv_filename:
        df.to_csv(f"{folder_path}/{csv_filename}.csv", index=False)
    else:
        df.to_csv(f"{folder_path}/{pickle_filename}.csv", index=False)


def create_folder_if_nonexist(folder_path: str) -> None:
    """Create folder at a specified path if it does not already exist.

    Args:
        folder_path (str): The path to create if it does not exist.
    """
    Path(folder_path).mkdir(parents=True, exist_ok=True)


def get_scenario_pkl_path(
    scenario: str = None,
    pkl_folder_type: str = None,
    default_path: bool = False,
    model_run: str = "",
    iteration_run: bool = False,
) -> str:
    if pkl_folder_type == "combined":
        return f"{PKL_FOLDER}/{COMBINED_OUTPUT_FOLDER_NAME}"

    pkl_folder_type_ext = (
        INTERMEDIATE_DATA_OUTPUT_NAME
        if pkl_folder_type == "intermediate"
        else FINAL_DATA_OUTPUT_NAME
    )
    default_path_ext = (
        PKL_DATA_INTERMEDIATE if pkl_folder_type == "intermediate" else PKL_DATA_FINAL
    )
    full_path = f"{PKL_FOLDER}/{scenario}/{pkl_folder_type_ext}"

    if default_path:
        return default_path_ext
    elif model_run:
        return f"{full_path}/run_{model_run}"
    elif iteration_run:
        base_scenario: Union[AnyStr, Optional[str]] = re.sub(UNDERSCORE_NUMBER_REGEX, "", scenario)
        return f"{PKL_FOLDER}/iteration_runs/{base_scenario}/{scenario}/{pkl_folder_type_ext}"
    return full_path


def return_pkl_paths(
    scenario_name: str, paths: Union[dict, None] = None, model_run: str = ""
) -> tuple:
    """Returns the paths for a scenario and customises the extension depening on the base path given to it and whether it is a specific model run.

    Args:
        scenario_name (str): The name of the scenario for which to create paths.
        paths (Union[dict, None], optional): Specific pkl paths to override the default paths. Defaults to None.
        model_run (str, optional): The iteration run of the model. Defaults to "".

    Returns:
        tuple: A tuple of three paths to for intermediate data folder(s) and final path.
    """
    intermediate_path_preprocessing = get_scenario_pkl_path(
        scenario=scenario_name,
        pkl_folder_type="intermediate",
    )
    intermediate_path = get_scenario_pkl_path(
        scenario=scenario_name, pkl_folder_type="intermediate", model_run=model_run
    )
    final_path = get_scenario_pkl_path(
        scenario=scenario_name, pkl_folder_type="final", model_run=model_run
    )
    if paths:
        if "intermediate_path" in paths.keys():
            intermediate_path_preprocessing = paths["intermediate_path"]
            intermediate_path = paths["intermediate_path"]

        if "final_path" in paths.keys():
            final_path = paths["final_path"]

    return intermediate_path_preprocessing, intermediate_path, final_path


def create_scenario_paths(scenario_name: str) -> None:
    """Create customized intermediate and final paths based on a scenario name.

    Args:
        scenario_name (str): The name of the scenario for which to create paths.
    """
    intermediate_path = get_scenario_pkl_path(scenario_name, "intermediate")
    final_path = get_scenario_pkl_path(scenario_name, "final")
    create_folders_if_nonexistant([intermediate_path, final_path])


def generate_files_to_path_dict(
    scenarios: Sequence,
    pkl_paths: Union[dict, None] = None,
    model_run: str = "",
    create_path: bool = False,
) -> dict:
    """Creates a filepath dictionary for each scenario in scenarios. Each filepath is based on pkl_paths and
    is customized in the return_pkl_paths with the optional model_run parameter.
    Each path is optionally created using tue create_path boolean flag.

    Args:
        scenarios (Sequence): The list of scenarios that form the keys of the filepath dictionary.
        pkl_paths (Union[dict, None], optional): Specific pkl paths to override the default paths. Defaults to None.
        model_run (str, optional): The iteration run of the model. Defaults to "".
        create_path (bool, optional): Flag to determine whether new folders should be generated. Defaults to False.

    Returns:
        dict: A nested filepath dictionary with scenario as key, file as second key, and each path as the value.
    """
    files_to_path: MutableMapping[str, Dict[str, str]] = {scenario: {} for scenario in scenarios}
    for scenario_name in scenarios:
        (
            intermediate_path_preprocessing,
            intermediate_path,
            final_path,
        ) = return_pkl_paths(scenario_name, pkl_paths, model_run)
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
            "calculated_emissivity_combined": intermediate_path_preprocessing,
        }
    return files_to_path
