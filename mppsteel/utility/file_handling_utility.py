"""Script for handling files and folder"""

import os
import pickle

from pathlib import Path

import pandas as pd

from mppsteel.utility.log_utility import get_logger

logger = get_logger("File Handling")

def read_pickle_folder(data_path: str, pkl_file: str = "", mode: str = "dict", log: bool = False):
    """Reads a path where pickle files are stores and saves them to a dictionary

    Args:
        data_path (str): A path in the repository where pickle files are stored

    Returns:
        [dict]: A dictionary with keys based on the file names without the extension.
    """
    if pkl_file:
        mode = "df"

    if mode == "df":
        if log:
            logger.info(f"||| Loading pickle file {pkl_file} from path {data_path}")
        with open(fr"{data_path}/{pkl_file}.pickle", "rb") as f:
            return pickle.load(f)

    if mode == "dict":
        if log:
            logger.info(f"||| Loading pickle files from path {data_path}")
        new_data_dict = {}
        for pkl_file in os.listdir(data_path):
            if log:
                logger.info(f"|||| Loading {pkl_file}")
            with open(fr"{data_path}/{pkl_file}", "rb") as f:
                new_data_dict[pkl_file.split(".")[0]] = pickle.load(f)
        return new_data_dict


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
        DataFrame: A dataframe of the data file
    """
    # Full path of the file
    full_filename = fr"{data_path}/{filename}.{ext}"
    # If else logic that determines which pandas function to call based on the extension
    logger.info(f"|| Extracting file {filename}.{ext}")
    if ext == "xlsx":
        return pd.read_excel(full_filename, sheet_name=sheet)
    elif ext == "csv":
        return pd.read_csv(full_filename)


def serialize_file(object, pkl_folder: str, filename: str):
    """Serializes a file using the pickle protocol.

    Args:
        object: The object that you want to serialize.
        pkl_folder (str): The folder where you want to store the pickle file.
        filename (str): The name of the file you want to use (do not include a file extension in the string)
    """
    with open(f"{pkl_folder}/{filename}.pickle", "wb") as f:
        # Pickle the 'data' using the highest protocol available.
        logger.info(f"* Saving Pickle file {filename} to path")
        pickle.dump(object, f, pickle.HIGHEST_PROTOCOL)


def serialize_df_dict(data_path: str, data_dict: dict):
    """Iterate through each df and store the file as pickle or feather. Does not return any object.

    Args:
        data_ref (dict): A data dictionary where the DataFrames are stored
        data_path (str): The path where the pickle files will be stored
    """
    logger.info(f"||| Serializing each df to a pickle file {data_path}")
    for df_name in data_dict.keys():
        serialize_file(data_dict[df_name], data_path, df_name)


def create_folders_if_nonexistant(folder_list: list):
    for folder_path in folder_list:
        if os.path.isdir(folder_path):
            logger.info(f'{folder_path} already exists')
            pass
        else:
            logger.info(f'{folder_path} does not exist yet. Creating folder.')
            Path(folder_path).mkdir(parents=True, exist_ok=True)


def pickle_to_csv(folder_path: str, pkl_folder: str, pickle_filename: str, csv_filename: str = ''):
    df = read_pickle_folder(pkl_folder, pickle_filename)
    logger.info(f'||| Saving {pickle_filename} pickle file as {csv_filename or pickle_filename}.csv')
    if csv_filename:
        df.to_csv(f"{folder_path}/{csv_filename}.csv", index=False)
    else:
        df.to_csv(f"{folder_path}/{pickle_filename}.csv", index=False)

def create_folder_if_nonexist(folder_path: str):
    Path(folder_path).mkdir(parents=True, exist_ok=True)
