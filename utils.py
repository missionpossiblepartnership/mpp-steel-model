"""Utility library for functions used throughout the module"""
import logging
import pickle
import sys
import os

import pandas as pd

from logging.handlers import TimedRotatingFileHandler

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = "MPP_STEEL_LOGFILE.log"

def get_console_handler():
    """Formats the log for console output

    Returns:
        StreamHandler: A formatted stream handler
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler

def get_file_handler():
    """Formats the log for file output

    Returns:
        [type]: A formatted file handler
    """
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler

def get_logger(logger_name, create_logfile: bool = False):
    """Creates a log object that can be outputted to file or std output.

    Args:
        logger_name ([type]): Defines the name of the log based on the user input.
        create_logfile (bool, optional): Determines whether to create a logfile. Defaults to False.

    Returns:
        [type]: [description]
    """
    generic_logger = logging.getLogger(logger_name)
    generic_logger.setLevel(logging.DEBUG) # better to have too much log than not enough
    generic_logger.addHandler(get_console_handler())
    if create_logfile:
        generic_logger.addHandler(get_file_handler())
    generic_logger.propagate = False # rarely necessary to propagate the error up to parent
    return generic_logger

logger = get_logger('Utils')


def read_pickle_folder(data_path: str):
    """Reads a path where pickle files are stores and saves them to a dictionary

    Args:
        data_path (str): A path in the repository where pickle files are stored

    Returns:
        [dict]: A dictionary with keys based on the file names without the extension.
    """
    logger.info(f'||| Loading pickle files from path {data_path}')
    new_data_dict = {}
    for pkl_file in os.listdir(data_path):
        logger.info(f'|||| Loading {pkl_file}')
        with open(fr'{data_path}/{pkl_file}', 'rb') as f:
            new_data_dict[pkl_file.split('.')[0]] = pickle.load(f)

    return new_data_dict


def extract_data(data_path: str, filename: str, ext: str, sheet: int=0) -> pd.DataFrame:
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
    full_filename = fr'{data_path}/{filename}.{ext}'
    # If else logic that determines which pandas function to call based on the extension
    logger.info(f'|| Extracting file {filename}.{ext}')
    if ext == 'xlsx':
        return pd.read_excel(full_filename, sheet_name=sheet)
    elif ext == 'csv':
        return pd.read_csv(full_filename)

def serialize_df(df: pd.DataFrame, data_path: str, filename: str):
    with open(f'{data_path}/{filename}.pickle', 'wb') as f:
        # Pickle the 'data' dictionary using the highest protocol available.
        logger.info(f'* Saving df {filename} to pickle')
        pickle.dump(df, f, pickle.HIGHEST_PROTOCOL)

def serialize_df_dict(data_path: str, data_dict: dict):
    """Iterate through each df and store the file as pickle or feather. Does not return any object.

    Args:
        data_ref (dict): A data dictionary where the DataFrames are stored
        data_path (str): The path where the pickle files will be stored
    """
    logger.info(f'||| Serializing each df to a pickle file {data_path}')
    for df_name in data_dict.keys():
        with open(f'{data_path}/{df_name}.pickle', 'wb') as f:
            # Pickle the 'data' dictionary using the highest protocol available.
            logger.info(f'* Saving df {df_name} to pickle')
            pickle.dump(data_dict[df_name], f, pickle.HIGHEST_PROTOCOL)