"""Utility library for functions used throughout the module"""
import logging
import itertools
import time
import pickle
import sys
import os

from collections import namedtuple
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pandas as pd
import numpy as np
import pycountry

from currency_converter import CurrencyConverter

from mppsteel.utility.reference_lists import NEW_COUNTRY_COL_LIST, FILES_TO_REFRESH

from mppsteel.model_config import (
    PKL_DATA_FINAL, OUTPUT_FOLDER, LOG_PATH,
    PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE,
    RESULTS_REGIONS_TO_MAP,
)

def get_today_time():
    return datetime.today().strftime('%y%m%d_%H%M%S')

LOG_FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def get_console_handler():
    """Formats the log for console output

    Returns:
        StreamHandler: A formatted stream handler
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(LOG_FORMATTER)
    return console_handler


def get_file_handler():
    """Formats the log for file output

    Returns:
        [type]: A formatted file handler
    """
    today_time = get_today_time()
    log_filepath = f"{LOG_PATH}/mppsteel_{today_time}.log"
    file_handler = TimedRotatingFileHandler(log_filepath, when="midnight")
    file_handler.setFormatter(LOG_FORMATTER)
    return file_handler


def get_logger(logger_name, create_logfile: bool = True):
    """Creates a log object that can be outputted to file or std output.

    Args:
        logger_name ([type]): Defines the name of the log based on the user input.
        create_logfile (bool, optional): Determines whether to create a logfile. Defaults to False.

    Returns:
        [type]: [description]
    """
    generic_logger = logging.getLogger(logger_name)
    generic_logger.setLevel(
        logging.DEBUG
    )  # better to have too much log than not enough
    generic_logger.addHandler(get_console_handler())
    if create_logfile:
        generic_logger.addHandler(get_file_handler())
    generic_logger.propagate = (
        False  # rarely necessary to propagate the error up to parent
    )
    return generic_logger


logger = get_logger("Utils")


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

def country_mapping_fixer(
    df: pd.DataFrame,
    country_colname: str,
    country_code_colname: str,
    country_to_code_dict: dict,
) -> pd.DataFrame:
    """Fixes country code mapping problems in a DataFrame by overriding the existing mapping with a dictionary.

    Args:
        df (pd.DataFrame): DataFrame that you want to amend.
        country_colname (str): The name of the column containing the countries to subset the dataframe with.
        country_code_colname (str): The name of the column containing the countries to subset the dataframe with.
        country_to_code_dict (dict): The name of the dictionary containing the country and the code.

    Returns:
        pd.DataFrame: An amended dataframe with the code mappings fixed
    """
    df_c = df.copy()

    logger.info(f"- Fixing the country codes for {list(country_to_code_dict.keys())}")
    for item in list(country_to_code_dict.items()):
        df_c.loc[df_c[country_colname] == item[0], country_code_colname] = item[1]
    return df_c

def match_country(country: str):
    # try to match the country to using pycountry.
    # If not match, return an empty string
    try:
        match = pycountry.countries.search_fuzzy(country)
        match = match[0].alpha_3
        return match
    except:  # Currently no exception specification.
        return ""

def country_matcher(country_list: list, output_type: str = "all") -> dict:
    """Fuzzy matches a list of countries and creates a mapping of the country to alpha-3 name.
    The function produces a dictionary of mappings and also a dictionary of all unmapped countries.

    Args:
        country_list (list): The list of countries you would like to map.
        output_type (str, optional): The output you want - mapped dictionary, unmapped dictionary or both.
        Defaults to 'all'.

    Returns:
        dict: A dictionary(ies) based on the output_type parameters
    """

    # Generate matched entries
    countries_dict = {}
    for country in country_list:
        countries_dict[country] = match_country(country)

    # Get reference of unmatched entries
    unmatched_dict = {}
    for item in countries_dict.items():
        if not item[1]:
            unmatched_dict[item[0]] = item[1]

    if output_type == "all":
        return countries_dict, unmatched_dict
    if output_type == "matches":
        return countries_dict
    if output_type == "nonmatches":
        return unmatched_dict


def official_country_name_getter(country_code: str):
    match = pycountry.countries.get(alpha_3=country_code)
    match_attributes = dir(match)
    if "official_name" in match_attributes:
        return match.official_name
    return ""


CountryMetadata = namedtuple("CountryMetadata", NEW_COUNTRY_COL_LIST)


def create_line_through_points(
    year_value_dict: dict, line_shape: str = "straight"
) -> pd.DataFrame:
    """A function that returns a dataframe based on a few data points.

    Args:
        year_value_dict (dict): A dictionary with year, value pairings, put as many as you want, minimum two.
        line_shape (str, optional): The shape of the fitting betwene points. Defaults to 'straight'.

    Returns:
        pd.DataFrame: A dataframe with an index as year and one value column.
    """
    logger.info(f'Creating line through points {year_value_dict}')
    # Creates a pairing for all elements based on location
    def create_value_pairings(iterable: list) -> list:
        value_pairings = []
        it = iter(iterable)
        for x in it:
            try:
                value_pairings.append((x, next(it)))
            except StopIteration:
                value_pairings.append((iterable[-2], iterable[-1]))
        return value_pairings

    # Create pairings for years and values
    years = [int(year) for year in year_value_dict.keys()]
    values = list(year_value_dict.values())
    year_pairs = create_value_pairings(years)
    value_pairs = create_value_pairings(values)

    # Create dataframes for every pairing
    df_list = []
    for year_pair, value_pair in zip(year_pairs, value_pairs):
        year_range = range(year_pair[0], year_pair[1] + 1)
        start_value = value_pair[0]
        end_value = value_pair[1] + 1
        if line_shape == "straight":
            values = np.linspace(start=start_value, stop=end_value, num=len(year_range))
        df = pd.DataFrame(data={"year": year_range, "values": values})
        df_list.append(df)
    # Combine pair DataFrames into one DataFrame
    combined_df = pd.concat(df_list)
    return combined_df.set_index("year")

def create_list_permutations(list1: list, list2: list):
    comb =  [list(zip(each_permutation, list2)) for each_permutation in itertools.permutations(list1, len(list2))]
    return list(itertools.chain(*comb))


def return_furnace_group(furnace_dict: dict, tech:str):
    for key in furnace_dict.keys():
        if tech in furnace_dict[key]:
            return furnace_dict[key]

pd.DataFrame().to_csv()

def pickle_to_csv(folder_path: str, pkl_folder: str, pickle_filename: str, csv_filename: str = ''):
    df = read_pickle_folder(pkl_folder, pickle_filename)
    logger.info(f'||| Saving {pickle_filename} pickle file as {csv_filename or pickle_filename}.csv')
    if csv_filename:
        df.to_csv(f"{folder_path}/{csv_filename}.csv", index=False)
    else:
        df.to_csv(f"{folder_path}/{pickle_filename}.csv", index=False)

def format_times(start_t: float, end_t: float):
    time_diff = end_t - start_t
    return f'{time_diff :0.2f} seconds | {time_diff / 60 :0.2f} minutes'

class TimeContainerClass:
    def __init__(self):
        self.time_container = {}

    def update_time(self, func_name: str, timings: str):
        self.time_container[func_name] = timings
    
    def return_time_container(self, return_object: bool = False):
        time_container = self.time_container
        for entry in time_container:
            print(f'The {entry} function took {time_container[entry]}')
        if return_object:
            return time_container

TIME_CONTAINER = TimeContainerClass()

def timer_func(func):
    def wrap_func(*args, **kwargs):
        starttime = time.time()
        result = func(*args, **kwargs)
        endtime = time.time()
        TIME_CONTAINER.update_time(func.__name__, format_times(starttime, endtime))
        return result
    return wrap_func

def add_scenarios(df: pd.DataFrame, scenario_dict: dict):
    df_c = df.copy()
    for key in scenario_dict.keys():
        df_c[f'scenario_{key}'] = scenario_dict[key]
    return df_c


def stdout_query(question: str, default: str, options: str):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
            It must be "yes" (the default), "no" or None (meaning
            an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    if default not in options:
        raise ValueError(f'invalid default answer {default}. Not in options: {options}')

    while True:
        sys.stdout.write(f'{question} {default}')
        choice = input().lower()
        if choice == '':
            return default
        elif choice != "" and choice in options:
            return choice
        elif choice != "" and choice not in options:
            sys.stdout.write(f"Please respond with a choice from {options}.\n")

def get_region_from_country_code(country_code: str, schema: str, country_ref_dict: dict):
    if country_code == 'TWN':
        country_code = 'CHN'
    country_metadata_obj = country_ref_dict[country_code]
    options = ["m49_code", "region", "continent", "wsa_region", "rmi_region"]
    if schema in dir(country_metadata_obj):
        return getattr(country_metadata_obj, schema)
    else:
        raise AttributeError(f'Schema: {schema} is not an attribute of {country_code} CountryMetadata object. Choose from the following options: {options}')

def add_regions(df: pd.DataFrame, country_ref_dict: dict, country_ref_col: str, region_schema: str,):
    df_c = df.copy()
    df_c[f'region_{region_schema}'] = df_c[country_ref_col].apply(lambda country: get_region_from_country_code(country, region_schema, country_ref_dict))
    return df_c

def add_results_metadata(df: pd.DataFrame, scenario_dict: dict):
    country_reference_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'country_reference_dict', 'dict')
    df_c = df.copy()
    df_c = add_scenarios(df_c, scenario_dict)
    for schema in RESULTS_REGIONS_TO_MAP:
        df_c = add_regions(df_c, country_reference_dict, 'country_code', schema)
    return df_c

def create_folder_if_nonexist(folder_path: str):
    Path(folder_path).mkdir(parents=True, exist_ok=True)


def get_currency_rate(base: str):
    logger.info(f'Getting currency exchange rate for {base}')
    c = CurrencyConverter()
    if base.lower() == 'usd':
        return c.convert(1, 'USD', 'EUR')
    if base.lower() == 'eur':
        return c.convert(1, 'EUR', 'USD')

def create_folders_if_nonexistant(folder_list: list):
    for folder_path in folder_list:
        if os.path.isdir(folder_path):
            logger.info(f'{folder_path} already exists')
            pass
        else:
            logger.info(f'{folder_path} does not exist yet. Creating folder.')
            Path(folder_path).mkdir(parents=True, exist_ok=True)