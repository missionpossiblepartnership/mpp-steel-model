"""Utility library for functions used throughout the module"""
import logging
import pickle
import sys
import os

from collections import namedtuple
from logging.handlers import TimedRotatingFileHandler

import pandas as pd
import numpy as np
import wbgapi as wb
import pycountry



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


def read_pickle_folder(data_path: str, pkl_file: str = '', mode: str = 'dict'):
    """Reads a path where pickle files are stores and saves them to a dictionary

    Args:
        data_path (str): A path in the repository where pickle files are stored

    Returns:
        [dict]: A dictionary with keys based on the file names without the extension.
    """
    if pkl_file:
        mode = 'df'
    
    if mode == 'df':
        logger.info(f'||| Loading pickle file {pkl_file} from path {data_path}')
        with open(fr'{data_path}/{pkl_file}.pickle', 'rb') as f:
            return pickle.load(f)

    if mode == 'dict':
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

def serialise_file(object, pkl_folder: str, filename: str):
    """Serializes a file using the pickle protocol.

    Args:
        object: The object that you want to serialize.
        pkl_folder (str): The folder where you want to store the pickle file.
        filename (str): The name of the file you want to use (do not include a file extension in the string)
    """
    with open(f'{pkl_folder}/{filename}.pickle', 'wb') as f:
        # Pickle the 'data' using the highest protocol available.
        logger.info(f'* Saving Pickle file {filename} to path')
        pickle.dump(object, f, pickle.HIGHEST_PROTOCOL)

def countries_extractor(extract_type: str = ['countries', 'regions']) -> pd.DataFrame:
    """Connects to the world bank api to get country-level metadata

    Args:
        extract_type (str, optional): Decide whether to return countries or regions. 
        Defaults to ['countries', 'regions'].

    Returns:
        pd.DataFrame: A DataFrame containing the country/region metadata.
    """
    countries = wb.economy.DataFrame()
    countries.reset_index(inplace=True)
    df_mapping = countries[countries['aggregate']][['id', 'name']]
    region_dict = pd.Series(df_mapping.name.values,index=df_mapping.id).to_dict()

    if extract_type == 'countries':
        countries = countries[~countries.id.isin(region_dict.keys())]

    elif extract_type == 'regions':
        countries = countries[countries.id.isin(region_dict.keys())]

    for col in ['region', 'adminregion', 'lendingType', 'incomeLevel']:
        countries[col] = countries[col].apply(lambda x: region_dict[x] if x in region_dict.keys() else '')
    countries.drop(['aggregate', 'lendingType', 'adminregion'], axis=1, inplace=True)

    countries.rename(columns={'id' : 'country_code', 'name': 'country'}, inplace=True)
    countries.reset_index(drop=True, inplace=True)

    return countries

def country_mapping_fixer(
    df: pd.DataFrame, country_colname: str, country_code_colname: str, country_to_code_dict: dict) -> pd.DataFrame:
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
    
    logger.info(f'- Fixing the country codes for {list(country_to_code_dict.keys())}')
    for item in list(country_to_code_dict.items()):
        df_c.loc[df_c[country_colname] == item[0], country_code_colname] = item[1]
    return df_c

def country_matcher(country_list: list, output_type: str = 'all') -> dict:
    """Fuzzy matches a list of countries and creates a mapping of the country to alpha-3 name.
    The function produces a dictionary of mappings and also a dictionary of all unmapped countries.

    Args:
        country_list (list): The list of countries you would like to map.
        output_type (str, optional): The output you want - mapped dictionary, unmapped dictionary or both.
        Defaults to 'all'.

    Returns:
        dict: A dictionary(ies) based on the output_type parameters
    """
    def match_country(country: str):
        # try to match the country to using pycountry.
        # If not match, return an empty string
        try:
            match = pycountry.countries.search_fuzzy(country)
            match = match[0].alpha_3
            return match
        except: # Currently no exception specification.
            return ''
    
    # Generate matched entries
    countries_dict = {}
    for country in country_list:
        countries_dict[country] = match_country(country)
        
    # Get reference of unmatched entries
    unmatched_dict = {}
    for item in countries_dict.items():
        if not item[1]:
            unmatched_dict[item[0]] = item[1]
            
    if output_type == 'all':
        return countries_dict, unmatched_dict
    if output_type == 'matches':
        return countries_dict
    if output_type == 'nonmatches':
        return unmatched_dict

def official_country_name_getter(country_code: str):
    match = pycountry.countries.get(alpha_3=country_code)
    match_attributes = dir(match)
    if 'official_name' in match_attributes:
        return match.official_name
    return ''

NEW_COUNTRY_COL_LIST = [
    'country_code', 'country', 'official_name',
    'm49_code', 'region', 'continent',
    'wsa_region', 'rmi_region'
    ]

CountryMetadata = namedtuple('CountryMetadata', NEW_COUNTRY_COL_LIST)

def create_line_through_points(year_value_dict: dict, line_shape: str = 'straight') -> pd.DataFrame:
    """A function that returns a dataframe based on a few data points.

    Args:
        year_value_dict (dict): A dictionary with year, value pairings, put as many as you want, minimum two.
        line_shape (str, optional): The shape of the fitting betwene points. Defaults to 'straight'.

    Returns:
        pd.DataFrame: A dataframe with an index as year and one value column.
    """

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
        year_range = range(year_pair[0], year_pair[1]+1)
        start_value = value_pair[0]
        end_value = value_pair[1]+1
        if line_shape == 'straight':
            values = np.linspace(start=start_value, stop=end_value, num=len(year_range))
        df = pd.DataFrame(data={'year': year_range, 'values': values})
        df_list.append(df)
    # Combine pair DataFrames into one DataFrame
    combined_df = pd.concat(df_list)
    return combined_df.set_index('year')