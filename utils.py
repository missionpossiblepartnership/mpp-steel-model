"""Utility library for functions used throughout the module"""
import logging
import pickle
import sys
import os

import pandas as pd
import wbgapi as wb

from thefuzz import process

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

def country_mapper(data_type: str = 'all'):
    """Creates a reference of the world bank country data.

    Args:
        data_type (str, optional): Defines the data you want to return. 
        'all': return both a dictionary mapper of countries to codes and a country reference list.
        'dict': return just the dictoinary mapper
        'list': return just the country list.
        Defaults to 'all'.

    Returns:
        dict, list: A dictionary or list or both.
    """
    temp_df = countries_extractor('countries')[['country', 'country_code']]
    mapping = dict(zip(temp_df['country'], temp_df['country_code']))
    list_ref = list(mapping.keys())

    if data_type == 'all':
        logger.info('Generating the country-to-code mapping and country reference')
        return mapping, list_ref
    if data_type == 'dict':
        logger.info('Generating the country-to-code mapping')
        return mapping
    if data_type == 'list':
        logger.info('Generating the country reference')
        return list_ref

def generate_country_matching_dict(
    countries_to_match: list, countries_to_reference: list, print_output: bool = False) -> dict:
    """Generates a country string matching dictionary based on thefuzz library.

    Args:
        countries_to_match (list): A list of the countries you want to match to the reference countries
        countries_to_reference (list): A list of the reference countries you want to match against.
        print_output (bool): Boolean that determines whether matches are printed to the console. Defaults to False.

    Returns:
        dict: A dictionary with the highest ranking matches. 
        Warning: does not always give a perfect result
    """
    country_matching_dict = {}
    logger.info('Generating a country match for each country')
    for country in countries_to_match:
        match = process.extract(country, countries_to_reference, limit=3)
        if print_output:
            print(f'Country: {country} | Matches: {match}')
        country_matching_dict[country] = match[0][0]
    return country_matching_dict

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
