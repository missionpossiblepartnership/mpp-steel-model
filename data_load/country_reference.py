"""Script that generates a country code mapper to be used to access country metadata"""

# For Data Manipulation
from collections import namedtuple

import pandas as pd
import pycountry


# For logger and units dict
from utils import (
    get_logger, read_pickle_folder, serialize_df, CountryMetadata)

from model_config import PKL_FOLDER

# Create logger
logger = get_logger('Country Reference')

def official_name_getter(country_code: str) -> str:
    """Uses pycountry to get the official name of a country given an alpha3 country code.

    Args:
        country_code (str): An alpha3 country code

    Returns:
        str: The official name of the country.
    """
    match = pycountry.countries.get(alpha_3=country_code)
    match_attributes = dir(match)
    if 'official_name' in match_attributes:
        return match.official_name
    return ''

def create_country_ref_dict(df: pd.DataFrame, country_metadata_nt: namedtuple) -> dict:
    """Creates a dictionary based on the a DataFrame with country metadata using 
    alpha3 country code as the dict key.

    Args:
        df (pd.DataFrame): A dataframe with country metadata.

    Returns:
        dict: [A dictionary with country code as the dictionary key.
    """
    logger.info('Creating Country Reference dictionary')
    country_ref_dict = {}
    for row in df.itertuples():
        country_ref_dict[row.country_code] = country_metadata_nt(
            row.country_code, row.country, row.official_name,
            row.m49_code, row.region, row.continent,  
            row.wsa_region, row.rmi_region )
    return country_ref_dict

def country_df_formatter(df: pd.DataFrame) -> pd.DataFrame:
    """Formats a country metadata DataFrame.

    Args:
        df (pd.DataFrame): A DataFrame with country metadata.

    Returns:
        pd.DataFrame: A DataFrame with country metadata.
    """
    logger.info('Formatting country DataFrame')
    df_c = df.copy()
    df_c.rename(columns={'ISO-alpha3 code': 'country_code'}, inplace=True)
    df_c['Official Country Name'] = df_c['country_code'].apply(lambda x: official_name_getter(x))
    current_columns = ['country_code', 'Country', 'Official Country Name', 'M49 Code', 'Region 1', 'Continent', 'WSA Group Region', 'RMI Model Region', 'country_code']
    new_columns = ['country_code', 'country', 'official_name', 'm49_code', 'region', 'continent', 'wsa_region', 'rmi_region']
    col_mapper = dict(zip(current_columns, new_columns))
    df_c.rename(mapper=col_mapper, axis=1, inplace=True)
    return df_c

def country_ref_getter(country_ref_dict: dict, country_code:str, ref: str = ''):
    """A getter function to retrieve an attribute of a Country Metadata object. 

    Args:
        country_ref_dict (dict): A country reference dict.
        country_code (str): An alpha3 country code
        ref (str, optional): A reference to the desired attribute. Defaults to ''.

    Returns:
        [type]: A country class object with the country data loaded as attributes.
    """
    if country_code in country_ref_dict.keys():
        country_class = country_ref_dict[country_code]
    if ref in dir(country_class):
        return getattr(country_class, ref)
    return country_class

def create_country_ref(serialize_only: bool = False) -> dict:
    """Preprocesses the country data.

    Args:
        serialize_only (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary of each country code to metadata.
    """
    logger.info('Creating final Country Reference dictionary')
    country_ref = read_pickle_folder(PKL_FOLDER, 'country_ref')
    country_ref = country_df_formatter(country_ref)
    cr_dict = create_country_ref_dict(country_ref, CountryMetadata)

    country_ref_getter(cr_dict, 'GBR', 'rmi_region')

    if serialize_only:
        serialize_df(cr_dict, PKL_FOLDER, 'country_reference_dict')
        return
    return cr_dict

create_country_ref(serialize_only=True)
