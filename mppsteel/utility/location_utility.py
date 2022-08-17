"""Utility library for managing location"""

import itertools
import random

import pandas as pd
import pycountry
from mppsteel.config.model_config import PKL_DATA_IMPORTS, MAIN_REGIONAL_SCHEMA

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import read_pickle_folder

logger = get_logger(__name__)


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
        pd.DataFrame: An amended dataframe with the code mappings fixed.
    """
    df_c = df.copy()

    logger.info(f"- Fixing the country codes for {list(country_to_code_dict.keys())}")
    for item in list(country_to_code_dict.items()):
        df_c.loc[df_c[country_colname] == item[0], country_code_colname] = item[1]
    return df_c


def match_country(country: str) -> str:
    """Matches a country string to a recognised ISO Alpha-3 country code using the pycountry library.

    Args:
        country (str): The string containing the country name you want to match.

    Returns:
        str: A string containing the matched country. Return an empty string if no match is found.
    """
    try:
        match = pycountry.countries.search_fuzzy(country)
        return match[0].alpha_3
    except:  # Currently no exception specification.
        return ""


def country_matcher(country_list: list, output_type: str = "matches") -> dict:
    """Fuzzy matches a list of countries and creates a mapping of the country to ISO Alpha-3 name.
    The function produces a dictionary of mappings and also a dictionary of all unmapped countries.

    Args:
        country_list (list): The list of countries you would like to map.
        output_type (str, optional): The output you want - mapped dictionary, unmapped dictionary or both. Defaults to 'all'.

    Returns:
        dict: Dictionary based on the output_type parameters.
    """

    # Generate matched entries
    countries_dict = {country: match_country(country) for country in country_list}
    # Get reference of unmatched entries
    unmatched_dict = {
        item[0]: item[1] for item in countries_dict.items() if not item[1]
    }
    if output_type == "matches":
        return_dict = countries_dict
    if output_type == "nonmatches":
        return_dict = unmatched_dict
    return return_dict


def get_unique_countries(country_arrays) -> list:
    """Gets a unique list of countries from a list of arrays of countries.

    Args:
        country_arrays ([type]): An array of countries.

    Returns:
        list: A list containing the unique countries from a list of lists.
    """
    b_set = {tuple(x) for x in country_arrays}
    b_list = [list(x) for x in b_set if x]
    return list(itertools.chain(*b_list))


def get_countries_from_group(
    country_ref: pd.DataFrame, grouping: str, group: str, exc_list: list = None
) -> list:
    """Returns the countries of a schema group.

    Args:
        country_ref (pd.DataFrame): A DataFrame containing the countries and region groupings.
        grouping (str): The regional schema you want to map
        group (str): The specific region you want to get the countries from.
        exc_list (list, optional): A flag to select the countries not in the group selected in `group`. Defaults to None.

    Returns:
        list: A list of countries either in `group` or not in `group` depending on the `exc_list` flag.
    """
    df_c = country_ref[["ISO-alpha3 code", grouping]].copy()
    code_list = (
        df_c.set_index([grouping, "ISO-alpha3 code"])
        .sort_index()
        .loc[group]
        .index.unique()
        .to_list()
    )
    if exc_list:
        exc_codes = [match_country(country) for country in exc_list]
        return list(set(code_list).difference(exc_codes))
    return code_list


def create_country_mapper(schema: str = "rmi", path: str = PKL_DATA_IMPORTS) -> dict:
    country_ref = read_pickle_folder(path, "country_ref", "df")
    mapper = {
        "Country": "country_name",
        "ISO-alpha3 code": "country_code",
        "M49 Code": "m49",
        "Region 1": "region",
        "Continent": "continent",
        "WSA Group Region": "wsa",
        "RMI Model Region": "rmi",
    }
    country_ref_c = country_ref.rename(mapper, axis=1)
    mapper = dict(zip(country_ref_c["country_code"], country_ref_c[schema]))
    mapper["TWN"] = "Japan, South Korea, and Taiwan"
    return mapper


def pick_random_country_from_region(
    country_df: pd.DataFrame, region: str, region_schema: str
) -> str:
    """Selects a random country from a country metadata dataframe based on a selected region_schema and region.

    Args:
        country_df (pd.DataFrame): The Country Metadata DataFrame.
        region (str): The region to select a random country from. Must exist in the region schema.
        region_schema (str): The schema of the region to select a random country from.

    Returns:
        str: The random country choice from a list.
    """
    country_list = get_countries_from_group(country_df, region_schema, region)
    return random.choice(country_list)


def pick_random_country_from_region_subset(plant_df: pd.DataFrame, region: str) -> str:
    """Picks a random country from a list of countries present in a Plant DataFrame for a specified region.

    Args:
        plant_df (pd.DataFrame): The plant_df containing the region and country metadata.
        region (str): The region to select a random country from. Must exist in the plant_df

    Returns:
        str: The random country choice from a list.
    """
    country_list = plant_df[plant_df[MAIN_REGIONAL_SCHEMA] == region][
        "country_code"
    ].unique()
    return random.choice(country_list)
