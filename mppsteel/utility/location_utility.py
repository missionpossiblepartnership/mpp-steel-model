"""Utility library for managing location"""

import itertools
from collections import namedtuple

import pandas as pd
import pycountry

from mppsteel.utility.log_utility import get_logger

from mppsteel.config.reference_lists import NEW_COUNTRY_COL_LIST, FILES_TO_REFRESH

logger = get_logger("Location Utility")


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


def official_country_attr_getter(country_code: str, attr: str = 'official_name') -> str:
    """Gets an attribute from a given country code. Using the pycountry library.

    Args:
        country_code (str): The official ISO Alpha-3 country code that you want to match.
        attr (str) : The attribute of the matched pycountry object. Defaults to 'official_name'.

    Returns:
        str: The attribute of the matched country object.
    """
    match = pycountry.countries.get(alpha_3=country_code)
    match_attributes = dir(match)
    if attr in match_attributes:
        return getattr(match, attr)
    return ""


def country_matcher(country_list: list, output_type: str = "all") -> dict:
    """Fuzzy matches a list of countries and creates a mapping of the country to ISO Alpha-3 name.
    The function produces a dictionary of mappings and also a dictionary of all unmapped countries.

    Args:
        country_list (list): The list of countries you would like to map.
        output_type (str, optional): The output you want - mapped dictionary, unmapped dictionary or both. Defaults to 'all'.

    Returns:
        dict: Dictionary(ies) based on the output_type parameters.
    """

    # Generate matched entries
    countries_dict = {country: match_country(country) for country in country_list}
    # Get reference of unmatched entries
    unmatched_dict = {
        item[0]: item[1] for item in countries_dict.items() if not item[1]
    }

    if output_type == "all":
        return countries_dict, unmatched_dict
    if output_type == "matches":
        return countries_dict
    if output_type == "nonmatches":
        return unmatched_dict


CountryMetadata = namedtuple("CountryMetadata", NEW_COUNTRY_COL_LIST)


def get_region_from_country_code(
    country_code: str, schema: str, country_ref_dict: dict
) -> str:
    """Gets a prespecified country region from a country code.

    Args:
        country_code (str): The country code you want to map to a region.
        schema (str): The region schema you want to use in your mapping.
        country_ref_dict (dict): The mapping of country codes to regions implemented as a CountryMetadata Class.

    Raises:
        AttributeError: If the schema inputted is not in the CountryMetaData class, then error is raised.

    Returns:
        str: The request region from the schema you have specified.
    """
    if country_code == "TWN":
        country_code = "CHN"  # !!! Not a political statement. Blame the lookup ref !!!!
    country_metadata_obj = country_ref_dict[country_code]
    if schema in dir(country_metadata_obj):
        return getattr(country_metadata_obj, schema)
    options = ["m49_code", "region", "continent", "wsa_region", "rmi_region"]
    raise AttributeError(
        f"Schema: {schema} is not an attribute of {country_code} CountryMetadata object. Choose from the following options: {options}"
    )


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
