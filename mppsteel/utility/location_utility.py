"""Utility library for managing location"""

from collections import namedtuple

import pandas as pd
import pycountry

from mppsteel.utility.log_utility import get_logger

from mppsteel.utility.reference_lists import NEW_COUNTRY_COL_LIST, FILES_TO_REFRESH

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
        pd.DataFrame: An amended dataframe with the code mappings fixed
    """
    df_c = df.copy()

    logger.info(f"- Fixing the country codes for {list(country_to_code_dict.keys())}")
    for item in list(country_to_code_dict.items()):
        df_c.loc[df_c[country_colname] == item[0], country_code_colname] = item[1]
    return df_c


def match_country(country: str) -> str:
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


def official_country_name_getter(country_code: str) -> str:
    match = pycountry.countries.get(alpha_3=country_code)
    match_attributes = dir(match)
    if "official_name" in match_attributes:
        return match.official_name
    return ""


CountryMetadata = namedtuple("CountryMetadata", NEW_COUNTRY_COL_LIST)


def get_region_from_country_code(
    country_code: str, schema: str, country_ref_dict: dict
) -> str:
    if country_code == "TWN":
        country_code = "CHN"  # !!! Not a political statement. Blame the lookup ref !!!!
    country_metadata_obj = country_ref_dict[country_code]
    if schema in dir(country_metadata_obj):
        return getattr(country_metadata_obj, schema)
    options = ["m49_code", "region", "continent", "wsa_region", "rmi_region"]
    raise AttributeError(
        f"Schema: {schema} is not an attribute of {country_code} CountryMetadata object. Choose from the following options: {options}"
    )
