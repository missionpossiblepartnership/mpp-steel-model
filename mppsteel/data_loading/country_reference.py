"""Script that generates a country code mapper to be used to access country metadata"""

# For Data Manipulation
from collections import namedtuple

import pandas as pd
import pandera as pa
import pycountry

from tqdm.auto import tqdm as tqdma

# For logger and units dict
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.location_utility import CountryMetadata
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger
from mppsteel.model_config import PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE
from mppsteel.validation.data_import_tests import COUNTRY_REF_SCHEMA

# Create logger
logger = get_logger("Country Reference")


def official_name_getter(country_code: str) -> str:
    """Uses pycountry to get the official name of a country given an alpha3 country code.

    Args:
        country_code (str): An alpha3 country code

    Returns:
        str: The official name of the country.
    """
    match = pycountry.countries.get(alpha_3=country_code)
    match_attributes = dir(match)
    if "official_name" in match_attributes:
        return match.official_name
    return ""


def create_country_ref_dict(df: pd.DataFrame, country_metadata_nt: namedtuple) -> dict:
    """Creates a dictionary based on the a DataFrame with country metadata using
    alpha3 country code as the dict key.

    Args:
        df (pd.DataFrame): A dataframe with country metadata.

    Returns:
        dict: [A dictionary with country code as the dictionary key.
    """
    logger.info("Creating Country Reference dictionary")
    country_ref_dict = {}

    def value_mapper(row, enum_dict):
        country_ref_dict[row[enum_dict["country_code"]]] = country_metadata_nt(
            row[enum_dict["country_code"]],
            row[enum_dict["country"]],
            row[enum_dict["official_name"]],
            row[enum_dict["m49_code"]],
            row[enum_dict["region"]],
            row[enum_dict["continent"]],
            row[enum_dict["wsa_region"]],
            row[enum_dict["rmi_region"]],
        )

    tqdma.pandas(desc="Create County Ref Dict")
    enumerated_cols = enumerate_iterable(df.columns)
    df.progress_apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    return country_ref_dict


@pa.check_input(COUNTRY_REF_SCHEMA)
def country_df_formatter(df: pd.DataFrame) -> pd.DataFrame:
    """Formats a country metadata DataFrame.

    Args:
        df (pd.DataFrame): A DataFrame with country metadata.

    Returns:
        pd.DataFrame: A DataFrame with country metadata.
    """
    logger.info("Formatting country DataFrame")
    df_c = df.copy()
    df_c.rename(columns={"ISO-alpha3 code": "country_code"}, inplace=True)
    df_c["Official Country Name"] = df_c["country_code"].apply(
        lambda x: official_name_getter(x)
    )
    current_columns = [
        "country_code",
        "Country",
        "Official Country Name",
        "M49 Code",
        "Region 1",
        "Continent",
        "WSA Group Region",
        "RMI Model Region",
        "country_code",
    ]
    new_columns = [
        "country_code",
        "country",
        "official_name",
        "m49_code",
        "region",
        "continent",
        "wsa_region",
        "rmi_region",
    ]
    col_mapper = dict(zip(current_columns, new_columns))
    df_c.rename(mapper=col_mapper, axis=1, inplace=True)
    return df_c


def country_ref_getter(
    country_ref_dict: dict, country_code: str, ref: str = ""
) -> CountryMetadata:
    """A getter function to retrieve an attribute of a Country Metadata object.

    Args:
        country_ref_dict (dict): A country reference dict.
        country_code (str): An alpha3 country code
        ref (str, optional): A reference to the desired attribute. Defaults to ''.

    Returns:
        CountryMetadata: A country class object with the country data loaded as attributes.
    """
    if country_code in country_ref_dict:
        country_class = country_ref_dict[country_code]
    if ref in dir(country_class):
        return getattr(country_class, ref)
    return country_class


@timer_func
def create_country_ref(serialize: bool = False) -> dict:
    """Preprocesses the country data.

    Args:
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary of each country code to metadata.
    """
    logger.info("Creating final Country Reference dictionary")
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref")
    country_ref = country_df_formatter(country_ref)
    cr_dict = create_country_ref_dict(country_ref, CountryMetadata)

    if serialize:
        serialize_file(cr_dict, PKL_DATA_INTERMEDIATE, "country_reference_dict")
        return
    return cr_dict
