"""Script that generates a country code mapper to be used to access country metadata"""

# For Data Manipulation
import pandas as pd
import pandera as pa
import pycountry

# For logger and units dict
from mppsteel.utility.log_utility import get_logger
from mppsteel.data_validation.data_import_tests import COUNTRY_REF_SCHEMA

# Create logger
logger = get_logger(__name__)


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
