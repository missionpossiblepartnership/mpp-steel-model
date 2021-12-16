"""Function to create a steel plant class."""
import pandas as pd

# For logger and units dict
from mppsteel.utility.utils import (
    get_logger,
    read_pickle_folder,
    serialize_file,
    country_mapping_fixer,
    country_matcher,
)

from mppsteel.model_config import PKL_FOLDER


# Create logger
logger = get_logger("Steel Plant Class")


def steel_plant_formatter(df: pd.DataFrame) -> pd.DataFrame:
    """Formats the steel plants data input.

    Args:
        df (pd.DataFrame): The dataframe of the loaded steel plant data.

    Returns:
        pd.DataFrame: A formatted dataframe.
    """
    logger.info("Preprocessing the steel plant data")
    df_c = df.copy()
    columns_of_interest = [
        "Plant name (English)",
        "Parent",
        "Country",
        "Coordinates",
        "Status",
        "Start of operation",
        "Plant Technology in 2020",
    ]

    new_column_names = [
        "plant_name",
        "parent",
        "country",
        "coordinates",
        "status",
        "start_of_operation",
        "plant_technology_2020",
    ]

    df_c = df_c[columns_of_interest]

    df_c.rename(columns=dict(zip(columns_of_interest, new_column_names)), inplace=True)

    df_c["country_code"] = ""

    return df_c


def steel_plant_preprocessor(serialize_only: bool = False) -> pd.DataFrame:
    """Generates the preprocessed Steel plant DataFrame.

    Args:
        serialize_only (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A dataframe containing the preprocessed steel plants.
    """
    steel_plants = read_pickle_folder(PKL_FOLDER, "steel_plants")
    steel_plants = steel_plant_formatter(steel_plants)
    steel_plant_countries = steel_plants["country"].unique().tolist()
    matching_dict, unmatched_dict = country_matcher(steel_plant_countries)
    logger.info(
        "- Applying the codes of the matched countries to the steel plant column"
    )
    steel_plants["country_code"] = steel_plants["country"].apply(
        lambda x: matching_dict[x]
    )

    country_fixer_dict = {"Korea, North": "PRK"}

    steel_plants = country_mapping_fixer(
        steel_plants, "country", "country_code", country_fixer_dict
    )

    if serialize_only:
        serialize_file(steel_plants, PKL_FOLDER, "steel_plants_processed")
        return
    return steel_plants
