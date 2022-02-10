"""Splits a DataFrame based on specified regions."""

# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.utility.log_utility import get_logger

from mppsteel.model_config import PKL_DATA_INTERMEDIATE

# Create logger
logger = get_logger("Regional Splitter")


def create_region_dict_generator(
    country_ref: dict,
    region_type: str,
    split_list: list = None,
    split_type: str = "equal",
) -> dict:
    """Creates a regional split based on a variety of calculations

    Args:
        country_ref (dict): A Dictionary containing the CountryMetadata objects
        region_type (str): The regional split you want to creat categories for.
        split_list (list, optional): A predefined split of the regions.
        The length of the list must be equal to the number of regions defined in 'region_type'.
        Defaults to None.
        split_type (str, optional): Defines the distribution a split should take. Defaults to 'equal'.

    Returns:
        dict: A dictionary containing each region as a key, and the allocated split as the value.
    """
    logger.info(f"-- Creating a regional split for {region_type}")
    region_list = []

    def extract_unique_attrs(attr_name: str):
        ref_list = [getattr(country_ref[key], attr_name) for key in country_ref]
        return list(set(ref_list))

    if region_type == "continent":
        region_list = extract_unique_attrs("continent")
    if region_type == "rmi":
        region_list = extract_unique_attrs("rmi_region")
    if region_type == "wsa":
        region_list = extract_unique_attrs("wsa_region")
    if region_type == "region":
        region_list = extract_unique_attrs("region")
    if split_list:
        logger.info(f"-- Using a predefined distributional split")
        return dict(zip(region_list, split_list))
    new_array = np.array(0)
    if split_type == "equal":
        logger.info(f"-- Creating an equal split")
        new_array = np.repeat(1 / len(region_list), len(region_list))
    if split_type == "random":
        logger.info(f"-- Creating a random split")
        new_array = np.random.dirichlet(np.ones(len(region_list)), size=1)[0]
    return dict(zip(region_list, new_array))


def split_regions(
    df: pd.DataFrame,
    regional_splits: dict,
    value_column: str = "value",
    drop_global: bool = False,
) -> pd.DataFrame:
    """Splits a DataFrame's 'value' column based on a regional split dictionary.

    Args:
        df (pd.DataFrame): A DataFrame containing the global timeseries.
        regional_splits (dict): A regional split dictionary with proportions on how to assign the split.
        value_column (str): The name of the column with the values to split.
        drop_global (bool, optional): Flag to determine whether to drop the global column. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with a regional split of the global values.
    """
    logger.info('-- Splitting the global timeseries database')
    df_c = df.copy()
    df_c["region"] = "Global"
    df_list = []
    for country, value in regional_splits():
        df_n = df_c.copy()
        df_n["region"] = country
        df_n[value_column] = df_n[value_column].apply(lambda x: x * value)
        df_list.append(df_n)
    new_dfs = pd.concat(df_list)
    if drop_global:
        logger.info('-- Dropping the global values')
        return new_dfs.reset_index(drop=True)
    return pd.concat([df_c, new_dfs]).reset_index(drop=True)


def create_regional_split(df: pd.DataFrame, region_type: str, split_type: str):
    country_ref_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    region_split_dict = create_region_dict_generator(
        country_ref_dict, region_type=region_type, split_type=split_type
    )
    return split_regions(df, region_split_dict)
