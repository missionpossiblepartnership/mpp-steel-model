"""Function to create a steel plant class."""
import pandas as pd

from mppsteel.data_loading.country_reference import match_country

# For logger and units dict
from mppsteel.utility.utils import (
    get_logger,
    timer_func,
    read_pickle_folder,
    serialize_file,
    country_mapping_fixer,
    country_matcher
)

from mppsteel.model_config import PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE

# Create logger
logger = get_logger("Steel Plant Formatter")

def steel_plant_formatter(df: pd.DataFrame, remove_non_operating_plants: bool = False) -> pd.DataFrame:
    """Formats the steel plants data input.

    Args:
        df (pd.DataFrame): The dataframe of the loaded steel plant data.

    Returns:
        pd.DataFrame: A formatted dataframe.
    """
    logger.info("Formatting the Steel Plant Data")
    df_c = df.copy()
    steel_plant_cols_to_remove = [
        'Fill in data BF-BOF',
        'Fill in data EAF', 'Fill in data DRI',
        'Estimated BF-BOF capacity (kt steel/y)',
        'Estimated EAF capacity (kt steel/y)',
        'Estimated DRI capacity (kt sponge iron/y)',
        'Estimated DRI-EAF capacity (kt steel/y)',
        'Source', 'Excel Tab'
    ]
    df_c.drop(steel_plant_cols_to_remove, axis=1, inplace = True)
    new_steel_plant_cols = [
        'plant_name', 'parent', 'country', 'region', 'coordinates', 'status', 'start_of_operation',
        'BFBOF_capacity', 'EAF_capacity', 'DRI_capacity', 'DRIEAF_capacity', 'abundant_res',
        'ccs_available', 'cheap_natural_gas', 'industrial_cluster', 'technology_in_2020']
    df_c = df_c.rename(mapper=dict(zip(df_c.columns, new_steel_plant_cols)), axis=1)

    df_c["country_code"] = ""

    df_c = extract_steel_plant_capacity(df_c)

    if remove_non_operating_plants:
        df_c = df_c[df_c['technology_in_2020'] != 'Not operating'].reset_index(drop=True)

    return df_c


def extract_steel_plant_capacity(df: pd.DataFrame):
    logger.info("Extracting Steel Plant Capacity")
    def convert_to_float(val):
        try:
            return float(val)
        except:
            if isinstance(val, float):
                return val
        return 0
    df_c = df.copy()
    capacity_cols = ['BFBOF_capacity', 'EAF_capacity', 'DRI_capacity', 'DRIEAF_capacity']
    for row in df_c.itertuples():
        tech = row.technology_in_2020
        for col in capacity_cols:
            if col == 'EAF_capacity':
                if tech == 'EAF':
                    value = convert_to_float(row.EAF_capacity)
                    df_c.loc[row.Index, 'primary_capacity_2020'] = 0
            elif col == 'BFBOF_capacity':
                if tech in ['Avg BF-BOF', 'BAT BF-BOF']:
                    value = convert_to_float(row.BFBOF_capacity)
                    df_c.loc[row.Index, 'primary_capacity_2020'] = value
            elif col == 'DRIEAF_capacity':
                if tech in ['DRI-EAF', 'DRI-EAF+CCUS']:
                    value = convert_to_float(row.DRIEAF_capacity)
                    df_c.loc[row.Index, 'primary_capacity_2020'] = value
            elif col == 'DRI_capacity':
                if tech == 'DRI':
                    value = convert_to_float(row.DRI_capacity)
                    df_c.loc[row.Index, 'primary_capacity_2020'] = value
            else:
                df_c.loc[row.Index, 'primary_capacity_2020'] = 0
    df_c['secondary_capacity_2020'] = df_c['EAF_capacity'].apply(lambda x: convert_to_float(x)) - df_c['DRIEAF_capacity'].apply(lambda x: convert_to_float(x)) 
    return df_c

def apply_countries_to_steel_plants(steel_plant_formatted: pd.DataFrame):
    logger.info("Applying Country Data to Steel Plants")
    df_c = steel_plant_formatted.copy()
    steel_plant_countries = df_c["country"].unique().tolist()
    matching_dict, unmatched_dict = country_matcher(steel_plant_countries)
    logger.info(
        "- Applying the codes of the matched countries to the steel plant column"
    )
    df_c["country_code"] = df_c["country"].apply(
        lambda x: matching_dict[x]
    )

    country_fixer_dict = {"Korea, North": "PRK"}

    steel_plants = country_mapping_fixer(df_c, "country", "country_code", country_fixer_dict)

    country_reference_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'country_reference_dict', 'df')

    steel_plants['region'] = steel_plants['country_code'].apply(lambda x: match_country(x, country_reference_dict))

    return steel_plants

@timer_func
def steel_plant_processor(
    serialize_only: bool = False,
    remove_non_operating_plants: bool = False) -> pd.DataFrame:
    """Generates the preprocessed Steel plant DataFrame.

    Args:
        serialize_only (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A dataframe containing the preprocessed steel plants.
    """
    logger.info("Preprocessing the Steel Plant Data")
    steel_plants = read_pickle_folder(PKL_DATA_IMPORTS, "steel_plants", remove_non_operating_plants)
    steel_plants = steel_plant_formatter(steel_plants)
    steel_plants = apply_countries_to_steel_plants(steel_plants)

    if serialize_only:
        serialize_file(steel_plants, PKL_DATA_INTERMEDIATE, "steel_plants_processed")
        return

    return steel_plants
