"""Function to create a steel plant class."""
import pandas as pd
import pandera as pa

from tqdm.auto import tqdm as tqdma

# For logger and units dict
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.location_utility import (
    country_mapping_fixer,
    country_matcher,
    create_country_mapper,
)

from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import PKL_DATA_FORMATTED, PKL_DATA_IMPORTS
from mppsteel.validation.data_import_tests import STEEL_PLANT_DATA_SCHEMA

# Create logger
logger = get_logger(__name__)

COLUMNS_TO_REMOVE = [
    "Fill in data BF-BOF",
    "Fill in data EAF",
    "Fill in data DRI",
    "Estimated BF-BOF capacity (kt steel/y)",
    "Estimated EAF capacity (kt steel/y)",
    "Estimated DRI capacity (kt sponge iron/y)",
    "Estimated DRI-EAF capacity (kt steel/y)",
    "Source",
]

NEW_COLUMN_NAMES = [
    "plant_id",
    "plant_name",
    "country",
    "region",
    "status",
    "start_of_operation",
    "BFBOF_capacity",
    "EAF_capacity",
    "DRI_capacity",
    "DRIEAF_capacity",
    "abundant_res",
    "ccs_available",
    "cheap_natural_gas",
    "industrial_cluster",
    "technology_in_2020",
    "primary",
]


@pa.check_input(STEEL_PLANT_DATA_SCHEMA)
def steel_plant_formatter(
    df: pd.DataFrame, remove_non_operating_plants: bool = False
) -> pd.DataFrame:
    """Formats the steel plants data input. By dropping columns.
    Renaming columns, extracting steel plant capacity.
    Adjusting plant capacity values. Removing non-operating plants.

    Args:
        df (pd.DataFrame): The dataframe of the initial steel plant data.

    Returns:
        pd.DataFrame: A formatted dataframe with the transformations applied.
    """
    logger.info("Formatting the Steel Plant Data")
    df_c = df.copy()

    df_c.drop(COLUMNS_TO_REMOVE, axis=1, inplace=True)
    df_c = df_c.rename(mapper=dict(zip(df_c.columns, NEW_COLUMN_NAMES)), axis=1)
    df_c["country_code"] = ""
    df_c = extract_steel_plant_capacity(df_c)

    if remove_non_operating_plants:
        df_c = df_c[df_c["technology_in_2020"] != "Not operating"].reset_index(
            drop=True
        )

    return df_c


def extract_steel_plant_capacity(df: pd.DataFrame) -> pd.DataFrame:
    """Creates new columns `plant_capacity` based on
    technology capacity columns. 

    Args:
        df (pd.DataFrame): Formatted Steel Plant data.

    Returns:
        pd.DataFrame: The DataFrame with the new columns.
    """
    logger.info("Extracting Steel Plant Capacity")

    def convert_to_float(val) -> float:
        """Coerces all values to a float.

        Args:
            val ([type]): Any value passed - could be numerical or non-numerical

        Returns:
            float: A float value. If non-numerical returns a zero float value.
        """
        if isinstance(val, float):
            return val
        try:
            return float(val)
        except:    
            return float(0)

    df_c = df.copy()

    def assign_plant_capacity(row):
        tech = row["technology_in_2020"]
        if tech == "EAF":
            return convert_to_float(row["EAF_capacity"])
        elif tech in {"Avg BF-BOF", "BAT BF-BOF"}:
            return convert_to_float(row["BFBOF_capacity"])
        elif tech in {"DRI-EAF", "DRI-EAF+CCUS"}:
            return convert_to_float(row["DRIEAF_capacity"])
        return 0

    tqdma.pandas(desc="Extract Steel Plant Capacity")
    df_c['plant_capacity'] = df_c.progress_apply(assign_plant_capacity, axis=1)
    return df_c


def get_plant_capacity(
    tech_capacities: dict, plant: str,
) -> float:
    """Returns the plant capacity value.

    Args:
        tech_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        plant (str): The plant name you want to calculate capacity for.

    Returns:
        float: The capacity value given the paramaters inputted to the function.
    """
    return tech_capacities[plant]


def map_plant_id_to_df(
    df: pd.DataFrame, steel_plants: pd.DataFrame, plant_identifier: str, reverse: bool = False
) -> pd.DataFrame:
    """Creates a column references with either the plant ID of the steel plant or the plant name based on the ID.

    Args:
        df (pd.DataFrame): The DataFrame containing the mapping of steel plants to id.
        plant_identifier (str): The plant identifier column in the DataFrame.
        reverse (bool, optional): Flag to create ID to name mapping rather than name to ID mapping. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with the newly added column.
    """
    plant_id_dict = dict(
        zip(steel_plants["plant_name"], steel_plants["plant_id"])
    )
    df_c = df.copy()
    if reverse:
        id_plant_dict = {v: k for k, v in plant_id_dict.items()}
        df_c["plant_name"] = df_c[plant_identifier].apply(lambda x: id_plant_dict[x])
    df_c["plant_id"] = df_c[plant_identifier].apply(lambda x: plant_id_dict[x])
    return df_c


def apply_countries_to_steel_plants(
    steel_plant_formatted: pd.DataFrame) -> pd.DataFrame:
    """Maps a country codes and region column to the Steel Plants.

    Args:
        steel_plant_formatted (pd.DataFrame): A DataFrame of the formatted steel plants.

    Returns:
        pd.DataFrame: A DataFrame with the newly added country code and region.
    """
    logger.info("Applying Country Data to Steel Plants")
    df_c = steel_plant_formatted.copy()
    steel_plant_countries = df_c["country"].unique().tolist()
    matching_dict, unmatched_dict = country_matcher(steel_plant_countries)
    logger.info(
        "- Applying the codes of the matched countries to the steel plant column"
    )
    df_c["country_code"] = df_c["country"].apply(lambda x: matching_dict[x])
    country_fixer_dict = {"Korea, North": "PRK"}
    steel_plants = country_mapping_fixer(
        df_c, "country", "country_code", country_fixer_dict
    )
    wsa_mapper = create_country_mapper('wsa')
    steel_plants["wsa_region"] = steel_plants["country_code"].apply(lambda x: wsa_mapper[x])
    rmi_mapper = create_country_mapper('rmi')
    steel_plants["rmi_region"] = steel_plants["country_code"].apply(lambda x: rmi_mapper[x])
    return steel_plants


@timer_func
def steel_plant_processor(
    serialize: bool = False, remove_non_operating_plants: bool = False
) -> pd.DataFrame:
    """Generates a fully preprocessed Steel Plant DataFrame.

    Args:
        serialize (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.
        remove_non_operating_plants (bool, optional): Option to remove non_operating plants from the reference.  Defaults to False.
    Returns:
        pd.DataFrame: A DataFrame containing the preprocessed steel plants.
    """
    logger.info("Preprocessing the Steel Plant Data")
    steel_plants = read_pickle_folder(PKL_DATA_IMPORTS, "steel_plants")
    steel_plants = steel_plant_formatter(steel_plants, remove_non_operating_plants)
    steel_plants = apply_countries_to_steel_plants(steel_plants)

    if serialize:
        serialize_file(steel_plants, PKL_DATA_FORMATTED, "steel_plants_processed")

    return steel_plants
