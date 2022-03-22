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
    match_country,
    get_region_from_country_code,
)

from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file,
    extract_data,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE
from mppsteel.validation.data_import_tests import STEEL_PLANT_DATA_SCHEMA

# Create logger
logger = get_logger("Steel Plant Formatter")

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
    "parent",
    "country",
    "region",
    "coordinates",
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
    df_c = adjust_capacity_values(df_c)

    if remove_non_operating_plants:
        df_c = df_c[df_c["technology_in_2020"] != "Not operating"].reset_index(
            drop=True
        )

    return df_c


def extract_steel_plant_capacity(df: pd.DataFrame) -> pd.DataFrame:
    """Creates new columns `primary_capacity_2020` and `secondary_capacity_2020` based on
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
    df_c["primary_capacity_2020"] = 0
    capacity_cols = [
        "BFBOF_capacity",
        "EAF_capacity",
        "DRI_capacity",
        "DRIEAF_capacity",
    ]

    def value_mapper(row, enum_dict: dict):
        tech = row[enum_dict["technology_in_2020"]]
        for col in capacity_cols:
            if (col == "EAF_capacity") & (tech == "EAF"):
                row[enum_dict["primary_capacity_2020"]] = 0
            elif (col == "BFBOF_capacity") & (tech in ["Avg BF-BOF", "BAT BF-BOF"]):
                row[enum_dict["primary_capacity_2020"]] = convert_to_float(
                    row[enum_dict["BFBOF_capacity"]]
                )
            elif (col == "DRIEAF_capacity") & (tech in ["DRI-EAF", "DRI-EAF+CCUS"]):
                row[enum_dict["primary_capacity_2020"]] = convert_to_float(
                    row[enum_dict["DRIEAF_capacity"]]
                )
            elif (col == "DRI_capacity") & (tech == "DRI"):
                row[enum_dict["primary_capacity_2020"]] = convert_to_float(
                    row[enum_dict["DRI_capacity"]]
                )
            else:
                row[enum_dict["primary_capacity_2020"]] = 0
        return row

    tqdma.pandas(desc="Extract Steel Plant Capacity")
    enumerated_cols = enumerate_iterable(df_c.columns)
    df_c = df_c.progress_apply(
        value_mapper, enum_dict=enumerated_cols, axis=1, raw=True
    )
    df_c["secondary_capacity_2020"] = df_c["EAF_capacity"].apply(
        lambda x: convert_to_float(x)
    ) - df_c["DRIEAF_capacity"].apply(lambda x: convert_to_float(x))
    return df_c


def adjust_capacity_values(df: pd.DataFrame) -> pd.DataFrame:
    """Adjusts capacity values based on technology values for each plant.

    Args:
        df (pd.DataFrame): A DataFrame of the formatted steel plant data with newly added capacity columns.

    Returns:
        pd.DataFrame: The DataFrame with the amended capacity values.
    """
    df_c = df.copy()
    average_eaf_secondary_capacity = df_c[
        (df_c["secondary_capacity_2020"] != 0) & (df_c["technology_in_2020"] == "EAF")
    ]["secondary_capacity_2020"].mean()

    def value_mapper(row, enum_dict: dict, avg_eaf_value: float):
        # use average bfof values for primary if technology is BOF but primary capapcity is 0
        if (row[enum_dict["BFBOF_capacity"]] != 0) & (
            row[enum_dict["technology_in_2020"]] in ["Avg BF-BOF", "BAT BF-BOF"]
        ):
            row[enum_dict["primary_capacity_2020"]] = row[enum_dict["BFBOF_capacity"]]
        # use bfbof values for primary capacity if technology is eaf
        if (row[enum_dict["BFBOF_capacity"]] != 0) & (
            row[enum_dict["technology_in_2020"]] == "EAF"
        ):
            row[enum_dict["primary_capacity_2020"]] = row[enum_dict["BFBOF_capacity"]]
        # use average eaf values for secondary capacity if technology is eaf but secondary capapcity is currently unknown
        if (row[enum_dict["technology_in_2020"]] == "EAF") & (
            row[enum_dict["secondary_capacity_2020"]] == 0
        ):
            row[enum_dict["secondary_capacity_2020"]] = avg_eaf_value
        # if plant has DRI capacity and DRI-EAF capacity, make primary capacity the residue
        if (
            (abs(row[enum_dict["primary_capacity_2020"]]) > 0)
            & (abs(row[enum_dict["secondary_capacity_2020"]]) > 0)
            & (
                row[enum_dict["primary_capacity_2020"]]
                + row[enum_dict["secondary_capacity_2020"]]
                == 0
            )
        ):
            row[enum_dict["secondary_capacity_2020"]] = row[
                enum_dict["DRIEAF_capacity"]
            ]
            row[enum_dict["primary_capacity_2020"]] = (
                row[enum_dict["DRI_capacity"]] - row[enum_dict["DRIEAF_capacity"]]
            )
        return row

    tqdma.pandas(desc="Adjust Capacity Values")
    enumerated_cols = enumerate_iterable(df_c.columns)
    df_c = df_c.progress_apply(
        value_mapper,
        enum_dict=enumerated_cols,
        avg_eaf_value=average_eaf_secondary_capacity,
        axis=1,
        raw=True,
    )
    df_c["combined_capacity"] = (
        df_c["primary_capacity_2020"] + df_c["secondary_capacity_2020"]
    )

    def total_capacity_value_mapper(row):
        if row['technology_in_2020'] == 'EAF':
            return row['primary_capacity_2020'] + row['secondary_capacity_2020']
        return row['primary_capacity_2020']
    df_c["total_capacity"] = df_c.progress_apply(
        total_capacity_value_mapper,
        axis=1
    )
    return df_c


def create_plant_capacities_dict(plant_df: pd.DataFrame) -> dict:
    """Generates a dictionary that contains each steel plants primary and secondary capacity.

    Returns:
        dict: A diction containing the plant name as the key and the capacity values + technology in 2020 as dict values.
    """
    plant_capacities = {}
    for row in plant_df.itertuples():
        plant_name = row.plant_name
        row = {
            "2020_tech": row.technology_in_2020,
            "primary_capacity": row.primary_capacity_2020,
            "secondary_capacity": row.secondary_capacity_2020,
        }
        plant_capacities[plant_name] = row
    return plant_capacities


def calculate_primary_and_secondary(
    tech_capacities: dict, plant: str, tech: str
) -> float:
    """Sums primary and secondary capacity if the technology is EAF, otherwise returns the primary capacity value.

    Args:
        tech_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        plant (str): The plant name you want to calculate primary and secondary capacity for.
        tech (str): The technology you want to calculate primary and secondary capacity for.

    Returns:
        float: The capacity value given the paramaters inputted to the function.
    """
    if tech == "EAF":
        return (
            tech_capacities[plant]["secondary_capacity"]
            + tech_capacities[plant]["primary_capacity"]
        )
    return tech_capacities[plant]["primary_capacity"]


def total_plant_capacity(plant_cap_dict: dict) -> float:
    """Returns the total capacity of all plants listed in the `plant_cap_dict` dictionary.

    Args:
        plant_cap_dict (dict): A dictionary containing plant: capacity/inital tech key:value pairs.

    Returns:
        float: Float value of the summation of all plant capacities using the `calculate_primary_and_secondary` function.
    """
    all_capacities = [
        calculate_primary_and_secondary(
            plant_cap_dict, plant, plant_cap_dict[plant]["2020_tech"]
        )
        for plant in plant_cap_dict
    ]
    all_capacities = [x for x in all_capacities if str(x) != "nan"]
    return sum(all_capacities)


def map_plant_id_to_df(
    df: pd.DataFrame, plant_identifier: str, reverse: bool = False
) -> pd.DataFrame:
    """Creates a column references with either the plant ID of the steel plant or the plant name based on the ID.

    Args:
        df (pd.DataFrame): The DataFrame containing the mapping of steel plants to id.
        plant_identifier (str): The plant identifier column in the DataFrame.
        reverse (bool, optional): Flag to create ID to name mapping rather than name to ID mapping. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with the newly added column.
    """
    steel_plants = read_pickle_folder(PKL_DATA_INTERMEDIATE, "plant_result_df")
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
    steel_plant_formatted: pd.DataFrame,
) -> pd.DataFrame:
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
    country_reference_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    steel_plants["region"] = steel_plants["country_code"].apply(
        lambda x: get_region_from_country_code(x, "wsa_region", country_reference_dict)
    )
    steel_plants["rmi_region"] = steel_plants["country_code"].apply(
        lambda x: get_region_from_country_code(x, "rmi_region", country_reference_dict)
    )
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
        serialize_file(steel_plants, PKL_DATA_INTERMEDIATE, "steel_plants_processed")

    return steel_plants
