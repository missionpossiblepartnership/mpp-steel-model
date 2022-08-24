"""Function to create a steel plant class."""
import random
import pandas as pd

from tqdm.auto import tqdm as tqdma

# For logger and units dict
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.location_utility import (
    country_mapping_fixer,
    country_matcher,
    create_country_mapper,
)

from mppsteel.utility.file_handling_utility import (
    extract_data,
    read_pickle_folder,
    serialize_file,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    IMPORT_DATA_PATH,
    MODEL_YEAR_START,
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS,
    STEEL_PLANT_EARLIEST_START_DATE,
    STEEL_PLANT_LATEST_START_DATE,
)


logger = get_logger(__name__)

NEW_COLUMN_NAMES = [
    "plant_id",
    "plant_name",
    "country",
    "region",
    "status",
    "start_of_operation",
    "BFBOF_capacity",
    "DRIEAF_capacity",
    "EAF_capacity",
    "initial_technology",
    "primary_capacity",
]

NATURAL_GAS_COUNTRIES = [
    "ARE",
    "ARG",
    "AUS",
    "BLR",
    "CAN",
    "DZA",
    "GBR",
    "GEO",
    "IRQ",
    "IRN",
    "KWT",
    "LBY",
    "MEX",
    "OMN",
    "PER",
    "PHL",
    "QAT",
    "RUS",
    "SAU",
    "TUR",
    "UKR",
    "USA",
]


def steel_plant_formatter(df: pd.DataFrame) -> pd.DataFrame:
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
    df_c = df_c[
        (df_c["Status"] == "operating") & (df_c["Plant Technology in 2020"] != "")
    ].copy()
    df_c.dropna(subset=["Plant Technology in 2020"], inplace=True)
    cols_to_remove = [
        col
        for col in df_c.columns
        if any(
            substring in col for substring in ["Nominal", "Pure", "present", "Source"]
        )
    ]
    df_c.drop(cols_to_remove, axis=1, inplace=True)
    df_c = df_c.rename(mapper=dict(zip(df_c.columns, NEW_COLUMN_NAMES)), axis=1)
    df_c["country_code"] = ""
    df_c = extract_steel_plant_capacity(df_c)
    return df_c


def extract_steel_plant_capacity(df: pd.DataFrame) -> pd.DataFrame:
    """Creates new columns `plant_capacity` based on technology capacity columns.

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
        tech = row["initial_technology"]
        if tech == "EAF":
            return convert_to_float(row["EAF_capacity"])
        elif tech in {"Avg BF-BOF", "BAT BF-BOF"}:
            return convert_to_float(row["BFBOF_capacity"])
        elif tech in {"DRI-EAF", "DRI-EAF+CCUS"}:
            return convert_to_float(row["DRIEAF_capacity"])
        return 0

    tqdma.pandas(desc="Extract Steel Plant Capacity")
    df_c["plant_capacity"] = df_c.progress_apply(assign_plant_capacity, axis=1)
    return df_c


def get_plant_capacity(
    tech_capacities: dict,
    plant: str,
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
    df: pd.DataFrame,
    steel_plants: pd.DataFrame,
    plant_identifier: str,
    reverse: bool = False,
) -> pd.DataFrame:
    """Creates a column references with either the plant ID of the steel plant or the plant name based on the ID.

    Args:
        df (pd.DataFrame): The main DataFrame to map.
        steel_plants (pd.DataFrame): The steel plant reference DataFrame.
        plant_identifier (str): The plant identifier column in the DataFrame.
        reverse (bool, optional): Flag to create ID to name mapping rather than name to ID mapping. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with the newly added column.
    """
    plant_id_dict = dict(zip(steel_plants["plant_name"], steel_plants["plant_id"]))
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
    matching_dict = country_matcher(steel_plant_countries, "matches")
    df_c["country_code"] = df_c["country"].apply(lambda x: matching_dict[x])
    country_fixer_dict = {"North Korea": "PRK", "South Korea": "KOR"}
    df_c = country_mapping_fixer(df_c, "country", "country_code", country_fixer_dict)
    df_c["cheap_natural_gas"] = df_c["country_code"].apply(
        lambda country_code: 1 if country_code in NATURAL_GAS_COUNTRIES else 0
    )
    wsa_mapper = create_country_mapper("wsa")
    df_c["wsa_region"] = df_c["country_code"].apply(lambda x: wsa_mapper[x])
    rmi_mapper = create_country_mapper("rmi")
    df_c["rmi_region"] = df_c["country_code"].apply(lambda x: rmi_mapper[x])
    return df_c


class PlantStartYearAssignor:
    def __init__(self):
        self.years_assigned = []

    def initiate_model(self, plants_to_assign: int):
        self.model_start_year = MODEL_YEAR_START
        self.plants_to_assign = plants_to_assign

    def return_start_year(self):
        plant_start_year = (self.model_start_year) - (self.plants_to_assign)
        self.plants_to_assign -= 1
        self.years_assigned.append(plant_start_year)
        return plant_start_year


def convert_start_year(
    row,
    steel_plant_start_year_assignor: PlantStartYearAssignor,
    start_year_randomness: bool = False,
) -> int:
    """Converts a string or int year value to an int year value.
    If the initial year value is the value `unknown`, return a integer within a range set by configurable parameters.

    Args:
        year_value (str): A variable containing the initial year value.

    Returns:
        int: An integer containing the year value.
    """
    if row.start_of_operation == "unknown":
        if start_year_randomness:
            return random.randrange(
                STEEL_PLANT_EARLIEST_START_DATE, STEEL_PLANT_LATEST_START_DATE, 1
            )
        return steel_plant_start_year_assignor.return_start_year()
    return int(row.start_of_operation)


def create_active_check_col(row: pd.Series, year: int) -> bool:
    """Checks whether a plant should be considered active or not based on its status attribute or whether the current year is before its start of operation.

    Args:
        row (pd.Series): The row containing the metadata for the plant.
        year (int): The current year to check against the start_of_operation attribute in `row`.

    Returns:
        bool: A boolean value depending on the logic check.
    """
    return (
        row.status in ["operating", "new model plant"]
        and row.start_of_operation <= year
    )


@timer_func
def steel_plant_processor(
    scenario_dict: dict, serialize: bool = False, from_csv: bool = False
) -> pd.DataFrame:
    """Generates a fully preprocessed Steel Plant DataFrame.

    Args:
        serialize (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.
    Returns:
        pd.DataFrame: A DataFrame containing the preprocessed steel plants.
    """
    logger.info("Preprocessing the Steel Plant Data")
    if from_csv:
        steel_plants = extract_data(
            IMPORT_DATA_PATH, "Steel Plant Data Anon", "xlsx"
        )
    else:
        steel_plants = read_pickle_folder(PKL_DATA_IMPORTS, "steel_plants")
    steel_plants = steel_plant_formatter(steel_plants)
    steel_plants = apply_countries_to_steel_plants(steel_plants)

    plants_to_assign_start_years = len(
        steel_plants[steel_plants["start_of_operation"] == "unknown"]
    )
    steel_plant_start_year_assignor = PlantStartYearAssignor()
    steel_plant_start_year_assignor.initiate_model(plants_to_assign_start_years)
    start_year_randomness = scenario_dict["start_year_randomness"]
    steel_plants["start_of_operation"] = steel_plants.apply(
        convert_start_year,
        start_year_randomness=start_year_randomness,
        steel_plant_start_year_assignor=steel_plant_start_year_assignor,
        axis=1,
    )
    steel_plants["end_of_operation"] = ""
    steel_plants["active_check"] = steel_plants.apply(
        create_active_check_col, year=MODEL_YEAR_START, axis=1
    )
    if serialize:
        serialize_file(steel_plants, PKL_DATA_FORMATTED, "steel_plants_processed")
    return steel_plants
