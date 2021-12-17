"""Module for formatting and producing getter functions for natural resources"""

# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from mppsteel.utility.utils import (
    get_logger,
    read_pickle_folder,
    country_mapping_fixer,
    country_matcher,
    serialize_file,
    timer_func,
)

from mppsteel.model_config import PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE, PKL_FOLDER

# Create logger
logger = get_logger("Natural Resource")


def normalise_data(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))


def natural_gas_formatter(df: pd.DataFrame, scaled: bool = False) -> pd.DataFrame:
    """Preprocesses the Natural Gas DataFrame.

    Args:
        df (pd.DataFrame): The original DataFrame for natural gas.
        scaled (bool, optional): Flag that turns value columns into scaled values between 0 and 1. Defaults to False.

    Returns:
        pd.DataFrame: The formatted dataframe for natural gas.
    """
    logger.info("Formatting the Natural Gas DataFrame")
    df_c = df.copy()
    df_c.drop(0, inplace=True)
    df_c.drop("API", axis="columns", inplace=True)
    df_c.rename(columns={"Unnamed: 1": "Country"}, inplace=True)
    df_c["Country"] = df_c["Country"].apply(lambda x: x.lstrip())

    value_columns = df_c.columns[1:]

    for column in value_columns:
        df_c[column] = df_c[column].replace("--", 0)
        df_c[column] = df_c[column].fillna(0)
        df_c[column] = df_c[column].astype(float)

    if scaled:
        logger.info("--- Scaling Natural Gas values")
        for column in value_columns:
            df_c.loc[df_c["Country"] == "World", column] = 0
            df_c[column] = normalise_data(df_c[column].values)

    df_c = df_c.melt(id_vars=["Country"], var_name="Year")
    df_c["Year"] = df_c["Year"].astype(int)
    df_c["value"] = df_c["value"].astype(float)
    return df_c


def wind_formatter(df: pd.DataFrame, scaled: bool = False) -> pd.DataFrame:
    """Preprocesses the Wind DataFrame.

    Args:
        df (pd.DataFrame): The original DataFrame for wind.

    Returns:
        pd.DataFrame: The formatted dataframe for wind.
    """
    logger.info("Formatting the Wind DataFrame")
    df_c = df.copy()
    df_c.drop(columns=["GeoName", "Sovereign", "Territory"], inplace=True)
    df_c.rename(
        columns={
            "Potential Fixed Foundations [GW]": "Fixed",
            "Potential Floating Foundations [GW]": "Floating",
        },
        inplace=True,
    )

    for column in ["Fixed", "Floating"]:
        df_c[column] = df_c[column].fillna(0)

    if scaled:
        logger.info("--- Scaling Wind values")
        for column in ["Fixed", "Floating"]:
            df_c[column] = normalise_data(df_c[column].values)

    df_c = df_c.melt(id_vars=["country_code"], var_name="wind_potential")
    df_c["value"] = df_c["value"].astype(float)
    df_c.set_index(["country_code", "wind_potential"], inplace=True)
    df_c.sort_index(inplace=True)
    return df_c


def solar_formatter(df: pd.DataFrame, scaled: bool = False) -> pd.DataFrame:
    """Preprocesses the Solar DataFrame.

    Args:
        df (pd.DataFrame): The original DataFrame for solar.
        scaled (bool, optional): Flag that turns value columns into scaled values between 0 and 1. Defaults to False.

    Returns:
        pd.DataFrame: The formatted dataframe for solar.
    """

    def column_mapper_generator(original_names: list, new_names: list) -> dict:
        column_mapper = {}
        for ind in range(len(original_names)):
            column_mapper[original_names[ind]] = new_names[ind]
        return column_mapper

    economic_columns_of_interest = [
        "iso_a3",
        "country_or_region",
        "total_population_2018",
        "total_area_2018",
        "human_development_index_2017",
        "gross_domestic_product_(usd_per_capita)_2018",
    ]

    solar_columns_of_interest = [
        "iso_a3",
        "country_or_region",
        "average_theoretical_potential_(ghi_kwh/m2/day)_long-term",
        "average_practical_potential_(pvout_level_1_kwh/kwp/day)_long-term",
        "average_economic_potential_(lcoe_usd/kwh)_2018",
        "average_pv_seasonality_index_long-term",
        "pv_equivalent_area_(%_of_total_area)_long-term",
    ]

    value_columns = [
        "theoretical_potential",
        "practical_potential",
        "economic_potential",
        "seasonality_index",
        "equivalent_area",
    ]

    new_solar_column_names = ["country_code", "country"] + value_columns

    column_units = {
        "theoretical_potential": "ghi kwh / m2 / day",
        "practical_potential": "kwh / kwp / day",
        "economic_potential": "lcoe usd / kwh",
        "seasonality_index": "unintless",
        "equivalent_area": "percentage",
    }

    df_c = df.copy()
    df_c.columns = [
        col.lower().replace(" ", "_").replace("\n", "").replace(",", "")
        for col in df_c.columns
    ]
    col_map = column_mapper_generator(solar_columns_of_interest, new_solar_column_names)
    economic_factors = df_c[economic_columns_of_interest].fillna(0)
    df_c = df_c[solar_columns_of_interest].fillna(0)
    df_c.rename(columns=col_map, inplace=True)

    if scaled:
        logger.info("--- Scaling Solar values")
        for column in value_columns:
            df_c[column] = normalise_data(df_c[column].values)

    df_c = df_c.melt(id_vars=["country_code", "country"], var_name="metric")
    df_c["unit"] = ""
    df_c["unit"] = df_c["metric"].apply(lambda x: column_units[x])
    df_c.drop(columns=["country", "unit"], inplace=True)
    df_c.set_index(["country_code", "metric"], inplace=True)
    df_c.sort_index(inplace=True)
    return df_c


# GETTER FUNCTIONS
def solar_getter(df: pd.DataFrame, country_code: str, metric: str = ""):
    """The getter function for the solar data.

    Args:
        df (pd.DataFrame): A preprocessed dataframe for solar.
        country_code (str): A country code contained the alpha-3
        metric (str, optional): The metric of interest for solar. Defaults to ''.

    Returns:
        [type]: A value if a metric is selected, else a dictionary of values if no metric is selected.
    """
    df_c = df.copy()
    if metric:
        logger.info(
            f"Getting the value for Solar | Country: {country_code} - Metric: {metric}"
        )
        value = df_c.loc[country_code, metric]["value"]
        return value
    values = df_c.loc[country_code]["value"]
    logger.info(f"Returning the values for Solar | Country: {country_code}")
    return {
        "theoretical_potential": values[4],
        "practical_potential": values[2],
        "economic_potential": values[0],
        "seasonality_index": values[3],
        "equivalent_area": values[1],
    }


def wind_getter(df: pd.DataFrame, country_code: str, metric: str = ""):
    """The getter function for the wind data.

    Args:
        df (pd.DataFrame): A preprocessed dataframe for wind.
        country_code (str): A country code contained the alpha-3
        metric (str, optional): The metric of interest for wind. Defaults to ''.

    Returns:
        [type]: A value if a metric is selected, else a dictionary of values if no metric is selected.
    """
    df_c = df.copy()
    if metric:
        logger.info(
            f"Getting the value for Wind | Country: {country_code} - Metric: {metric}"
        )
        value = df_c.loc[country_code, metric]["value"][0]
        return value
    values = df_c.loc[country_code]["value"]
    logger.info(f"Returning the values for Wind | Country: {country_code}")
    return {"fixed": values[0], "floating": values[1]}


def natural_gas_getter(df: pd.DataFrame, country_code: str, year: int = 0):
    """The getter function for the natural gas data.

    Args:
        df (pd.DataFrame): A preprocessed dataframe for natural gas.
        country_code (str): A country code contained the alpha-3
        year (str, optional): The year of interest for natural gas. Defaults to ''.

    Returns:
        [type]: A value if a metric is selected, else a dictionary of values if no metric is selected.
    """
    if year == 0:
        # Need to fix this to generate summaries
        logger.info(f"Returning the values for Natural Gas | Country: {country_code}")

        year_index = [year_val[1] for year_val in df.index]
        values = df.loc[country_code]["value"]
        return dict(zip(year_index, values))
    logger.info(
        f"Getting the value for Natural Gas | Country: {country_code} - Year: {year}"
    )
    return df.loc[country_code, year]["value"].values[0]


NG_FIXED_COUNTRIES = {
    "World": "World",
    "Burma": "MMR",
    "Congo-Brazzaville": "COG",
    "Congo-Kinshasa": "COD",
    "Côte d’Ivoire": "CIV",
    "Former Czechoslovakia": "CZE",
    "Former Serbia and Montenegro": "SCG",
    "Former U.S.S.R.": "RUS",
    "Former Yugoslavia": "SRB",
    "Gambia, The": "GMB",
    "Germany, East": "GER",
    "Germany, West": "GER",
    "Hawaiian Trade Zone": "USA",
    "Laos": "LAO",
    "Macau": "MAC",
    "Netherlands Antilles": "ANT",
    "North Korea": "PRK",
    "Palestinian Territories": "PSE",
    "Saint Vincent/Grenadines": "VCT",
    "South Korea": "KOR",
    "U.S. Pacific Islands": "USA",
    "U.S. Territories": "USA",
    "U.S. Virgin Islands": "VIR",
}

@timer_func
def natural_resource_preprocessor(serialize_only: bool = False) -> dict:
    """Preprocesses dataframe for wind, solar and natural gas.

    Args:
        serialize_only (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary with keys for 'solar', 'wind' and 'natrual gas' and dataframes for the respective values.
    """

    # SOLAR
    solar_df = read_pickle_folder(PKL_DATA_IMPORTS, "solar")
    solar_df = solar_formatter(solar_df, scaled=True)

    # WIND
    wind_df = read_pickle_folder(PKL_DATA_IMPORTS, "wind")
    territory_fixer = {
        "Overlapping claim Western Saharan Exclusive Economic Zone": "Western Sahara"
    }
    sovereign_fixer = {
        "Overlapping claim Western Saharan Exclusive Economic Zone": "Morocco"
    }
    wind_df.loc[
        wind_df["GeoName"] == list(territory_fixer.keys())[0], "Territory"
    ] = list(territory_fixer.values())[0]
    wind_df.loc[
        wind_df["GeoName"] == list(sovereign_fixer.keys())[0], "Sovereign"
    ] = list(sovereign_fixer.values())[0]
    wind_countries = wind_df["Sovereign"].unique().tolist()
    wind_matching_dict, wind_unmatched_dict = country_matcher(wind_countries)
    wind_df["country_code"] = ""
    wind_df["country_code"] = wind_df["Sovereign"].apply(
        lambda x: wind_matching_dict[x]
    )
    wind_country_fixer_dict = {
        "South Korea": "KOR",
        "North Korea": "RPK",
        "Cape Verde": "CPV",
    }
    country_mapping_fixer(wind_df, "Sovereign", "country_code", wind_country_fixer_dict)
    wind_df = wind_formatter(wind_df, scaled=True)

    # NATURAL GAS
    natural_gas_df = read_pickle_folder(PKL_DATA_IMPORTS, "natural_gas")
    natural_gas_df = natural_gas_formatter(natural_gas_df, scaled=True)
    natural_gas_countries = natural_gas_df["Country"].unique().tolist()
    ng_matching_dict, unmatched_dict = country_matcher(natural_gas_countries)
    natural_gas_df["country_code"] = ""
    natural_gas_df["country_code"] = natural_gas_df["Country"].apply(
        lambda x: ng_matching_dict[x]
    )
    natural_gas_df = country_mapping_fixer(
        natural_gas_df, "Country", "country_code", NG_FIXED_COUNTRIES
    )
    natural_gas_df = (
        natural_gas_df.drop("Country", axis=1)
        .set_index(["country_code", "Year"])
        .sort_index()
    )

    if serialize_only:
        # Serialize timeseries
        serialize_file(wind_df, PKL_DATA_INTERMEDIATE, "wind_processed")
        serialize_file(solar_df, PKL_DATA_INTERMEDIATE, "solar_processed")
        serialize_file(natural_gas_df, PKL_DATA_INTERMEDIATE, "natural_gas_processed")
        return
    return {"solar": solar_df, "wind": wind_df, "natural_gas": natural_gas_df}
