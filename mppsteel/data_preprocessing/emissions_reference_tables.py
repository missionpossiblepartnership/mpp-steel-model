"""Script that creates the price and emissions tables."""

# For Data Manipulation
import itertools
from typing import Tuple, Union
import pandas as pd

from tqdm import tqdm

# For logger and units dict
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import move_cols_to_front
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    return_pkl_paths,
    serialize_file,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.config.model_config import (
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS,
    TON_TO_KILOGRAM_FACTOR,
)
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST


logger = get_logger(__name__)


def generate_s1_s3_emissions(
    business_cases: pd.DataFrame,
    s1_emissivity_factors: pd.DataFrame,
    s3_emissivity_factors: pd.DataFrame

) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Creates a DataFrame with emissivity for S1, S2 & S3 for each technology.
    Multiples the emissivity values by the standardized business cases.

    Args:
        df (pd.DataFrame): The standardised business cases DataFrame.
        s1_emissivity_factors (pd.DataFrame, optional): Emissions Factors for S1. Defaults to None.
        s3_emissivity_factors (pd.DataFrame, optional): Emissions Factors for S3. Defaults to None.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A DataFrame of the emissivity per scope.
    """

    # Create resources reference list
    s1_emissivity_resources = s1_emissivity_factors["Metric"].unique().tolist()
    s3_emissions_resources = s3_emissivity_factors["Fuel"].unique().tolist()
    s1_emissivity_factors.set_index(["Metric"], inplace=True)
    s3_emissivity_factors.set_index(["Fuel", "Year"], inplace=True)
    s1_emissivity_factors["Value"] = s1_emissivity_factors["Value"]

    # Create a year range
    year_range = MODEL_YEAR_RANGE

    df_list = []

    logger.info("calculating emissions reference tables")

    def s1_s2_emissions_mapper(row):
        if row.material_category in s1_emissivity_resources:  # tCO2 / GJ
            # S1 emissions without process emissions or CCS/CCU
            s1_value = s1_emissivity_factors.loc[row.material_category]["Value"]
            row["S1"] = row.value * (s1_value / TON_TO_KILOGRAM_FACTOR)
        else:
            row["S1"] = 0
        if row.material_category in s3_emissions_resources:  # t / GJ or t / t
            emission_unit_value = s3_emissivity_factors.loc[
                row.material_category, year
            ]["value"]
            if row.material_category == "BF slag":
                emission_unit_value = emission_unit_value * -1
            row["S3"] = row.value * emission_unit_value
        else:
            row["S3"] = 0

        return row

    for year in tqdm(
        year_range, total=len(year_range), desc="Emissions Reference Table"
    ):
        df_c = business_cases.copy()
        df_c["year"] = year
        df_c["S1"] = ""
        df_c["S2"] = ""
        df_c["S3"] = ""
        df_c = df_c.apply(
            s1_s2_emissions_mapper,
            axis=1,
        )
        df_list.append(df_c)
    combined_df = pd.concat(df_list)
    combined_df.drop(labels=["value"], axis=1, inplace=True)
    combined_df = combined_df.melt(
        id_vars=["technology", "year", "material_category", "unit"],
        var_name=["scope"],
        value_name="emissions",
    )
    return combined_df.reset_index(drop=True).copy()


def scope1_emissions_calculator(
    s1_emissions: pd.DataFrame, business_case_ref: dict
) -> pd.DataFrame:
    """Combines regular emissions with process emissions and ccs/ccu emissions to get a complete
    emissions reference for a list of technologies.

    Args:
        s1_emissions (pd.DataFrame): The standardised business cases DataFrame.
        business_case_ref (list): The busienss cases reference dictionary.

    Returns:
        pd.DataFrame: A DataFrame of the emissivity for scope 1.
    """
    df_c = s1_emissions.copy()
    year_tech_product_list = itertools.product(
        df_c.index.get_level_values(0).unique().values, TECH_REFERENCE_LIST
    )
    for year, technology in year_tech_product_list:
        emissions_difference = (
            business_case_ref[(technology, "Process emissions")]
            - business_case_ref[(technology, "Used CO2")]
            - business_case_ref[(technology, "Captured CO2")]
        )
        df_c.loc[year, technology]["emissions"] = (
            df_c.loc[year, technology]["emissions"] + emissions_difference
        )
    return df_c


def generate_emissions_dataframe(
    business_cases: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Creates the base of an emissions DataFrame based on S1 and S3 emissions.
    Args:
        business_cases (pd.DataFrame): The standardised business cases DataFrame.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A DataFrame of the emissivity per scope and a carbon tax DataFrame.
    """

    # S1 emissions covers the Green House Gas (GHG) emissions that a company makes directly.
    s1_emissivity_factors = read_pickle_folder(
        PKL_DATA_IMPORTS, "s1_emissions_factors", "df"
    )

    # S3 emissions: all the emissions associated, not with the company itself,
    # but that the organisation is indirectly responsible for, up and down its value chain.
    s3_emissivity_factors = read_pickle_folder(
        PKL_DATA_FORMATTED, "final_scope3_ef_df", "df"
    )

    return generate_s1_s3_emissions(
        business_cases=business_cases,
        s1_emissivity_factors=s1_emissivity_factors,
        s3_emissivity_factors=s3_emissivity_factors,
    )


def get_s2_emissions(
    power_grid_emissions_ref: dict,
    business_case_ref: dict,
    year: int,
    country_code: str,
    technology: str,
) -> float:
    """Calculates the Scope 2 Emissions based on the power_grid_emissions_ref and the business_case_ref, for a given technology, year and country code.

    Args:
        power_grid_emissions_ref (dict): The power model values dictionary ref.
        business_cases (dict): The standardised business cases reference dict.
        year (int): The year to retrieve S2 emissions for.
        country_code (str): The country code to retrieve S2 values for.
        technology (str): The technology to retrieve S2 emissions for.
    Returns:
        float: The S2 emission value for a particular year technology, region and scenario inputs.
    """
    # Scope 2 Emissions: These are the emissions it makes indirectly
    # like when the electricity or energy it buys for heating and cooling buildings
    return (
        power_grid_emissions_ref[(year, country_code)]
        * business_case_ref[(technology, "Electricity")]
    )


def add_hydrogen_emissions_to_s3_column(
    combined_emissions_df: pd.DataFrame,
    h2_emissions_ref: dict,
    business_case_ref: dict,
    country_codes: list,
) -> pd.DataFrame:
    """Adds the Hydrogen emissions to scope 3 emissions.

    Args:
        combined_emissions_df (pd.DataFrame): The combined emissions DataFrame containing s1, s2 & s3 emissions.
        h2_emissions_ref (dict): The hydrogen emissions reference dictionary.
        business_case_ref (dict): The standardised business cases reference dict.
        country_codes (list): A list of all country codes of steel plants in the model.

    Returns:
        pd.DataFrame: The DataFrame with the modified Scope 3 emissivity values.
    """
    df = combined_emissions_df.copy()
    years = df.index.get_level_values(0).unique()
    technologies = df.index.get_level_values(1).unique()
    year_tech_country_product = list(
        itertools.product(years, technologies, country_codes)
    )
    for year, technology, country_code in tqdm(
        year_tech_country_product,
        total=len(year_tech_country_product),
        desc="Hydrogen S3 emission additions",
    ):
        h2_emissions = h2_emissions_ref[(year, country_code)]
        hydrogen_consumption = business_case_ref[(technology, "Hydrogen")]
        current_s3_value = df.loc[(year, technology), "s3_emissivity"].copy()
        df.loc[(year, technology), "s3_emissivity"] = current_s3_value + (
            h2_emissions * hydrogen_consumption
        )
    return df


def regional_s2_emissivity(
    power_grid_emissions_ref: dict, plant_country_codes: list, business_case_ref: dict
) -> pd.DataFrame:
    """Creates a DataFrame for S2 emissivity reference for each region.

    Args:
        power_grid_emissions_ref (dict): The power model values dictionary ref.
        plant_country_codes (list): List of all country codes in the steel plant DataFrame.
        business_case_ref (dict): The standardised business cases reference dict.

    Returns:
        pd.DataFrame: A DataFrame with the S2 emissivity values for each country code, tech and year iteration.
    """
    df_list = []
    year_country_code_tech_product = list(
        itertools.product(MODEL_YEAR_RANGE, plant_country_codes, TECH_REFERENCE_LIST)
    )
    for year, country_code, technology in tqdm(
        year_country_code_tech_product,
        total=len(year_country_code_tech_product),
        desc="All Country Code S2 Emission: Year Loop",
    ):
        value = get_s2_emissions(
            power_grid_emissions_ref, business_case_ref, year, country_code, technology
        )
        entry = {
            "year": year,
            "country_code": country_code,
            "technology": technology,
            "s2_emissivity": value,
        }
        df_list.append(entry)
    return pd.DataFrame(df_list)


def combine_emissivity(
    s1_ref: pd.DataFrame, s2_ref: pd.DataFrame, s3_ref: pd.DataFrame
) -> pd.DataFrame:
    """Combines scope 1, scope 2 and scope 3 DataFrames into a combined DataFrame.

    Args:
        s1_ref (pd.DataFrame): Scope 1 DataFrame
        s2_ref (pd.DataFrame): Scope 2 DataFrame
        s3_ref (pd.DataFrame): Scope3 DataFrame

    Returns:
        pd.DataFrame: A DataFrame with scopes 1, 2 and 3 data.
    """
    logger.info("Combining S2 Emissions with S1 & S3 emissivity")
    rmi_mapper = create_country_mapper()
    s2_ref = s2_ref.reset_index(drop=True).set_index(["year", "technology"]).copy()
    total_emissivity = s2_ref.join(
        s1_ref.rename({"emissions": "s1_emissivity"}, axis=1)
    ).join(s3_ref.rename({"emissions": "s3_emissivity"}, axis=1))
    total_emissivity["combined_emissivity"] = (
        total_emissivity["s1_emissivity"]
        + total_emissivity["s2_emissivity"]
        + total_emissivity["s3_emissivity"]
    )
    total_emissivity["region"] = total_emissivity["country_code"].apply(
        lambda x: rmi_mapper[x]
    )
    return total_emissivity


def final_combined_emissions_formatting(
    combined_emissions_df: pd.DataFrame,
) -> pd.DataFrame:
    """Formats the Combined Emissions DataFrame.

    Args:
        combined_emissions_df (pd.DataFrame): The combined DataFrame.

    Returns:
        pd.DataFrame: _description_
    """
    df_c = combined_emissions_df.copy()
    df_c = df_c.reset_index().set_index(["year", "country_code", "technology"])
    # change_column_order
    new_col_order = move_cols_to_front(
        df_c,
        [
            "region",
            "s1_emissivity",
            "s2_emissivity",
            "s3_emissivity",
            "combined_emissivity",
        ],
    )
    return df_c[new_col_order].reset_index()


@timer_func
def generate_emissions_flow(
    scenario_dict: dict, pkl_paths: Union[dict, None] = None, serialize: bool = False
) -> pd.DataFrame:
    """Complete flow for createing the emissivity reference for Scopes 1, 2 & 3.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        pkl_paths (Union[dict, None], optional): A dictionary containing custom pickle paths. Defaults to {}.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: The combined S1, S2 & S3 emissions DataFrame reference.
    """
    _, intermediate_path, _ = return_pkl_paths(
        scenario_name=scenario_dict["scenario_name"], paths=pkl_paths
    )
    business_cases_summary = read_pickle_folder(
        PKL_DATA_FORMATTED, "standardised_business_cases", "df"
    ).reset_index()
    business_case_ref = read_pickle_folder(
        PKL_DATA_FORMATTED, "business_case_reference", "df"
    )
    power_grid_emissions_ref = read_pickle_folder(
        intermediate_path, "power_grid_emissions_ref", "df"
    )
    h2_emissions_ref = read_pickle_folder(intermediate_path, "h2_emissions_ref", "df")
    steel_plants = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
    steel_plant_country_codes = list(steel_plants["country_code"].unique())
    emissions = generate_emissions_dataframe(business_cases_summary)
    emissions_s1_summary = emissions[emissions["scope"] == "S1"]
    s1_emissivity = (
        emissions_s1_summary[["technology", "year", "emissions"]]
        .groupby(by=["year", "technology"])
        .sum()
    )
    s1_emissivity = scope1_emissions_calculator(s1_emissivity, business_case_ref)
    s3_emissivity = (
        emissions[emissions["scope"] == "S3"][["technology", "year", "emissions"]]
        .groupby(by=["year", "technology"])
        .sum()
    )
    s2_emissivity = regional_s2_emissivity(
        power_grid_emissions_ref, steel_plant_country_codes, business_case_ref
    )
    combined_emissivity = combine_emissivity(
        s1_emissivity, s2_emissivity, s3_emissivity
    )
    combined_emissivity = add_hydrogen_emissions_to_s3_column(
        combined_emissivity,
        h2_emissions_ref,
        business_case_ref,
        steel_plant_country_codes,
    )
    combined_emissivity = final_combined_emissions_formatting(combined_emissivity)
    if serialize:
        serialize_file(s1_emissivity, intermediate_path, "calculated_s1_emissivity")
        serialize_file(s3_emissivity, intermediate_path, "calculated_s3_emissivity")
        serialize_file(s2_emissivity, intermediate_path, "calculated_s2_emissivity")
        serialize_file(
            combined_emissivity, intermediate_path, "calculated_emissivity_combined"
        )
    return combined_emissivity
