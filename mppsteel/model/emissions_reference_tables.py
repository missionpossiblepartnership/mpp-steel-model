"""Script that creates the price and emissions tables."""

# For Data Manipulation
import itertools
from typing import Tuple, Union
import pandas as pd

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

# For logger and units dict
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import move_cols_to_front
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.data_loading.pe_model_formatter import (
    POWER_HYDROGEN_COUNTRY_MAPPER,
    pe_model_data_getter,
)
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS
)
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST
from mppsteel.data_loading.data_interface import (
    scope1_emissions_getter,
    scope3_ef_getter,
)

# Create logger
logger = get_logger(__name__)


def generate_s1_s3_emissions(
    business_cases: pd.DataFrame,
    single_year: int = None,
    s1_emissivity_factors: pd.DataFrame = None,
    s3_emissivity_factors: pd.DataFrame = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Creates a DataFrame with emissivity for S1, S2 & S3 for each technology.
    Multiples the emissivity values by the standardized business cases.

    Args:
        df (pd.DataFrame): The standardised business cases DataFrame.
        single_year (int, optional): A single year that you want to calculate emissivity for. Defaults to None.
        s1_emissivity_factors (pd.DataFrame, optional): Emissions Factors for S1. Defaults to None.
        s3_emissivity_factors (pd.DataFrame, optional): Emissions Factors for S3. Defaults to None.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A DataFrame of the emissivity per scope.
    """

    # Create resources reference list
    s1_emissivity_resources = s1_emissivity_factors["Metric"].unique().tolist()
    s3_emissions_resources = s3_emissivity_factors["Fuel"].unique().tolist()

    # Create a year range
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    if single_year:
        year_range = [single_year]

    df_list = []

    logger.info(f"calculating emissions reference tables")

    def value_mapper(row):
        resource = row["material_category"]
        resource_consumed = row["value"]

        if resource in s1_emissivity_resources:
            emission_unit_value = scope1_emissions_getter(
                s1_emissivity_factors, resource
            )
            # S1 emissions without process emissions or CCS/CCU
            row["S1"] = resource_consumed * emission_unit_value
        else:
            row["S1"] = 0

        if resource in s3_emissions_resources:
            emission_unit_value = scope3_ef_getter(
                s3_emissivity_factors, resource, year
            )
            if resource == 'BF slag':
                emission_unit_value = emission_unit_value * -1
            row["S3"] = resource_consumed * emission_unit_value
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

        tqdma.pandas(desc="Apply Emissions to Technology Resource Usage")
        df_c = df_c.progress_apply(value_mapper, axis=1,)

        df_list.append(df_c)

    combined_df = pd.concat(df_list)
    combined_df.drop(labels=["value"], axis=1, inplace=True)
    combined_df = combined_df.melt(
        id_vars=["technology", "year", "material_category", "unit"],
        var_name=["scope"],
        value_name="emissions",
    )
    return combined_df.reset_index(drop=True).copy()

def create_emissions_ref_dict(df: pd.DataFrame, tech_list: list) -> dict:
    """Creates a reference to technologies, resources and emissions for a predefined list of technologies and resources.

    Args:
        df (pd.DataFrame): The standardised business cases DataFrame.
        tech_list (list): A list of technologies that you want to create the emissions reference for.

    Returns:
        dict: A dictionary of technology, resources and emissions.
    """
    value_ref_dict = {}
    resource_list = ["Process emissions", "Captured CO2", "Used CO2"]
    for technology in tech_list:
        resource_dict = {}
        for resource in resource_list:
            try:
                val = df[
                    (df["technology"] == technology)
                    & (df["material_category"] == resource)
                ]["value"].values[0]
            except:
                val = 0
            resource_dict[resource] = val
        value_ref_dict[technology] = resource_dict
    return value_ref_dict


def full_emissions(
    df: pd.DataFrame, emissions_exceptions_dict: dict, tech_list: list
) -> pd.DataFrame:
    """Combines regular emissions with process emissions and ccs/ccu emissions to get a complete
    emissions reference for a list of technologies.

    Args:
        df (pd.DataFrame): The standardised business cases DataFrame.
        emissions_exceptions_dict (dict): A dictionary of technology, resources and emissions.
        tech_list (list): The technologies you want to retrieve full emissions reference for.

    Returns:
        pd.DataFrame: A DataFrame of the emissivity per scope and a carbon tax DataFrame. 
    """

    df_c = df.copy()
    for year in df_c.index.get_level_values(0).unique().values:
        for technology in tech_list:
            val = df_c.loc[year, technology]["emissions"]
            em_exc_dict = emissions_exceptions_dict[technology]
            process_emission = em_exc_dict["Process emissions"]
            combined_ccs_ccu_emissions = (
                em_exc_dict["Used CO2"] + em_exc_dict["Captured CO2"]
            )
            df_c.loc[year, technology]["emissions"] = (
                val + process_emission - combined_ccs_ccu_emissions
            )
    return df_c


def generate_emissions_dataframe(business_cases: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
    power_model: dict,
    business_case_ref: dict,
    country_mapper: dict,
    year: int,
    country_code: str,
    technology: str
) -> float:
    """_summary_

    Args:
        power_model (dict): The power model outputs dictionary.
        hydrogen_model (dict): The hydrogen model outputs dictionary.
        business_cases (pd.DataFrame): The standardised business cases DataFrame.
        country_mapper (dict): A reference to the countries for mapping purposes.
        year (int): The year to retrieve S2 emissions for.
        country_code (str): The country code to retrieve S2 values for.
        technology (str): The technology to retrieve S2 emissions for.
    Returns:
        float: The S2 emission value for a particular year technology, region and scenario inputs.
    """
    # Scope 2 Emissions: These are the emissions it makes indirectly
    # like when the electricity or energy it buys for heating and cooling buildings

    electricity_emissions = pe_model_data_getter(
        power_model,
        country_mapper,
        POWER_HYDROGEN_COUNTRY_MAPPER,
        year,
        country_code
    )
    electricity_consumption = business_case_ref[(technology, 'Electricity')]
    return (
        electricity_emissions * electricity_consumption
    )

def add_hydrogen_emissions_to_s3_column(
    combined_emissions_df: pd.DataFrame, 
    hydrogen_model: dict,
    business_case_ref: dict,
    country_codes: list,
):
    rmi_mapper = create_country_mapper()
    df = combined_emissions_df.copy()
    years = df.index.get_level_values(0).unique()
    technologies = df.index.get_level_values(1).unique()
    year_tech_country_product = list(itertools.product(years, technologies, country_codes))
    for year, technology, country_code in tqdm(
        year_tech_country_product, total=len(year_tech_country_product), desc='Hydrogen S3 emission additions'):
        h2_emissions = pe_model_data_getter(
            hydrogen_model,
            rmi_mapper,
            POWER_HYDROGEN_COUNTRY_MAPPER,
            year,
            country_code
        ) / 1000
        hydrogen_consumption = business_case_ref[(technology, 'Hydrogen')]
        current_s3_value = df.loc[(year, technology), 's3_emissivity'].copy()
        df.loc[(year, technology), 's3_emissivity'] = current_s3_value + (h2_emissions * hydrogen_consumption)
    return df


def regional_s2_emissivity(
    power_model: pd.DataFrame, 
    plant_country_code_mapper: dict, 
    business_case_ref: dict
) -> pd.DataFrame:
    """Creates a DataFrame for S2 emissivity reference for each region.

    Args:

    Returns:
        pd.DataFrame: A DataFrame with the S2 emissivity values for each country within a reference of steel plants.
    """
    
    rmi_mapper = create_country_mapper()
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in tqdm(
        year_range,
        total=len(year_range),
        desc="All Country Code S2 Emission: Year Loop",
    ):
        for country_code, technology in itertools.product(plant_country_code_mapper, TECH_REFERENCE_LIST):
            value = get_s2_emissions(
                power_model,
                business_case_ref,
                rmi_mapper,
                year,
                country_code,
                technology
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
    s2_ref = s2_ref.reset_index(drop=True).set_index(['year', 'technology']).copy()
    total_emissivity = s2_ref.join(
        s1_ref.rename({'emissions': 's1_emissivity'}, axis=1)
        ).join(s3_ref.rename({'emissions': 's3_emissivity'}, axis=1)
    )
    total_emissivity["combined_emissivity"] = (
        total_emissivity["s1_emissivity"]
        + total_emissivity["s2_emissivity"]
        + total_emissivity["s3_emissivity"]
    )
    total_emissivity['region'] = total_emissivity['country_code'].apply(lambda x: rmi_mapper[x])
    return total_emissivity

def final_combined_emissions_formatting(combined_emissions_df: pd.DataFrame):
    df_c = combined_emissions_df.copy()
    df_c = df_c.reset_index().set_index(["year", "country_code", "technology"])
    # change_column_order
    new_col_order = move_cols_to_front(
        df_c,
        ["region","s1_emissivity", "s2_emissivity", "s3_emissivity", "combined_emissivity"],
    )
    return df_c[new_col_order].reset_index()


def emissivity_getter(
    df_ref: pd.DataFrame, year: int, country_code: str, technology: str, scope: str
) -> Union[pd.DataFrame, float]:
    """A getter function for the combined emissions DataFrame.

    Args:
        df_ref (pd.DataFrame): The combined S1, S2 & S3 emissions DataFrame reference.
        year (int): The requested year.
        country_code (str): The requested region (varies S2 emissions).
        technology (str): The requested technology.
        scope (str): The requested scope level of the emissions.

    Returns:
        Union[pd.DataFrame, float]: A float value of the emissions based on the scope specified as a parameter. 
    """
    year = min(2050, year)
    if not technology:
        return 0
    if scope not in {"s1", "s2", "s3"}:
        return df_ref.loc[year, country_code, technology]["combined_emissivity"]
    emissivity_mapper = {
        "s1": "s1_emissivity",
        "s2": "s2_emissivity",
        "s3": "s3_emissivity",
    }
    return df_ref.loc[year, country_code, technology][emissivity_mapper[scope]]


@timer_func
def generate_emissions_flow(
    scenario_dict: dict, serialize: bool = False
) -> pd.DataFrame:
    """Complete flow for createing the emissivity reference for Scopes 1, 2 & 3.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: The combined S1, S2 & S3 emissions DataFrame reference. 
    """
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    business_cases_summary = read_pickle_folder(
        PKL_DATA_FORMATTED, "standardised_business_cases", "df"
    ).reset_index()
    business_case_ref = read_pickle_folder(
        PKL_DATA_FORMATTED, "business_case_reference", "df"
    )
    power_grid_emissions_formatted = read_pickle_folder(
        intermediate_path, "power_grid_emissions_formatted", "df"
    )
    hydrogen_emissions_formatted = read_pickle_folder(
        intermediate_path, "hydrogen_emissions_formatted", "df"
    )
    steel_plants = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
    steel_plant_country_codes = list(steel_plants["country_code"].unique())
    emissions_df = business_cases_summary.copy()
    emissions = generate_emissions_dataframe(
        business_cases_summary)
    emissions_s1_summary = emissions[emissions["scope"] == "S1"]
    s1_emissivity = (
        emissions_s1_summary[["technology", "year", "emissions"]]
        .groupby(by=["year", "technology"])
        .sum()
    )
    em_exc_ref_dict = create_emissions_ref_dict(emissions_df, TECH_REFERENCE_LIST)
    s1_emissivity = full_emissions(s1_emissivity, em_exc_ref_dict, TECH_REFERENCE_LIST)
    s3_emissivity = (
        emissions[emissions["scope"] == "S3"][["technology", "year", "emissions"]]
        .groupby(by=["year", "technology"])
        .sum()
    )
    s2_emissivity = regional_s2_emissivity(power_grid_emissions_formatted, steel_plant_country_codes, business_case_ref)
    combined_emissivity = combine_emissivity(
        s1_emissivity, s2_emissivity, s3_emissivity
    )
    combined_emissivity = add_hydrogen_emissions_to_s3_column(combined_emissivity, hydrogen_emissions_formatted, business_case_ref, steel_plant_country_codes)
    combined_emissivity = final_combined_emissions_formatting(combined_emissivity)
    if serialize:
        serialize_file(em_exc_ref_dict, intermediate_path, "em_exc_ref_dict")
        serialize_file(s1_emissivity, intermediate_path, "calculated_s1_emissivity")
        serialize_file(s3_emissivity, intermediate_path, "calculated_s3_emissivity")
        serialize_file(s2_emissivity, intermediate_path, "calculated_s2_emissivity")
        serialize_file(
            combined_emissivity, intermediate_path, "calculated_emissivity_combined"
        )
    return combined_emissivity
