"""Script that creates the price and emissions tables."""

# For Data Manipulation
from typing import Tuple
import pandas as pd

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

# For logger and units dict
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import move_cols_to_front
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger

from mppsteel.data_loading.pe_model_formatter import (
    RE_DICT,
    power_data_getter,
    hydrogen_data_getter,
)

from mppsteel.data_loading.data_interface import load_business_cases

from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_IMPORTS,
    PKL_DATA_INTERMEDIATE,
)

from mppsteel.config.reference_lists import TECH_REFERENCE_LIST, SWITCH_DICT

from mppsteel.data_loading.data_interface import (
    scope1_emissions_getter,
    scope3_ef_getter,
    carbon_tax_getter,
)

from mppsteel.utility.location_utility import (
    country_mapping_fixer,
    country_matcher,
    match_country,
    get_region_from_country_code,
)

from mppsteel.utility.transform_units import transform_units

from mppsteel.config.model_scenarios import (
    COST_SCENARIO_MAPPER,
    GRID_DECARBONISATION_SCENARIOS,
)

# Create logger
logger = get_logger("Emissions Reference")


def apply_emissions(
    df: pd.DataFrame,
    single_year: int = None,
    year_end: int = 2021,
    s1_emissivity_factors: pd.DataFrame = None,
    s3_emissivity_factors: pd.DataFrame = None,
    carbon_tax_df: pd.DataFrame = None,
    non_standard_dict: dict = None,
    scope: str = "1",
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    # Create resources reference list
    s1_emissivity_resources = s1_emissivity_factors["Metric"].unique().tolist()
    s3_emissions_resources = s3_emissivity_factors["Fuel"].unique().tolist()

    # Create a year range
    year_range = range(MODEL_YEAR_START, tuple({year_end + 1 or 2021})[0])
    if single_year:
        year_range = [single_year]

    df_list = []

    logger.info(f"calculating emissions reference tables")

    def value_mapper(row, enum_dict: dict):
        resource = row[enum_dict["material_category"]]
        resource_consumed = row[enum_dict["value"]]

        if resource in s1_emissivity_resources:
            emission_unit_value = scope1_emissions_getter(
                s1_emissivity_factors, resource
            )
            # S1 emissions without process emissions or CCS/CCU
            row[enum_dict["S1"]] = resource_consumed * emission_unit_value
        else:
            row[enum_dict["S1"]] = 0

        if resource in s3_emissions_resources:
            emission_unit_value = scope3_ef_getter(
                s3_emissivity_factors, resource, year
            )
            row[enum_dict["S3"]] = resource_consumed * emission_unit_value
        else:
            row[enum_dict["S3"]] = 0

        if carbon_tax_df is not None:
            carbon_tax_unit = carbon_tax_getter(carbon_tax_df, year)
        else:
            carbon_tax_unit = 0

        if scope == "1":
            S1_value = row[enum_dict["S1"]]
            row[enum_dict["carbon_cost"]] = S1_value * carbon_tax_unit
        elif scope == "2&3":
            S2_value = row[enum_dict["S2"]]
            S3_value = row[enum_dict["S3"]]
            row[enum_dict["carbon_cost"]] = (S2_value + S3_value) * carbon_tax_unit

        return row

    for year in tqdm(
        year_range, total=len(year_range), desc="Emissions Reference Table"
    ):

        df_c = df.copy()
        df_c["year"] = year
        df_c["S1"] = ""
        df_c["S2"] = ""
        df_c["S3"] = ""
        df_c["carbon_cost"] = ""

        tqdma.pandas(desc="Apply Emissions to Technology Resource Usage")
        enumerated_cols = enumerate_iterable(df_c.columns)
        df_c = df_c.progress_apply(
            value_mapper, enum_dict=enumerated_cols, axis=1, raw=True
        )

        df_list.append(df_c)

    combined_df = pd.concat(df_list)
    combined_df.drop(labels=["value"], axis=1, inplace=True)
    combined_df = combined_df.melt(
        id_vars=["technology", "year", "material_category", "unit"],
        var_name=["scope"],
        value_name="emissions",
    )

    if carbon_tax_df:
        combined_df = combined_df.loc[combined_df["scope"] != "carbon_cost"].copy()

    carbon_df = (
        combined_df.loc[combined_df["scope"] == "carbon_cost"]
        .reset_index(drop=True)
        .copy()
    )
    carbon_df.rename(mapper={"emissions": "value"}, axis=1, inplace=True)
    emissions_df = (
        combined_df.loc[combined_df["scope"] != "carbon_cost"]
        .reset_index(drop=True)
        .copy()
    )

    return emissions_df, carbon_df


def create_emissions_ref_dict(df: pd.DataFrame, tech_ref_list: list) -> dict:
    value_ref_dict = {}
    resource_list = ["Process emissions", "Captured CO2", "Used CO2"]
    for technology in tech_ref_list:
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


def generate_emissions_dataframe(
    df: pd.DataFrame, year_end: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    # S1 emissions covers the Green House Gas (GHG) emissions that a company makes directly
    s1_emissivity_factors = read_pickle_folder(
        PKL_DATA_IMPORTS, "s1_emissions_factors", "df"
    )

    # Scope 2 Emissions: These are the emissions it makes indirectly
    # like when the electricity or energy it buys for heating and cooling buildings

    # S3 emissions: all the emissions associated, not with the company itself,
    # but that the organisation is indirectly responsible for, up and down its value chain.
    s3_emissivity_factors = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "final_scope3_ef_df", "df"
    )

    non_standard_dict_ref = create_emissions_ref_dict(df, TECH_REFERENCE_LIST)

    emissions, carbon = apply_emissions(
        df=df.copy(),
        year_end=year_end,
        s1_emissivity_factors=s1_emissivity_factors,
        s3_emissivity_factors=s3_emissivity_factors,
        non_standard_dict=non_standard_dict_ref,
        scope="1",
    )

    return emissions, carbon


def get_s2_emissions(
    power_model: dict,
    hydrogen_model: dict,
    business_cases: pd.DataFrame,
    country_ref_dict: dict,
    year: int,
    country_code: str,
    technology: str,
    electricity_cost_scenario: str,
    grid_scenario: str,
    hydrogen_cost_scenario: str,
) -> float:

    electricity_cost_scenario = COST_SCENARIO_MAPPER[electricity_cost_scenario]
    grid_scenario = GRID_DECARBONISATION_SCENARIOS[grid_scenario]
    hydrogen_cost_scenario = COST_SCENARIO_MAPPER[hydrogen_cost_scenario]

    electricity_emissions = power_data_getter(
        power_model,
        "emissions",
        year,
        country_code,
        country_ref_dict,
        re_dict=RE_DICT,
        grid_scenario=grid_scenario,
        cost_scenario=electricity_cost_scenario,
    )

    h2_emissions = hydrogen_data_getter(
        hydrogen_model,
        "emissions",
        year,
        country_code,
        country_ref_dict,
        cost_scenario=hydrogen_cost_scenario,
    )

    bcases = (
        business_cases.loc[business_cases["technology"] == technology]
        .copy()
        .reset_index(drop=True)
    )
    hydrogen_consumption = 0
    electricity_consumption = 0
    if "Hydrogen" in bcases["material_category"].unique():
        hydrogen_consumption = bcases[bcases["material_category"] == "Hydrogen"][
            "value"
        ].values[0]
    if "Electricity" in bcases["material_category"].unique():
        electricity_consumption = bcases[bcases["material_category"] == "Electricity"][
            "value"
        ].values[0]

    return ((h2_emissions / 1000) * hydrogen_consumption) + (
        transform_units(electricity_emissions, "mwh_gj", "larger") * electricity_consumption
    )


def regional_s2_emissivity(
    electricity_cost_scenario: str, grid_scenario: str, hydrogen_cost_scenario: str
) -> pd.DataFrame:
    b_df = load_business_cases()
    power_model_formatted = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "power_model_formatted", "df"
    )
    hydrogen_model_formatted = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "hydrogen_model_formatted", "df"
    )
    steel_plants = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
    )
    steel_plant_country_codes = list(steel_plants["country_code"].unique())
    country_ref_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in tqdm(
        year_range,
        total=len(year_range),
        desc="All Country Code S2 Emission: Year Loop",
    ):
        for country_code in steel_plant_country_codes:
            for technology in SWITCH_DICT:
                value = get_s2_emissions(
                    power_model_formatted,
                    hydrogen_model_formatted,
                    b_df,
                    country_ref_dict,
                    year,
                    country_code,
                    technology,
                    electricity_cost_scenario,
                    grid_scenario,
                    hydrogen_cost_scenario,
                )
                entry = {
                    "year": year,
                    "country_code": country_code,
                    "technology": technology,
                    "s2_emissivity": value,
                }
                df_list.append(entry)
    combined_df = pd.DataFrame(df_list)
    return combined_df


def combine_emissivity(
    s1_ref: pd.DataFrame, s2_ref: pd.DataFrame, s3_ref: pd.DataFrame
) -> pd.DataFrame:
    logger.info("Combining S2 Emissions with S1 & S3 emissivity")
    country_ref_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    total_emissivity = s2_ref.set_index(["year", "country_code", "technology"]).copy()
    total_emissivity["s1_emissivity"] = ""
    total_emissivity["s3_emissivity"] = ""
    country_codes = total_emissivity.index.get_level_values(1).unique()

    total_emissivity.reset_index()
    total_emissivity['region']= (total_emissivity['country_code'].apply(lambda x: get_region_from_country_code(
        x, "rmi_region", country_ref_dict
    )))
    
    total_emissivity.set_index(["year", "country_code", "technology"])
    
    technologies = total_emissivity.index.get_level_values(2).unique()
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in tqdm(year_range, total=len(year_range), desc="s1 and s3 value assigning"):
        for country_code in country_codes:
            for technology in technologies:
                total_emissivity.loc[
                    (year, country_code, technology), "s1_emissivity"
                ] = s1_ref.loc[year, technology]["emissions"]
                total_emissivity.loc[
                    (year, country_code, technology), "s3_emissivity"
                ] = s3_ref.loc[year, technology]["emissions"]

    total_emissivity["combined_emissivity"] = (
        total_emissivity["s1_emissivity"]
        + total_emissivity["s2_emissivity"]
        + total_emissivity["s3_emissivity"]
    )
    # change_column_order
    new_col_order = move_cols_to_front(
        total_emissivity,
        ["region","s1_emissivity", "s2_emissivity", "s3_emissivity", "combined_emissivity"],
    )
    return total_emissivity[new_col_order].reset_index()


def emissivity_getter(
    df_ref: pd.DataFrame, year: int, country_code: str, technology: str, scope: str
) -> float:
    year = min(2050, year)
    emissivity_mapper = {
        "s1": "s1_emissivity",
        "s2": "s2_emissivity",
        "s3": "s3_emissivity",
    }
    if scope in ["s1", "s2", "s3"]:
        return df_ref.loc[year, country_code, technology][emissivity_mapper[scope]]
    else:
        return df_ref.loc[year, country_code, technology]["combined_emissivity"]


@timer_func
def generate_emissions_flow(
    scenario_dict: dict, serialize: bool = False
) -> pd.DataFrame:
    business_cases_summary = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "standardised_business_cases", "df"
    )
    electricity_cost_scenario = scenario_dict["electricity_cost_scenario"]
    grid_scenario = scenario_dict["grid_scenario"]
    hydrogen_cost_scenario = scenario_dict["hydrogen_cost_scenario"]
    business_cases_summary_c = (
        business_cases_summary.loc[business_cases_summary["material_category"] != 0]
        .copy()
        .reset_index(drop=True)
    )
    emissions_df = business_cases_summary_c.copy()
    emissions, carbon = generate_emissions_dataframe(
        business_cases_summary_c, MODEL_YEAR_END
    )
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
    s2_emissivity = regional_s2_emissivity(
        electricity_cost_scenario, grid_scenario, hydrogen_cost_scenario
    )
    combined_emissivity = combine_emissivity(
        s1_emissivity, s2_emissivity, s3_emissivity
    )

    if serialize:
        serialize_file(s1_emissivity, PKL_DATA_INTERMEDIATE, "calculated_s1_emissivity")
        serialize_file(s3_emissivity, PKL_DATA_INTERMEDIATE, "calculated_s3_emissivity")
        serialize_file(s2_emissivity, PKL_DATA_INTERMEDIATE, "calculated_s2_emissivity")
        serialize_file(
            combined_emissivity, PKL_DATA_INTERMEDIATE, "calculated_emissivity_combined"
        )
    return combined_emissivity
