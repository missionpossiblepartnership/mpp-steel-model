"""Script that creates the price and emissions tables."""

# For Data Manipulation
import pandas as pd

from tqdm import tqdm

# For logger and units dict
from mppsteel.utility.utils import (
    get_logger,
    read_pickle_folder,
    serialize_file,
    timer_func,
)

from mppsteel.model_config import MODEL_YEAR_END, MODEL_YEAR_START, PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE
from mppsteel.utility.reference_lists import TECH_REFERENCE_LIST

from mppsteel.data_loading.data_interface import (
    commodity_data_getter,
    static_energy_prices_getter,
    scope1_emissions_getter,
    grid_emissivity_getter,
    scope3_ef_getter,
    carbon_tax_getter,
)

# Create logger
logger = get_logger("Emissions")

def apply_emissions(
    df: pd.DataFrame,
    single_year: int = None,
    year_end: int = 2021,
    s1_emissions_df: pd.DataFrame = None,
    s2_emissions_df: pd.DataFrame = None,
    s3_emissions_df: pd.DataFrame = None,
    carbon_tax_df: pd.DataFrame = None,
    non_standard_dict: dict = None,
    scope: str = "1",
) -> pd.DataFrame:

    # Create resources reference list
    s1_emissions_resources = s1_emissions_df["Metric"].unique().tolist()
    s2_emissions_resources = ["Electricity"]
    s3_emissions_resources = s3_emissions_df["Fuel"].unique().tolist()

    # Create a year range
    year_range = range(MODEL_YEAR_START, tuple({year_end + 1 or 2021})[0])
    if single_year:
        year_range = [single_year]

    df_list = []

    for year in year_range:
        logger.info(f"calculating year {year}")
        df_c = df.copy()
        df_c["year"] = year
        df_c["S1"] = ""
        df_c["S2"] = ""
        df_c["S3"] = ""
        df_c["carbon_cost"] = ""

        for row in df_c.itertuples():
            resource = row.material_category
            resource_consumed = row.value

            if resource in s1_emissions_resources:
                emission_unit_value = scope1_emissions_getter(s1_emissions_df, resource)
                # S1 emissions without process emissions or CCS/CCU
                df_c.loc[row.Index, "S1"] = (
                    resource_consumed * emission_unit_value / 1000
                )
            else:
                df_c.loc[row.Index, "S1"] = 0

            if resource in s2_emissions_resources:
                emission_unit_value = grid_emissivity_getter(s2_emissions_df, year)
                df_c.loc[row.Index, "S2"] = (
                    resource_consumed * emission_unit_value / 1000
                )
            else:
                df_c.loc[row.Index, "S2"] = 0

            if resource in s3_emissions_resources:
                emission_unit_value = scope3_ef_getter(s3_emissions_df, resource, year)
                df_c.loc[row.Index, "S3"] = resource_consumed * emission_unit_value
            else:
                df_c.loc[row.Index, "S3"] = 0

            if carbon_tax_df is not None:
                carbon_tax_unit = carbon_tax_getter(carbon_tax_df, year)
            else:
                carbon_tax_unit = 0

            if scope == "1":
                S1_value = df_c.at[row.Index, "S1"]
                df_c.loc[row.Index, "carbon_cost"] = S1_value * carbon_tax_unit
            elif scope == "2&3":
                S2_value = df_c.at[row.Index, "S2"]
                S3_value = df_c.at[row.Index, "S3"]
                df_c.loc[row.Index, "carbon_cost"] = (
                    S2_value + S3_value
                ) * carbon_tax_unit

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


def create_emissions_ref_dict(df: pd.DataFrame, tech_ref_list: list):
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


def full_emissions(df: pd.DataFrame, emissions_exceptions_dict: dict, tech_list: list):
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

def generate_emissions_dataframe(df: pd.DataFrame, year_end: int):

    # S1 emissions covers the Green House Gas (GHG) emissions that a company makes directly
    s1_emissions = read_pickle_folder(PKL_DATA_IMPORTS, "s1_emissions_factors", "df")

    # Scope 2 Emissions: These are the emissions it makes indirectly
    # like when the electricity or energy it buys for heating and cooling buildings
    grid_emissivity = read_pickle_folder(PKL_DATA_IMPORTS, "grid_emissivity", "df")

    # S3 emissions: all the emissions associated, not with the company itself,
    # but that the organisation is indirectly responsible for, up and down its value chain.
    final_scope3_ef_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, "final_scope3_ef_df", "df")

    non_standard_dict_ref = create_emissions_ref_dict(df, TECH_REFERENCE_LIST)

    emissions, carbon = apply_emissions(
        df=df.copy(),
        year_end=year_end,
        s1_emissions_df=s1_emissions,
        s2_emissions_df=grid_emissivity,
        s3_emissions_df=final_scope3_ef_df,
        non_standard_dict=non_standard_dict_ref,
        scope="1",
    )

    return emissions, carbon

@timer_func
def generate_emissions_flow(serialize_only: bool = False):
    business_cases_summary = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "standardised_business_cases", "df"
    )
    business_cases_summary_c = (
        business_cases_summary.loc[business_cases_summary["material_category"] != 0]
        .copy()
        .reset_index(drop=True)
    )
    emissions_df = business_cases_summary_c.copy()
    emissions, carbon = generate_emissions_dataframe(business_cases_summary_c, MODEL_YEAR_END)
    emissions_s1_summary = emissions[emissions["scope"] == "S1"]
    s1_summary_df = (
        emissions_s1_summary[["technology", "year", "emissions"]]
        .groupby(by=["year", "technology"])
        .sum()
    )
    em_exc_ref_dict = create_emissions_ref_dict(emissions_df, TECH_REFERENCE_LIST)
    s1_summary_df = full_emissions(s1_summary_df, em_exc_ref_dict, TECH_REFERENCE_LIST)
    emissions_s2_summary = (
        emissions[emissions["scope"] == "S2"][["technology", "year", "emissions"]]
        .groupby(by=["year", "technology"])
        .sum()
    )
    emissions_s3_summary = (
        emissions[emissions["scope"] == "S3"][["technology", "year", "emissions"]]
        .groupby(by=["year", "technology"])
        .sum()
    )

    if serialize_only:
        serialize_file(s1_summary_df, PKL_DATA_INTERMEDIATE, "calculated_s1_emissions")
        serialize_file(emissions_s2_summary, PKL_DATA_INTERMEDIATE, "calculated_s2_emissions")
        serialize_file(emissions_s3_summary, PKL_DATA_INTERMEDIATE, "calculated_s3_emissions")
        return
    return {
        "s1_calculations": s1_summary_df,
        "s2_calculations": emissions_s2_summary,
        "s3_calculations": emissions_s3_summary,
    }
