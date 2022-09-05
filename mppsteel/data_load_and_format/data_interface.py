"""Functions to format and access data imports"""

# For Data Manipulation
import pandas as pd
import numpy as np
import pandera as pa

from typing import Tuple, Union

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.dataframe_utility import extend_df_years

from mppsteel.data_validation.data_import_tests import (
    PLASTIC_SCHEMA,
    SCOPE3_EF_SCHEMA_1,
    SCOPE3_EF_SCHEMA_2,
    CAPEX_OPEX_PER_TECH_SCHEMA,
)

# For logger and units dict
from mppsteel.utility.file_handling_utility import (
    extract_data,
    read_pickle_folder,
    serialize_file,
)
from mppsteel.utility.dataframe_utility import melt_and_index, convert_currency_col

# Get model parameters
from mppsteel.config.model_config import (
    GIGAJOULE_TO_MEGAJOULE_FACTOR,
    IMPORT_DATA_PATH,
    MEGATON_TO_TON,
    MODEL_YEAR_END,
    PETAJOULE_TO_GIGAJOULE,
    PKL_DATA_IMPORTS,
    PKL_DATA_FORMATTED,
    EMISSIONS_FACTOR_SLAG,
    MET_COAL_ENERGY_DENSITY_MJ_PER_KG,
    PLASTIC_WASTE_ENERGY_DENSITY_MJ_PER_KG,
    TON_TO_KILOGRAM_FACTOR,
    USD_TO_EUR_CONVERSION_DEFAULT,
)
from mppsteel.config.reference_lists import KG_RESOURCES


logger = get_logger(__name__)

COMMODITY_MATERIAL_MAPPER = {
    "391510": "Plastic waste",
}


@pa.check_input(SCOPE3_EF_SCHEMA_2)
def format_scope3_ef_2(df: pd.DataFrame, emissions_factor_slag: float) -> pd.DataFrame:
    """Format scope 3 emissions sheets

    Args:
        df (pd.DataFrame): A data frame containing the timeseries for Slag emissions.
        emissions_factor_slag (float): An emissions factor value for slag.

    Returns:
        pd.DataFrame: A formatted dataframe with the scope 3 emissions factors.
    """
    df_c = df.copy()
    df_c = df_c.drop(["Unnamed: 1"], axis=1).loc[0:0]
    df_c = df_c.melt(id_vars=["Year"])
    df_c.rename(columns={"Year": "metric", "variable": "year"}, inplace=True)
    df_c["value"] = df_c["value"].astype(float)
    df_c["value"] = df_c["value"].apply(lambda x: x * emissions_factor_slag)
    return df_c


@pa.check_input(SCOPE3_EF_SCHEMA_1)
def modify_scope3_ef_1(
    df: pd.DataFrame, slag_values: np.ndarray, met_coal_density: float
) -> pd.DataFrame:
    """Formatting steps for the Scope 3 Emissions Factors.

    Args:
        df (pd.DataFrame): A DataFrame of Scope 3 Emission Energy Factors
        slag_values (np.ndarray): An array of values for slag
        met_coal_density (float): A singular value representing the density of met coal.

    Returns:
        pd.DataFrame: A DataFrame of the reformatted data.
    """
    df_c = df.copy()
    scope3_df = df_c.set_index(["Category", "Fuel", "Unit"])
    scope3_df.loc[
        "Scope 3 Emissions Factor", "BF slag", "ton CO2eq / ton slag"
    ] = slag_values
    met_coal_values = scope3_df.loc[
        "Scope 3 Emissions Factor", "Met coal", "MtCO2eq / PJ"
    ]
    met_coal_values = met_coal_values.apply(lambda x: x * met_coal_density)
    scope3_df.loc[
        "Scope 3 Emissions Factor", "Met coal", "MtCO2eq / PJ"
    ] = met_coal_values
    scope3_df.reset_index(inplace=True)
    scope3_df = scope3_df.melt(id_vars=["Category", "Fuel", "Unit"], var_name="Year")

    def standardise_units(row):
        return (
            row.value * (MEGATON_TO_TON / PETAJOULE_TO_GIGAJOULE)
            if row.Fuel in {"Natural gas", "Met coal", "Thermal coal"}
            else row.value
        )

    scope3_df["value"] = scope3_df.apply(standardise_units, axis=1)  # to ton/GJ

    scope3_df = extend_df_years(scope3_df, "Year", MODEL_YEAR_END)
    return scope3_df


def capex_generator(
    capex_dict: dict, technology: str, year: int, output_type: str = "all"
) -> Union[dict, pd.DataFrame]:
    """Creates an interface to the tabular capex data.

    Args:
        capex_dict (dict): A capex dictionary with each capex type as a DataFrame
        technology (str): The technology that you want to access
        year (int): The year that you want to access
        output_type (str, optional): Flag whether to access all the the capex values or whichever you specify. Defaults to 'all'.

    Returns:
        Union[dict, pd.DataFrame]: A (dict) if output_type is set to 'all'. Otherwise returns the specific output_type specified (as float).
    """

    greenfield = capex_dict["greenfield"].loc[technology, year].value
    brownfield = capex_dict["brownfield"].loc[technology, year].value
    other_opex = capex_dict["other_opex"].loc[technology, year].value

    capex_dict = {
        "greenfield": greenfield,
        "brownfield": brownfield,
        "other_opex": other_opex,
    }

    if output_type == "all":
        return capex_dict
    return capex_dict[output_type]


@pa.check_input(CAPEX_OPEX_PER_TECH_SCHEMA)
def capex_dictionary_generator(
    greenfield_df: pd.DataFrame,
    brownfield_df: pd.DataFrame,
    other_df: pd.DataFrame,
    eur_to_usd: float,
) -> dict:
    """A dictionary of greenfield, brownfield and other_opex.

    Args:
        greenfield_df (pd.DataFrame): A dataframe of greenfield capex.
        brownfield_df (pd.DataFrame): A dataframe of brownfield capex.
        other_df (pd.DataFrame): A dataframe of other opex.
        eur_to_usd (float): The rate used ot convert EUR values to USD.

    Returns:
        dict: A dictionary of the formatted capex and opex dataframes.
    """
    index_cols = ["Technology", "Year"]
    gf_df = melt_and_index(greenfield_df, ["Technology"], "Year", index_cols)
    bf_df = melt_and_index(brownfield_df, ["Technology"], "Year", index_cols)
    oo_df = melt_and_index(other_df, ["Technology"], "Year", index_cols)
    gf_df = convert_currency_col(gf_df, "value", eur_to_usd)
    bf_df = convert_currency_col(bf_df, "value", eur_to_usd)
    oo_df = convert_currency_col(oo_df, "value", eur_to_usd)

    gf_df = extend_df_years(gf_df, "Year", MODEL_YEAR_END, index_cols)
    bf_df = extend_df_years(bf_df, "Year", MODEL_YEAR_END, index_cols)
    oo_df = extend_df_years(oo_df, "Year", MODEL_YEAR_END, index_cols)

    return {
        "greenfield": gf_df,
        "brownfield": bf_df,
        "other_opex": oo_df,
    }


@pa.check_input(PLASTIC_SCHEMA)
def format_commodities_data(df: pd.DataFrame, material_mapper: dict) -> pd.DataFrame:
    """Formats the Commodities dataset.

    Args:
        df (pd.DataFrame): A DataFrame containing the commodities data.
        material_mapper (dict): A dictionary mapping the material to the commoodity code to the commodity.

    Returns:
        pd.DataFrame: A DataFrame of the formatted commmodities data.
    """
    df_c = df.copy()
    logger.info("Formatting the plastic_prices data")

    def generate_implied_prices(row):
        return 0 if row.netenergy_gj == 0 else row.trade_value / row.netenergy_gj

    columns_of_interest = [
        "Year",
        "Reporter",
        "Commodity Code",
        "Netweight (kg)",
        "Trade Value (US$)",
    ]
    df_c = df_c[columns_of_interest]
    df_c.columns = ["year", "reporter", "commodity_code", "netweight_kg", "trade_value"]
    df_c["commodity"] = df_c["commodity_code"].apply(lambda x: material_mapper[str(x)])
    df_c = df_c[df_c["commodity"] == "Plastic waste"].copy()
    df_c["netweight_kg"].fillna(0, inplace=True)
    df_c["netenergy_gj"] = df_c["netweight_kg"] * (
        PLASTIC_WASTE_ENERGY_DENSITY_MJ_PER_KG / GIGAJOULE_TO_MEGAJOULE_FACTOR
    )
    df_c["implied_price"] = df_c.apply(generate_implied_prices, axis=1)
    df_c = extend_df_years(df_c, "year", MODEL_YEAR_END)
    return df_c


@timer_func
def format_business_cases(bc_df: pd.DataFrame) -> pd.DataFrame:
    """Formats the initial business cases input sheet

    Args:
        bc_df (pd.DataFrame): The initial business cases sheet.

    Returns:
        pd.DataFrame: The formatted business cases sheet.
    """
    bc_df_c = bc_df.copy()
    bc_df_c = bc_df_c.melt(
        id_vars=["Material", "Type of metric", "Unit"],
        var_name="technology",
        value_name="value",
    ).copy()
    bc_df_c.rename(
        {
            "Material": "material_category",
            "Type of metric": "metric_type",
            "Unit": "unit",
        },
        axis=1,
        inplace=True,
    )
    bc_df_c["material_category"] = bc_df_c["material_category"].apply(
        lambda x: x.strip()
    )
    return bc_df_c.set_index(["technology", "material_category"])


@timer_func
def create_capex_opex_dict(serialize: bool = False, from_csv: bool = False) -> dict:
    """Creates a Dictionary containing Greenfield, Brownfield and Opex values.

    Args:
        serialize (bool, optional): Flag to serialize the dictionary. Defaults to False.

    Returns:
        dict: A dictionary containing the capex/opex values.
    """
    if from_csv:
        greenfield_capex_df = extract_data(
            IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 0
        )
        brownfield_capex_df = extract_data(
            IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 1
        )
        other_opex_df = extract_data(
            IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 2
        )
    else:
        greenfield_capex_df = read_pickle_folder(PKL_DATA_IMPORTS, "greenfield_capex")
        brownfield_capex_df = read_pickle_folder(PKL_DATA_IMPORTS, "brownfield_capex")
        other_opex_df = read_pickle_folder(PKL_DATA_IMPORTS, "other_opex")
    capex_dict = capex_dictionary_generator(
        greenfield_capex_df,
        brownfield_capex_df,
        other_opex_df,
        1 / USD_TO_EUR_CONVERSION_DEFAULT,
    )
    if serialize:
        serialize_file(capex_dict, PKL_DATA_FORMATTED, "capex_dict")
    return capex_dict


@timer_func
def generate_preprocessed_emissions_data(
    serialize: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Complete process flow for the preprocessed emissivity data.

    Args:
        serialize (bool, optional): Flag to serialize the emissivity data. Defaults to False.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple of the commodities data and S3 emission factors data.
    """
    plastic_prices = read_pickle_folder(PKL_DATA_IMPORTS, "plastic_prices")
    commodities_df = format_commodities_data(plastic_prices, COMMODITY_MATERIAL_MAPPER)
    s3_emissions_factors_2 = read_pickle_folder(
        PKL_DATA_IMPORTS, "s3_emissions_factors_2"
    )
    scope3df_2_formatted = format_scope3_ef_2(
        s3_emissions_factors_2, EMISSIONS_FACTOR_SLAG
    )
    slag_new_values = scope3df_2_formatted["value"].values
    s3_emissions_factors_1 = read_pickle_folder(
        PKL_DATA_IMPORTS, "s3_emissions_factors_1"
    )
    final_scope3_ef_df = modify_scope3_ef_1(
        s3_emissions_factors_1, slag_new_values, MET_COAL_ENERGY_DENSITY_MJ_PER_KG
    )
    if serialize:
        serialize_file(commodities_df, PKL_DATA_FORMATTED, "commodities_df")
        serialize_file(final_scope3_ef_df, PKL_DATA_FORMATTED, "final_scope3_ef_df")
    return commodities_df, final_scope3_ef_df


def bc_unit_adjustments(row: pd.Series) -> pd.Series:
    """Adjusts the units of the business cases input depending on the type of resource (Energy or Mass resource)

    Args:
        row (pd.Series): The input series passed as an apply function.

    Returns:
        pd.Series: The reformatted units.
    """

    return (
        row.value / TON_TO_KILOGRAM_FACTOR
        if row.material_category in KG_RESOURCES
        else row.value
    )


@timer_func
def create_business_case_reference(
    serialize: bool = True, from_csv: bool = False
) -> tuple:
    """Turns the business cases into a reference dictionary for fast access. But saves a dataframe version and dict as pickle file depending on the `serialize` boolean flag.

    Args:
        serialize (bool, optional): Boolean flag to determine whether the final output is serialized. Defaults to True.

    Returns:
        dict: The dictionary reference of the standardised business cases.
    """
    if from_csv:
        business_cases = extract_data(
            IMPORT_DATA_PATH, "Technology Business Cases", "csv"
        )
    else:
        business_cases = read_pickle_folder(
            PKL_DATA_IMPORTS, "technology_business_cases"
        )
    business_cases = format_business_cases(business_cases)
    business_cases.reset_index(inplace=True)
    business_cases["value"] = business_cases.apply(bc_unit_adjustments, axis=1)
    business_cases.set_index(["technology", "material_category"], inplace=True)
    business_case_reference = business_cases.to_dict()["value"]
    if serialize:
        serialize_file(
            business_cases, PKL_DATA_FORMATTED, "standardised_business_cases"
        )
        serialize_file(
            business_case_reference, PKL_DATA_FORMATTED, "business_case_reference"
        )
    return business_cases, business_case_reference
