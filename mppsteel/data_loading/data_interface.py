"""Functions to format and access data imports"""

# For Data Manipulation
import pandas as pd
import pandera as pa
import numpy as np

from typing import Tuple, Union
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.log_utility import get_logger

from mppsteel.validation.data_import_tests import (
    ETHANOL_PLASTIC_CHARCOAL_SCHEMA,
    SCOPE3_EF_SCHEMA_1,
    SCOPE3_EF_SCHEMA_2,
    CAPEX_OPEX_PER_TECH_SCHEMA,
)

# For logger and units dict
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file
)
from mppsteel.utility.dataframe_utility import (
    melt_and_index, convert_currency_col
    )

# Get model parameters
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    PKL_DATA_IMPORTS,
    PKL_DATA_FORMATTED,
    EMISSIONS_FACTOR_SLAG,
    ENERGY_DENSITY_MET_COAL_MJ_KG,
    USD_TO_EUR_CONVERSION_DEFAULT
)

# Create logger
logger = get_logger("Data Interface")

COMMODITY_MATERIAL_MAPPER = {
    "4402": "charcoal",
    "220710": "ethanol",
    "391510": "plastic",
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
    df: pd.DataFrame, slag_values: np.array, met_coal_density: float
) -> pd.DataFrame:
    """Formatting steps for the Scope 3 Emissions Factors.

    Args:
        df (pd.DataFrame): A DataFrame of Scope 3 Emission Energy Factors
        slag_values (np.array): An array of values for slag
        met_coal_density (float): A singular value representing the density of met coal.

    Returns:
        pd.DataFrame: A DataFrame of the reformatted data.
    """
    df_c = df.copy()
    scope3df_index = df_c.set_index(["Category", "Fuel", "Unit"])
    scope3df_index.loc[
        "Scope 3 Emissions Factor", "BF slag", "ton CO2eq / ton slag"
    ] = slag_values/1000 # from [t CO2/ t slag] to [t CO2/ kg slag] see standardized BC
    met_coal_values = scope3df_index.loc[
        "Scope 3 Emissions Factor", "Met coal", "MtCO2eq / PJ"
    ]
    met_coal_values = met_coal_values.apply(lambda x: x * met_coal_density)
    scope3df_index.loc[
        "Scope 3 Emissions Factor", "Met coal", "MtCO2eq / PJ"
    ] = met_coal_values
    scope3df_index.reset_index(inplace=True)
    return scope3df_index.melt(id_vars=["Category", "Fuel", "Unit"], var_name="Year")


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
    greenfield_df: pd.DataFrame, brownfield_df: pd.DataFrame, 
    other_df: pd.DataFrame, eur_to_usd: float = 1 / USD_TO_EUR_CONVERSION_DEFAULT
) -> dict:
    """A dictionary of greenfield, brownfield and other_opex.

    Args:
        greenfield_df (pd.DataFrame): A dataframe of greenfield capex.
        brownfield_df (pd.DataFrame): A dataframe of brownfield capex.
        other_df (pd.DataFrame): A dataframe of other opex.

    Returns:
        dict: A dictionary of the formatted capex and opex dataframes.
    """
    gf_df = melt_and_index(
            greenfield_df, ["Technology"], "Year", ["Technology", "Year"]
        )
    bf_df = melt_and_index(
            brownfield_df, ["Technology"], "Year", ["Technology", "Year"]
        )
    oo_df = melt_and_index(
            other_df, ["Technology"], "Year", ["Technology", "Year"]
        )
    gf_df = convert_currency_col(gf_df, 'value', eur_to_usd)
    bf_df = convert_currency_col(bf_df, 'value', eur_to_usd)
    oo_df = convert_currency_col(oo_df, 'value', eur_to_usd)
    
    return {
        "greenfield": gf_df,
        "brownfield": bf_df,
        "other_opex": oo_df,
    }


def carbon_tax_getter(df: pd.DataFrame, year: int) -> float:
    """Function to get a carbon tax value at a particular year.

    Args:
        df (pd.DataFrame): A DataFrame containing the carbon tax timeseries
        year (int): The year that you want to query.

    Returns:
        float: The value of the carbon tax at a particular year
    """
    df_c = df.copy()
    df_c.columns = [col.lower() for col in df_c.columns]
    df_c.set_index(["year"], inplace=True)
    return df_c.loc[year]["value"]


def scope1_emissions_getter(df: pd.DataFrame, metric: str, as_ton: bool = True) -> float:
    """Function to get the Scope 1 Emissions value at a particular year.

    Args:
        df (pd.DataFrame): The DataFrame containing the Scope 1 Emissions metrics and values.
        metric (str): The metric you are querying.
        as_ton (bool): Convert from kg to ton. Defaults to True.

    Returns:
        float: The value of the Scope 1 Emission Metric at a particular year.
    """
    df_c = df.copy()
    df_c.set_index(["Metric"], inplace=True)
    values = df_c.loc[metric]["Value"]
    if as_ton:
        return values / 1000
    return values


def ccs_co2_getter(df: pd.DataFrame, metric: str, year: int) -> float:
    """Function to get the CCS CO2 value at a particular year.

    Args:
        df (pd.DataFrame): The DataFrame containing the CCS & CO2 figures.
        metric (str): The metric you are querying (CCS or CO2).
        year (int): The year that you want to query.

    Returns:
        float: The value of the metric at a particular year.
    """
    year = min(MODEL_YEAR_END, year)
    df_c = df.copy()
    df_c.set_index(["Metric", "Year"], inplace=True)
    return df_c.loc[metric, year]["Value"]


def static_energy_prices_getter(df: pd.DataFrame, metric: str, year: str) -> float:
    """Function to get the static energy price at a particular year.

    Args:
        df (pd.DataFrame): A DataFrame containing the static energy metrics and prices.
        metric (str): The metric you are querying.
        year (str): The year that you want to query.

    Returns:
        float: The value of the metric at a particular year.
    """
    year = min(MODEL_YEAR_END, year)
    df_c = df.copy()
    df_c.set_index(["Metric", "Year"], inplace=True)
    return df_c.loc[metric, year]["Value"]


@pa.check_input(ETHANOL_PLASTIC_CHARCOAL_SCHEMA)
def format_commodities_data(df: pd.DataFrame, material_mapper: dict) -> pd.DataFrame:
    """Formats the Commodities dataset.

    Args:
        df (pd.DataFrame): A DataFrame containing the commodities data.
        material_mapper (dict): A dictionary mapping the material to the commoodity code to the commodity.

    Returns:
        pd.DataFrame: A DataFrame of the formatted commmodities data.
    """
    df_c = df.copy()
    logger.info("Formatting the ethanol_plastics_charcoal data")
    columns_of_interest = [
        "Year",
        "Reporter",
        "Commodity Code",
        "Netweight (kg)",
        "Trade Value (US$)",
    ]
    df_c = df_c[columns_of_interest]
    df_c.columns = ["year", "reporter", "commodity_code", "netweight", "trade_value"]
    df_c["commodity_code"] = df_c["commodity_code"].apply(
        lambda x: material_mapper[str(x)]
    )
    df_c["implied_price"] = ""
    df_c["netweight"].fillna(0, inplace=True)
    for row in df_c.itertuples():
        if row.netweight == 0:
            df_c.loc[row.Index, "implied_price"] = 0
        else:
            df_c.loc[row.Index, "implied_price"] = row.trade_value / row.netweight
    return df_c


def commodity_data_getter(df: pd.DataFrame, commodity: str = None) -> float:
    """A getter function for the commodities data.

    Args:
        df (pd.DataFrame): A DataFrame containing the preprocessed commodities data.
        commodity (str, optional): The commodity you want to get the value for. Defaults to None.

    Returns:
        float: The value of the commodity data for the parameters you have entered.
    """
    df_c = df.copy()
    if commodity:
        df_c = df_c[df_c["commodity_code"] == commodity]
        value_productsum = (
            sum(df_c["netweight"] * df_c["implied_price"]) / df_c["netweight"].sum()
        )
        return value_productsum
    else:
        values_dict = {}
        for commodity_ref in list(COMMODITY_MATERIAL_MAPPER.values()):
            new_df = df_c[df_c["commodity_code"] == commodity_ref]
            value_productsum = (
                sum(new_df["netweight"] * new_df["implied_price"])
                / new_df["netweight"].sum()
            )
            values_dict[commodity_ref] = value_productsum
    return values_dict


def scope3_ef_getter(df: pd.DataFrame, fuel: str, year: str) -> float:
    """A getter function for Scope 3 Emissions Factors

    Args:
        df (pd.DataFrame): A DataFrame containing the S3 Emissions Factors.
        fuel (str): The fuel you would like to get a value for.
        year (str): The year you would like to get a value for.

    Returns:
        float: The value of the S3 Emissions Factors data for the parameters you have entered.
    """
    df_c = df.copy()
    df_c.set_index(["Fuel", "Year"], inplace=True)
    return df_c.loc[fuel, year]["value"]


@timer_func
def format_business_cases(serialize: bool):
    bc_df = read_pickle_folder(
        PKL_DATA_IMPORTS, "excel_business_cases"
    )
    bc_df =  bc_df.melt(id_vars=['Material', 'Type of metric', 'Unit'], var_name='technology', value_name='value').copy()
    bc_df.rename({'Material': 'material_category', 'Type of metric': 'metric_type', 'Unit': 'unit'}, axis=1, inplace=True)
    if serialize:
        serialize_file(bc_df, PKL_DATA_FORMATTED, "standardised_business_cases")
    return bc_df


@timer_func
def create_capex_opex_dict(scenario_dict: dict, serialize: bool = False) -> dict:
    """Creates a Dictionary containing Greenfield, Brownfield and Opex values.

    Args:
        serialize (bool, optional): Flag to serialize the dictionary. Defaults to False.

    Returns:
        dict: A dictionary containing the capex/opex values.
    """
    eur_to_usd_rate = scenario_dict['eur_to_usd']
    greenfield_capex_df = read_pickle_folder(PKL_DATA_IMPORTS, "greenfield_capex")
    brownfield_capex_df = read_pickle_folder(PKL_DATA_IMPORTS, "brownfield_capex")
    other_opex_df = read_pickle_folder(PKL_DATA_IMPORTS, "other_opex")
    capex_dict = capex_dictionary_generator(
        greenfield_capex_df, brownfield_capex_df, other_opex_df, eur_to_usd_rate
    )
    if serialize:
        serialize_file(capex_dict, PKL_DATA_FORMATTED, "capex_dict")
    return capex_dict

@timer_func
def generate_preprocessed_emissions_data(
    scenario_dict: dict = None, serialize: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Complete process flow for the preprocessed emissivity data.

    Args:
        serialize (bool, optional): Flag to serialize the emissivity data. Defaults to False.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple of the commodities data and S3 emission factors data.
    """
    ethanol_plastic_charcoal = read_pickle_folder(
        PKL_DATA_IMPORTS, "ethanol_plastic_charcoal"
    )
    commodities_df = format_commodities_data(
        ethanol_plastic_charcoal, COMMODITY_MATERIAL_MAPPER
    )
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
        s3_emissions_factors_1, slag_new_values, ENERGY_DENSITY_MET_COAL_MJ_KG
    )
    if serialize:
        serialize_file(commodities_df, PKL_DATA_FORMATTED, "commodities_df")
        serialize_file(final_scope3_ef_df, PKL_DATA_FORMATTED, "final_scope3_ef_df")
    return commodities_df, final_scope3_ef_df


def format_bc(df: pd.DataFrame) -> pd.DataFrame:
    """Formula to format the standardised business cases DataFrame.

    Args:
        df (pd.DataFrame): The standardised business cases.

    Returns:
        pd.DataFrame: The formatted standardised business cases.
    """
    df_c = df.copy()
    df_c["material_category"] = df_c["material_category"].apply(lambda x: x.strip())
    return df_c


def load_business_cases() -> pd.DataFrame:
    """Loads the standardised business cases and returns the formatted DataFrame.

    Returns:
        pd.DataFrame: The formatted standardised business cases.
    """
    standardised_business_cases = read_pickle_folder(
        PKL_DATA_FORMATTED, "standardised_business_cases", "df"
    )
    return format_bc(standardised_business_cases)


def load_materials() -> list:
    """Loads the standarised and formatted business cases and returns the unique materials.

    Returns:
        list: A list of all untque materials in the business cases.
    """
    return load_business_cases()["material_category"].unique().tolist()
