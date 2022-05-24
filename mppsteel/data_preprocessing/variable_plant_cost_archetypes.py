"""Script to determine the variable plant cost types dependent on regions."""

import itertools
import pandas as pd
from tqdm import tqdm
from pathlib import Path

from mppsteel.config.model_config import (
    PKL_DATA_FORMATTED,
    TON_TO_KILOGRAM_FACTOR,
    MODEL_YEAR_RANGE,
    PKL_DATA_IMPORTS,
)
from mppsteel.config.reference_lists import RESOURCE_CATEGORY_MAPPER
from mppsteel.utility.utils import cast_to_float
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file,
    get_scenario_pkl_path,
)
from mppsteel.model_tests.df_tests import (
    test_negative_df_values,
    test_negative_list_values,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.dataframe_utility import convert_currency_col

# Create logger
logger = get_logger(__name__)


def generate_feedstock_dict(eur_to_usd_rate: float, project_dir=None) -> dict:
    """Creates a feedstock dictionary that combines all non-energy model commodities into one dictionary.
    The dictionary has a pairing of the commodity name and the price.

    Args:
        eur_to_usd (float): The rate used ot convert EUR values to USD.

    Returns:
        dict: A dictionary containing the pairing of feedstock name and price.
    """

    def standardise_units(row):
        return (
            row.Value * TON_TO_KILOGRAM_FACTOR
            if row.Metric in {"BF slag", "Other slag"}
            else row.Value
        )
    if project_dir is not None:
        feedstock_prices = read_pickle_folder(project_dir / PKL_DATA_IMPORTS, "feedstock_prices", "df")
    else:
        feedstock_prices = read_pickle_folder(PKL_DATA_IMPORTS, "feedstock_prices", "df")
    feedstock_prices = convert_currency_col(feedstock_prices, "Value", eur_to_usd_rate)
    feedstock_prices["Value"] = feedstock_prices.apply(standardise_units, axis=1)
    if project_dir is not None:
        commodities_df = read_pickle_folder(project_dir / PKL_DATA_FORMATTED, "commodities_df", "df")
    else:
        commodities_df = read_pickle_folder(PKL_DATA_FORMATTED, "commodities_df", "df")
    commodities_dict = {
        "Plastic waste": sum(
            commodities_df["netenergy_gj"] * commodities_df["implied_price"]
        )
        / commodities_df["netenergy_gj"].sum()
    }
    return {
        **commodities_dict,
        **dict(zip(feedstock_prices["Metric"], feedstock_prices["Value"])),
    }


def plant_variable_costs(scenario_dict: dict) -> pd.DataFrame:
    """Creates a DataFrame reference of each plant's variable cost.

    Args:
        scenario_dict (dict): Dictionary with Scenarios.

    Returns:
        pd.DataFrame: A DataFrame containing each plant's variable costs.
    """
    intermediate_path = get_scenario_pkl_path(
        scenario_dict["scenario_name"], "intermediate"
    )
    eur_to_usd_rate = scenario_dict["eur_to_usd"]

    steel_plants = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
    steel_plant_region_ng_dict = (
        steel_plants[["country_code", "cheap_natural_gas"]]
        .set_index("country_code")
        .to_dict()["cheap_natural_gas"]
    )
    power_grid_prices_ref = read_pickle_folder(
        intermediate_path, "power_grid_prices_ref", "df"
    )
    h2_prices_ref = read_pickle_folder(intermediate_path, "h2_prices_ref", "df")
    bio_model_prices_ref = read_pickle_folder(
        intermediate_path, "bio_model_prices_ref", "df"
    )
    ccs_model_transport_ref = read_pickle_folder(
        intermediate_path, "ccs_model_transport_ref", "df"
    )
    ccs_model_storage_ref = read_pickle_folder(
        intermediate_path, "ccs_model_storage_ref", "df"
    )
    business_cases = read_pickle_folder(
        PKL_DATA_FORMATTED, "standardised_business_cases", "df"
    ).reset_index()
    static_energy_prices = read_pickle_folder(
        PKL_DATA_IMPORTS, "static_energy_prices", "df"
    )[["Metric", "Year", "Value"]]
    static_energy_prices.set_index(["Metric", "Year"], inplace=True)
    feedstock_dict = generate_feedstock_dict(eur_to_usd_rate)
    steel_plant_country_codes = list(steel_plants["country_code"].unique())
    product_range_year_country = list(
        itertools.product(MODEL_YEAR_RANGE, steel_plant_country_codes)
    )
    df_list = []
    for year, country_code in tqdm(
        product_range_year_country,
        total=len(product_range_year_country),
        desc="Variable Cost Loop",
    ):
        df = generate_variable_costs(
            year=year,
            country_code=country_code,
            business_cases_df=business_cases,
            ng_dict=steel_plant_region_ng_dict,
            feedstock_dict=feedstock_dict,
            static_energy_df=static_energy_prices,
            electricity_ref=power_grid_prices_ref,
            hydrogen_ref=h2_prices_ref,
            bio_ref=bio_model_prices_ref,
            ccs_storage_ref=ccs_model_storage_ref,
            ccs_transport_ref=ccs_model_transport_ref,
        )
        df_list.append(df)

    df = pd.concat(df_list).reset_index(drop=True)
    df["cost_type"] = df["material_category"].apply(
        lambda material: RESOURCE_CATEGORY_MAPPER[material]
    )
    return df[df["cost_type"] != "Emissivity"].copy()


def vc_mapper(
    row: pd.Series,
    country_code: str,
    year: int,
    static_year: int,
    electricity_ref: dict,
    hydrogen_ref: dict,
    bio_ref: dict,
    ccs_transport_ref: dict,
    ccs_storage_ref: dict,
    feedstock_dict: dict,
    static_energy_df: pd.DataFrame,
    ng_flag: int,
) -> float:
    """_summary_

    Args:
        row (pd.Series): A Series containing the consumption rates for each technology and resource.
        country_code (str): The country code to create a reference for.
        year (int): The year to create a reference for.
        static_year (int): The static year to be used for datasets that end after a certain year.
        electricity_ref (dict): The electricity grid price reference dict.
        hydrogen_ref (dict): The hydrogen price reference dict.
        bio_ref (dict): The bio price reference dict.
        ccs_transport_ref (dict): The ccs transport price reference dict.
        ccs_storage_ref (dict): The ccs storage price reference dict.
        feedstock_dict (dict): The feedstock price reference dict.
        static_energy_df (pd.DataFrame): The static energy reference dict.
        ng_flag (int): The natural gas flag price.

    Returns:
        float: A float value containing the variable cost value to assign.
    """
    if row.material_category == "Electricity":
        return row.value * electricity_ref[(year, country_code)]

    elif row.material_category == "Hydrogen":
        return row.value * hydrogen_ref[(year, country_code)]

    elif RESOURCE_CATEGORY_MAPPER[row.material_category] == "Bio Fuels":
        return row.value * bio_ref[(year, country_code)]

    elif RESOURCE_CATEGORY_MAPPER[row.material_category] == "CCS":
        return row.value * (
            ccs_transport_ref[country_code] + ccs_storage_ref[country_code]
        )

    elif (row.material_category == "Natural gas") and (ng_flag == 1):
        return (
            row.value * static_energy_df.loc["Natural gas - low", static_year]["Value"]
        )

    elif (row.material_category == "Natural gas") and (ng_flag == 0):
        return (
            row.value * static_energy_df.loc["Natural gas - high", static_year]["Value"]
        )

    elif row.material_category == "Plastic waste":
        return row.value * feedstock_dict["Plastic waste"]

    elif (RESOURCE_CATEGORY_MAPPER[row.material_category] == "Fossil Fuels") and (
        row.material_category not in ["Natural gas", "Plastic waste"]
    ):
        return (
            row.value
            * static_energy_df.loc[row.material_category, static_year]["Value"]
        )

    elif RESOURCE_CATEGORY_MAPPER[row.material_category] == "Feedstock":
        return row.value * feedstock_dict[row.material_category]

    elif RESOURCE_CATEGORY_MAPPER[row.material_category] == "Other Opex":
        if row.material_category in {"BF slag", "Other slag"}:
            return row.value * feedstock_dict[row.material_category]

        elif row.material_category == "Steam":
            return row.value * static_energy_df.loc["Steam", static_year]["Value"]
    return 0


def generate_variable_costs(
    year: int,
    country_code: str,
    business_cases_df: pd.DataFrame,
    ng_dict: dict,
    feedstock_dict: dict = None,
    static_energy_df: pd.DataFrame = None,
    electricity_ref: pd.DataFrame = None,
    hydrogen_ref: pd.DataFrame = None,
    bio_ref: pd.DataFrame = None,
    ccs_storage_ref: pd.DataFrame = None,
    ccs_transport_ref: pd.DataFrame = None,
) -> pd.DataFrame:
    """Generates a DataFrame based on variable cost parameters for a particular region passed to it.

    Args:
        year (int): The year you want to create variable costs for.
        country_code (str): The country code that you want to get energy assumption prices for.
        business_cases_df (pd.DataFrame): A DataFrame of standardised variable costs.
        ng_dict (dict): A dictionary of country_codes as keys and ng_flags as values for whether a particular country contains natural gas
        feedstock_dict (dict, optional): A dictionary containing feedstock resources and prices. Defaults to None.
        static_energy_df (pd.DataFrame, optional): A DataFrame containing static energy prices. Defaults to None.
        electricity_ref (pd.DataFrame, optional): The shared MPP Power assumptions model. Defaults to None.
        hydrogen_ref (pd.DataFrame, optional): The shared MPP Hydrogen assumptions model. Defaults to None.
        bio_ref (pd.DataFrame, optional): The shared MPP Bio assumptions model. Defaults to None.
        ccs_storage_ref (pd.DataFrame, optional): The shared MPP CCS assumptions model. Defaults to None.
        ccs_transport_ref (pd.DataFrame, optional): The shared MPP CCS assumptions model. Defaults to None.
    Returns:
        pd.DataFrame: A DataFrame containing variable costs for a particular region.
    """
    df_c = business_cases_df.copy()
    static_year = min(2026, year)
    ng_flag = ng_dict[country_code]
    df_c["year"] = year
    df_c["country_code"] = country_code
    df_c["cost"] = df_c.apply(
        vc_mapper,
        country_code=country_code,
        year=year,
        static_year=static_year,
        electricity_ref=electricity_ref,
        hydrogen_ref=hydrogen_ref,
        bio_ref=bio_ref,
        ccs_transport_ref=ccs_transport_ref,
        ccs_storage_ref=ccs_storage_ref,
        feedstock_dict=feedstock_dict,
        static_energy_df=static_energy_df,
        ng_flag=ng_flag,
        axis=1,
    )
    return df_c


def format_variable_costs(
    variable_cost_df: pd.DataFrame, group_data: bool = True
) -> pd.DataFrame:
    """Formats a Variable Costs DataFrame generated via the plant_variable_costs function.

    Args:
        variable_cost_df (pd.DataFrame): A DataFrame generated from the plant_variable_costs function.
        group_data (bool, optional): Boolean flag that groups data by "country_code", "year", "technology". Defaults to True.

    Returns:
        pd.DataFrame: A formatted variable costs DataFrame.
    """

    df_c = variable_cost_df.copy()
    df_c.reset_index(drop=True, inplace=True)
    if group_data:
        df_c.drop(
            ["material_category", "unit", "cost_type", "value"], axis=1, inplace=True
        )
        df_c = (
            df_c.groupby(by=["country_code", "year", "technology"])
            .sum()
            .sort_values(by=["country_code", "year", "technology"])
        )
        df_c["cost"] = df_c["cost"].apply(lambda x: cast_to_float(x))
        return df_c
    return df_c


@timer_func
def generate_variable_plant_summary(
    scenario_dict: dict, serialize: bool = False
) -> pd.DataFrame:
    """The complete flow for creating variable costs.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the variable plant results.
    """
    intermediate_path = get_scenario_pkl_path(
        scenario_dict["scenario_name"], "intermediate"
    )
    variable_costs = plant_variable_costs(scenario_dict)
    variable_costs_summary = format_variable_costs(variable_costs)
    variable_costs_summary_material_breakdown = format_variable_costs(
        variable_costs, group_data=False
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(
            variable_costs_summary, intermediate_path, "variable_costs_regional"
        )
        serialize_file(
            variable_costs_summary_material_breakdown,
            intermediate_path,
            "variable_costs_regional_material_breakdown",
        )
    return variable_costs_summary
