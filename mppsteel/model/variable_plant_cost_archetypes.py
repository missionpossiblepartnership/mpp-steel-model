"""Script to determine the variable plant cost types dependent on regions."""

import itertools
import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    PKL_DATA_FORMATTED,
    USD_TO_EUR_CONVERSION_DEFAULT,
    MODEL_YEAR_END,
    PKL_DATA_IMPORTS,
    MODEL_YEAR_START,
)
from mppsteel.config.reference_lists import RESOURCE_CATEGORY_MAPPER
from mppsteel.utility.utils import cast_to_float
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.dataframe_utility import convert_currency_col
from mppsteel.data_loading.data_interface import (
    commodity_data_getter,
    static_energy_prices_getter,
)
from mppsteel.data_loading.pe_model_formatter import (
    pe_model_data_getter,
    POWER_HYDROGEN_COUNTRY_MAPPER,
    BIO_COUNTRY_MAPPER,
    CCUS_COUNTRY_MAPPER
)

# Create logger
logger = get_logger(__name__)

def generate_feedstock_dict(eur_to_usd_rate: float = 1 / USD_TO_EUR_CONVERSION_DEFAULT) -> dict:
    """Creates a feedstock dictionary that combines all non-energy model commodities into one dictionary.
    The dictionary has a pairing of the commodity name and the price.

    Returns:
        dict: A dictionary containing the pairing of feedstock name and price.
    """
    commodities_df = read_pickle_folder(PKL_DATA_FORMATTED, "commodities_df", "df")
    feedstock_prices = read_pickle_folder(PKL_DATA_IMPORTS, "feedstock_prices", "df")
    feedstock_prices = convert_currency_col(feedstock_prices, 'Value', eur_to_usd_rate)
    commodities_dict = commodity_data_getter(commodities_df)
    commodity_dictname_mapper = {
        "plastic": "Plastic waste",
        "ethanol": "Ethanol",
        "charcoal": "Charcoal",
    }
    for key in commodity_dictname_mapper:
        commodities_dict[commodity_dictname_mapper[key]] = commodities_dict.pop(key)
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
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    eur_to_usd_rate = scenario_dict['eur_to_usd']

    steel_plants = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
    steel_plant_region_ng_dict = (
        steel_plants[["country_code", "cheap_natural_gas"]]
        .set_index("country_code")
        .to_dict()["cheap_natural_gas"]
    )
    rmi_mapper = create_country_mapper()
    power_grid_prices_formatted = read_pickle_folder(
        intermediate_path, "power_grid_prices_formatted", "df"
    )
    hydrogen_prices_formatted = read_pickle_folder(
        intermediate_path, "hydrogen_prices_formatted", "df"
    )
    bio_price_model_formatted = read_pickle_folder(
        intermediate_path, "bio_price_model_formatted", "df"
    )
    ccus_transport_model_formatted = read_pickle_folder(
        intermediate_path, "ccus_transport_model_formatted", "df"
    )
    ccus_storage_model_formatted = read_pickle_folder(
        intermediate_path, "ccus_storage_model_formatted", "df"
    )
    business_cases = read_pickle_folder(
        PKL_DATA_FORMATTED, "standardised_business_cases", "df"
    ).reset_index()
    static_energy_prices = read_pickle_folder(
        PKL_DATA_IMPORTS, "static_energy_prices", "df"
    )[["Metric", "Year", "Value"]]
    static_energy_prices = convert_currency_col(static_energy_prices, 'Value', eur_to_usd_rate)
    feedstock_dict = generate_feedstock_dict(eur_to_usd_rate)
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    steel_plant_country_codes = list(steel_plants["country_code"].unique())
    product_range_year_country = list(itertools.product(year_range, steel_plant_country_codes))

    ccus_transport_ref = {}
    ccus_ccus_storage_ref = {}
    for country_code in tqdm(
        steel_plant_country_codes,
        total=len(steel_plant_country_codes),
        desc="CCUS Ref Loop",
    ):
        ccus_transport_ref[country_code] = pe_model_data_getter(
            ccus_transport_model_formatted,
            rmi_mapper,
            CCUS_COUNTRY_MAPPER,
            region_code=country_code
        )
        ccus_ccus_storage_ref[country_code] = pe_model_data_getter(
            ccus_storage_model_formatted,
            rmi_mapper,
            CCUS_COUNTRY_MAPPER,
            region_code=country_code
        )

    electricity_ref = {}
    hydrogen_ref = {}
    bio_ref = {}
    for year, country_code in tqdm(
        product_range_year_country,
        total=len(product_range_year_country),
        desc="PE Model Ref Loop",
    ):
        electricity_ref[(year, country_code)] = pe_model_data_getter(
            power_grid_prices_formatted,
            rmi_mapper,
            POWER_HYDROGEN_COUNTRY_MAPPER,
            year,
            country_code,
        )
        hydrogen_ref[(year, country_code)] = pe_model_data_getter(
            hydrogen_prices_formatted,
            rmi_mapper,
            POWER_HYDROGEN_COUNTRY_MAPPER,
            year,
            country_code,
        )
        bio_ref[(year, country_code)] = pe_model_data_getter(
            bio_price_model_formatted,
            rmi_mapper,
            BIO_COUNTRY_MAPPER,
            year,
            country_code,
        )

    df_list = []
    for year, country_code in tqdm(product_range_year_country, total=len(product_range_year_country), desc="Variable Cost Loop"):
        df = generate_variable_costs(
            year=year,
            country_code=country_code,
            business_cases_df=business_cases,
            ng_dict=steel_plant_region_ng_dict,
            feedstock_dict=feedstock_dict,
            static_energy_df=static_energy_prices,
            electricity_ref=electricity_ref,
            hydrogen_ref=hydrogen_ref,
            bio_ref=bio_ref,
            ccus_ccus_storage_ref=ccus_ccus_storage_ref,
            ccus_transport_ref=ccus_transport_ref
        )
        df_list.append(df)

    df = pd.concat(df_list).reset_index(drop=True)
    df['cost_type'] = df['material_category'].apply(
        lambda material: RESOURCE_CATEGORY_MAPPER[material])
    return df

def vc_mapper(row: str, feedstock_dict: dict, static_energy_df: pd.DataFrame, regional_prices: dict, ng_flag: int, static_year: int):
    
    if RESOURCE_CATEGORY_MAPPER[row.material_category] == 'Fossil Fuels':
        if (row.material_category == "Natural gas") and (ng_flag == 1):
            return row.value * static_energy_prices_getter(static_energy_df, "Natural gas - low", static_year)
        elif (row.material_category == "Natural gas") and (ng_flag == 0):
            return row.value * static_energy_prices_getter(static_energy_df, "Natural gas - high", static_year)
        elif row.material_category == "Plastic waste":
            return row.value * feedstock_dict[row.material_category]
        return row.value * static_energy_prices_getter(static_energy_df, row.material_category, static_year)

    if RESOURCE_CATEGORY_MAPPER[row.material_category] == 'Feedstock':
        return row.value * feedstock_dict[row.material_category]

    if RESOURCE_CATEGORY_MAPPER[row.material_category] == 'Bio Fuels':
        return row.value * regional_prices['bio_price']

    if row.material_category == "Electricity":
        return row.value * regional_prices['electricity_price']

    if row.material_category == "Hydrogen":
        return row.value * regional_prices['hydrogen_price']

    if RESOURCE_CATEGORY_MAPPER[row.material_category] == 'Other Opex':
        if row.material_category in {'BF slag', 'Other slag'}:
            price = feedstock_dict[row.material_category]
        elif row.material_category == "Steam":
            price = static_energy_prices_getter(
                static_energy_df, row.material_category, static_year
            )
        return row.value * price

    if RESOURCE_CATEGORY_MAPPER[row.material_category] == 'CCS':
        return row.value * (regional_prices['ccus_storage_price'] + regional_prices['ccus_transport_price'])
    
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
    ccus_ccus_storage_ref: pd.DataFrame = None,
    ccus_transport_ref: pd.DataFrame = None,
) -> pd.DataFrame:
    """Generates a DataFrame based on variable cost parameters for a particular region passed to it.

    Args:
        business_cases_df (pd.DataFrame): A DataFrame of standardised variable costs.
        country_code (str): The country code that you want to get energy assumption prices for.
        ng_flag (int): A flag for whether a particular country contains natural gas
        feedstock_dict (dict, optional): A dictionary containing feedstock resources and prices. Defaults to None.
        static_energy_df (pd.DataFrame, optional): A DataFrame containing static energy prices. Defaults to None.
        power_df (pd.DataFrame, optional): The shared MPP Power assumptions model. Defaults to None.
        hydrogen_df (pd.DataFrame, optional): The shared MPP Hydrogen assumptions model. Defaults to None.
        bio_df (pd.DataFrame, optional): The shared MPP Bio assumptions model. Defaults to None.
        ccus_storage (pd.DataFrame, optional): The shared MPP CCUS assumptions model. Defaults to None.
        ccus_transport (pd.DataFrame, optional): The shared MPP CCUS assumptions model. Defaults to None.
    Returns:
        pd.DataFrame: A DataFrame containing variable costs for a particular region.
    """
    df_c = business_cases_df.copy()
    static_year = min(2026, year)
    regional_price_dict = {
        'electricity_price': electricity_ref[(year, country_code)],
        'hydrogen_price': hydrogen_ref[(year, country_code)],
        'bio_price': bio_ref[(year, country_code)],
        'ccus_transport_price': ccus_transport_ref[(country_code)],
        'ccus_storage_price': ccus_ccus_storage_ref[(country_code)]
    }
    ng_flag = ng_dict[country_code]
    df_c['cost'] = df_c.apply(
        vc_mapper, 
        feedstock_dict=feedstock_dict,
        static_energy_df=static_energy_df,
        regional_prices=regional_price_dict,
        ng_flag=ng_flag,
        static_year=static_year,
        axis=1
    )
    df_c["year"] = year
    df_c["country_code"] = country_code
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
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
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
