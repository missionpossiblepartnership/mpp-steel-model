"""Formats Price & Emissions Model Data and defines getter functions"""


import contextlib
import pandas as pd
from tqdm import tqdm
from typing import Iterable

from mppsteel.config.model_config import (
    EXAJOULE_TO_GIGAJOULE,
    GIGAJOULE_TO_MEGAJOULE_FACTOR,
    GIGATON_TO_MEGATON_FACTOR,
    INITIAL_MET_COAL_PRICE_USD_PER_GJ,
    MEGATON_TO_TON,
    MMBTU_TO_GJ,
    MODEL_YEAR_END,
    THERMAL_COAL_ENERGY_DENSITY,
    TON_TO_KILOGRAM_FACTOR,
    MEGAWATT_HOURS_TO_GIGAJOULES,
    MODEL_YEAR_RANGE,
    PKL_DATA_IMPORTS,
    HYDROGEN_ENERGY_DENSITY_MJ_PER_KG,
)
from mppsteel.config.model_scenarios import (
    CCS_CAPACITY_SCENARIOS,
    COST_SCENARIO_MAPPER,
    FOSSIL_FUEL_SCENARIOS,
    GRID_DECARBONISATION_SCENARIOS,
    BIOMASS_SCENARIOS,
    CCS_SCENARIOS,
)
from mppsteel.config.reference_lists import (
    ALL_EUROPE_COUNTRIES,
    CENTRAL_ASIA_COUNTRIES,
    CIS_COUNTRIES,
    EASTERN_EUROPE_COUNTRIES,
    EU_COUNTRIES,
    JAPAN_SOUTHKOREA_TAIWAN,
    NAFTA_COUNTRIES,
    NORTH_ASIA_COUNTRIES,
    SOUTH_ASIA_COUNTRIES,
    SOUTHEAST_ASIA_COUNTRIES,
    WESTERN_EUROPE_COUNTRIES
)
from mppsteel.utility.dataframe_utility import extend_df_years
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import (
    expand_melt_and_sort_years,
    convert_currency_col,
)
from mppsteel.utility.location_utility import get_countries_from_group
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file,
    get_scenario_pkl_path,
)

from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)

RE_DICT = {
    "solar": "Price of onsite solar",
    "wind": "Price of onsite wind",
    "wind_and_solar": "Price of onsite wind + solar",
    "gas": "Price of onsite gas + ccs",
}

def power_hydrogen_region_reference_generator(country_ref: pd.DataFrame) -> dict:
    """Creates a dictionary reference for the CCS model by mapping each model region to a distinct country code.
    The order of regions needs to proceed from the broadest region to the individual country to ensure that
    the lowest level of detail is preserved in a later function.

    Args:
        country_ref (pd.DataFrame): The country ref used to map country codes to regions.

    Returns:
        dict: A dictionary containing a mapping key of [country_code] to CCS value.
    """
    row_countries = get_countries_from_group(
            country_ref, "RMI Model Region", "RoW"
        )
    oceania_countries = get_countries_from_group(
            country_ref, "Continent", "Oceania"
        )
    row_countries = set(row_countries).difference(
        oceania_countries
    )
    asia_countries = get_countries_from_group(
            country_ref, "Continent", "Asia"
        )
    asia_countries.append("TWN")
    separate_asia_countries = JAPAN_SOUTHKOREA_TAIWAN + ["CHN", "IND"]
    for country in separate_asia_countries:
        with contextlib.suppress(Exception):
            asia_countries.remove(country)
    return {
        "RoW": row_countries,
        "Asia": asia_countries,
        "South_BAU_Americas": get_countries_from_group(
            country_ref, "RMI Model Region", "South and Central America"
        ),
        "Middle_East": get_countries_from_group(
            country_ref, "RMI Model Region", "Middle East"
        ),
        "Africa": get_countries_from_group(
            country_ref, "RMI Model Region", "Africa"
        ),
        "AustralasiaOceania": get_countries_from_group(
            country_ref, "Continent", "Oceania"
        ),
        "Southeast_Asia": get_countries_from_group(
            country_ref, "RMI Model Region", "Southeast Asia"
        ),
        "EU": get_countries_from_group(
            country_ref, "RMI Model Region", "Europe"
        ),
        "Russia": get_countries_from_group(
            country_ref, "RMI Model Region", "CIS"
        ),
        "US": NAFTA_COUNTRIES,
        "Japan_SthKorea_Taiwan": JAPAN_SOUTHKOREA_TAIWAN,
        "China": ["CHN"],
        "India": ["IND"]
    }


def bio_region_reference_generator(country_ref: pd.DataFrame) -> dict:
    """Creates a dictionary reference for the Biomass model by mapping each model region to a distinct country code.
    The order of regions needs to proceed from the broadest region to the individual country to ensure that
    the lowest level of detail is preserved in a later function.

    Args:
        country_ref (pd.DataFrame): The country ref used to map country codes to regions.

    Returns:
        dict: A dictionary containing a mapping key of [year, country_code] to biomodel value.
    """
    return {
        "EU": get_countries_from_group(country_ref, "RMI Model Region", "Europe"),
        "South and central Americas": get_countries_from_group(
            country_ref, "RMI Model Region", "South and Central America"
        ),
        "Africa": get_countries_from_group(country_ref, "RMI Model Region", "Africa"),
        "CIS": get_countries_from_group(country_ref, "RMI Model Region", "CIS"),
        "Middle_East": get_countries_from_group(country_ref, "RMI Model Region", "Middle East"),
        "South Asia": get_countries_from_group(country_ref, "Region 1", "Southern Asia"),
        "Southeast_Asia": get_countries_from_group(country_ref, "Region 1", "South-eastern Asia"),
        "Western Europe": get_countries_from_group(country_ref, "Region 1", "Western Europe"),
        "East Europe": get_countries_from_group(country_ref, "Region 1", "Eastern Europe"),
        "Carribbean": get_countries_from_group(country_ref, "Region 1", "Caribbean"),
        "Central Asia": get_countries_from_group(country_ref, "Region 1", "Central Asia") + ["GEO", "TUR"],
        "AustralasiaOceania": get_countries_from_group(
            country_ref, "Continent", "Oceania"
        ),
        "North Asia": NORTH_ASIA_COUNTRIES,
        "Japan_SthKorea_Taiwan": JAPAN_SOUTHKOREA_TAIWAN,
        "NAFTA": NAFTA_COUNTRIES,
        "US": ["USA"],
        "India": ["IND"],
        "China": ["CHN"],
        "Russia": ["RUS"]
    }

def ccs_region_reference_generator(country_ref: pd.DataFrame) -> dict:
    """Creates a dictionary reference for the CCS model by mapping each model region to a distinct country code.
    The order of regions needs to proceed from the broadest region to the individual country to ensure that
    the lowest level of detail is preserved in a later function.

    Args:
        country_ref (pd.DataFrame): The country ref used to map country codes to regions.

    Returns:
        dict: A dictionary containing a mapping key of [country_code] to CCS value.
    """
    return {
        "Global": get_countries_from_group(country_ref, "RMI Model Region", "RoW"),
        "Europe": get_countries_from_group(country_ref, "RMI Model Region", "Europe"),
        "Africa": get_countries_from_group(country_ref, "RMI Model Region", "Africa"),
        "Middle East": get_countries_from_group(
            country_ref, "RMI Model Region", "Middle East"
        ),
        "Other Latin America": get_countries_from_group(
            country_ref, "RMI Model Region", "South and Central America"
        ),
        "Other Eurasia ": get_countries_from_group(
            country_ref, "RMI Model Region", "CIS"
        ),
        "Dynamic Asia": get_countries_from_group(
            country_ref, "RMI Model Region", "Southeast Asia"
        ),
        "Other East Asia": get_countries_from_group(
            country_ref, "RMI Model Region", "RoW"
        ),
        "Japan": ["JPN"],
        "Korea": ["KOR", "PRK"],
        "US": ["USA"],
        "China": ["CHN", "TWN"],
        "India": ["IND"],
        "Mexico": ["MEX"],
        "Canada": ["CAN"],
        "Russia": ["RUS"],
        "Indonesia": ["IDN"],
        "Brazil": ["BRA"]
    }


CCS_CAPACITY_REGION_MAPPER = {
    "China": ["China"],
    "Europe": ["Europe"],
    "Latin America": ["South and Central America"],
    "Middle East": ["Middle East"],
    "North America": ["NAFTA"],
    "Rest of Asia and Pacific": ["RoW"],
    "North + South Korea": ["Southeast Asia"],
    "Japan": ["Japan, South Korea, and Taiwan"],
    "India": ["India"],
    "Russia": ["CIS"],
    "Africa": ["Africa"],
}


def fossil_fuel_region_reference_generator(country_ref: pd.DataFrame) -> dict:
    """Creates a dictionary reference for the Fossil Fuel model by mapping each model region to a distinct country code.
    The order of regions needs to proceed from the broadest region to the individual country to ensure that
    the lowest level of detail is preserved in a later function.

    Args:
        country_ref (pd.DataFrame): The country ref used to map country codes to regions.

    Returns:
        dict: A dictionary containing a mapping key of [country_code] to Fossil Fuel value.
    """
    asia_and_pacific = get_countries_from_group(
        country_ref, "RMI Model Region", "Southeast Asia"
        ) + get_countries_from_group(
            country_ref, "RMI Model Region", "RoW"
        )
    return {

        "North America": get_countries_from_group(
            country_ref, "RMI Model Region", "NAFTA"
        ),
        "Latin America": get_countries_from_group(
            country_ref, "RMI Model Region", "South and Central America"
        ),
        "Europe": get_countries_from_group(
            country_ref, "RMI Model Region", "Europe"
        ),
        "Russia": get_countries_from_group(
            country_ref, "RMI Model Region", "CIS"
        ),
        "Africa": get_countries_from_group(
            country_ref, "RMI Model Region", "Africa"
        ),
        "Middle East": get_countries_from_group(
            country_ref, "RMI Model Region", "Middle East"
        ),
        "Rest of Asia and Pacific": asia_and_pacific,
        "Australasia/Oceania": asia_and_pacific,
        "China": get_countries_from_group(
            country_ref, "RMI Model Region", "China"
        ),
        "India": get_countries_from_group(
            country_ref, "RMI Model Region", "India"
        ),
        "Japan": get_countries_from_group(
            country_ref, "RMI Model Region", "Japan, South Korea, and Taiwan"
        )
}

def get_intersection_of_ordered_list(ordered_list: Iterable, mapping_list: Iterable) -> list:
    return [x for x in mapping_list if x in frozenset(ordered_list)] 


def final_mapper(
    model: pd.DataFrame, reference_mapper: dict, year_range: range = None
) -> dict:
    """Helper function that creates a dictionary mapping of of country codes to regions and optionally a year range to a model's values.

    Args:
        model (pd.DataFrame): The model used to create a dict reference.
        reference_mapper (dict): The reference mapper of region to country codes.
        year_range (range, optional): The year range of the model incase the model varies by year. Defaults to None.

    Returns:
        dict: The dictionary mapping of the model with a key of [country_code] to value or [year, country_code] to value if `year_range` is active.
    """
    final_mapper = {}
    if year_range:
        for year in tqdm(
            year_range,
            total=len(year_range),
            desc="Generating PE Model Reference Dictionary",
        ):
            ordered_region_list = get_intersection_of_ordered_list(
                reference_mapper.keys(), 
                model.index.get_level_values(1).unique()
            )
            for model_region in ordered_region_list:
                for country_code in reference_mapper[model_region]:
                    final_mapper[(year, country_code)] = model.loc[
                        (year, model_region), "value"
                    ]
    else:
        ordered_region_list = get_intersection_of_ordered_list(
            reference_mapper.keys(),
            model.index
        )
        for model_region in ordered_region_list:
            for country_code in reference_mapper[model_region]:
                final_mapper[country_code] = model.loc[model_region, "value"]
    return final_mapper


def fossil_fuel_mapper(
    model: pd.DataFrame, reference_mapper: dict, year_range
) -> dict:
    """Helper function that creates a dictionary mapping of of country codes to regions and optionally a year range to a model's values.

    Args:
        model (pd.DataFrame): The model used to create a dict reference.
        reference_mapper (dict): The reference mapper of region to country codes.
        year_range (range, optional): The year range of the model incase the model varies by year. Defaults to None.

    Returns:
        dict: The dictionary mapping of the model with a key of [country_code] to value or [year, country_code] to value if `year_range` is active.
    """
    final_mapper = {}
    resources = model['variable'].unique()

    for year in tqdm(
        year_range,
        total=len(year_range),
        desc="Generating PE Model Reference Dictionary",
    ):
        for resource in resources:
            model_r = model[model['variable'] == resource].copy()
            for model_region in set.intersection(set(reference_mapper.keys()), set(model_r.index.get_level_values(1).unique())):
                for country_code in reference_mapper[model_region]:
                    final_mapper[(year, country_code, resource)] = model_r.loc[
                        (year, model_region), "value"
                    ]
    for resource in resources:
        model_r = model[model['variable'] == resource].copy()
        for year in year_range:
            final_mapper[(year, "TWN", resource)] = model_r.loc[(year, "China"), "value"]
    return final_mapper



def model_reference_generator(
    model: pd.DataFrame,
    country_ref: pd.DataFrame,
    region_mapper: dict,
    year_range: range,
) -> dict:
    """Helper function that creates a dictionary mapping of of country codes to regions and optionally a year range to a model's values.

    Args:
        model (pd.DataFrame): The model used to create a dict reference.
        country_ref (pd.DataFrame): The country ref used to map country codes to regions.
        reference_mapper (dict): The reference mapper of region to country codes.
        year_range (range, optional): The year range of the model incase the model varies by year. Defaults to None.

    Returns:
        dict: The dictionary mapping of the model with a key of [country_code] to value or [year, country_code] to value if `year_range` is active.
    """
    final_mapper = {}
    for year in tqdm(
        year_range,
        total=len(year_range),
        desc="Generating PE Model Reference Dictionary",
    ):
        for model_region in model.index.get_level_values(1).unique():
            if model_region == "Global":
                final_mapper[(year, "GBL")] = model.loc[(year, model_region), "value"]
            else:
                for rmi_region in region_mapper[model_region]:
                    for country_code in get_countries_from_group(
                        country_ref, "RMI Model Region", rmi_region
                    ):
                        final_mapper[(year, country_code)] = model.loc[
                            (year, model_region), "value"
                        ]
    for year in year_range:
        final_mapper[(year, "TWN")] = model.loc[(year, "China"), "value"]
    return final_mapper


def subset_power(
    pdf: pd.DataFrame,
    scenario_dict: dict = None,
    customer: str = "Industry",
    grid_scenario: str = "BAU",
    cost_scenario: str = "Baseline",
    currency_conversion_factor: float = None,
    per_gj: bool = False,
) -> pd.DataFrame:
    """Subsets the power model according to scenario parameters passed from the scenario dict.

    Args:
        pdf (pd.DataFrame): The full reference power dataframe.
        scenario_dict (dict, optional): The scenario_dict containing the full scenario setting for the current model run. Defaults to None.
        customer (str, optional): The parameter setting for the customer column. Defaults to 'Industry'.
        grid_scenario (str, optional): The parameter setting for the grid_scenario. Defaults to 'Central'.
        cost_scenario (str, optional): The parameter setting for the cost_scenario. Defaults to 'Baseline'.
        currency_conversion_factor (float, optional): The currency conversion factor that converts one currency to another. Defaults to None.
        per_gj (bool, optional): A boolean flag that converts a metric from per megawatt hour to per gigajoule. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame of the subsetted model.
    """
    if scenario_dict:
        cost_scenario = COST_SCENARIO_MAPPER[scenario_dict["electricity_cost_scenario"]]
        grid_scenario = GRID_DECARBONISATION_SCENARIOS[scenario_dict["grid_scenario"]]
    pdf_c = pdf.copy()
    pdf_c = pdf_c[
        (pdf_c["Customer"] == customer)
        & (pdf_c["Grid scenario"] == grid_scenario)
        & (pdf_c["Cost scenario "] == cost_scenario)
    ]
    years = [year_col for year_col in pdf_c.columns if isinstance(year_col, int)]
    pdf_c = pdf_c.melt(
        id_vars=["Region", "Unit"],
        value_vars=years,
        var_name="year",
        value_name="value",
    )
    pdf_c.columns = [col.lower().strip() for col in pdf_c.columns]
    if currency_conversion_factor:
        pdf_c = convert_currency_col(pdf_c, "value", currency_conversion_factor)
    if per_gj:
        pdf_c["value"] = pdf_c["value"] / MEGAWATT_HOURS_TO_GIGAJOULES
    pdf_c = extend_df_years(pdf_c, "year", MODEL_YEAR_END)
    return pdf_c[["year", "region", "unit", "value"]].set_index(["year", "region"])


def subset_hydrogen(
    h2df: pd.DataFrame,
    scenario_dict: dict = None,
    prices: bool = False,
    variable: str = "H2 price",
    cost_scenario: str = "Min",
    prod_scenario: str = "Utility plant, dedicated VREs",
    currency_conversion_factor: float = None,
    price_per_gj: bool = False,
    emissions_per_gj: bool = False,
) -> pd.DataFrame:
    """Subsets the hydrogen model according to scenario parameters passed from the scenario dict.

    Args:
        h2df (pd.DataFrame): The full reference hydrogen dataframe.
        scenario_dict (dict, optional): The scenario_dict containing the full scenario setting for the current model run. Defaults to None.
        prices (bool, optional): A boolean flag to determine if the variable parameter will be used to subset the model's variable column.
        variable (str, optional): The parameter setting for the variable column. Defaults to 'H2 price'.
        cost_scenario (str, optional): The parameter setting for the cost_scenario column. Defaults to 'Min'.
        prod_scenario (str, optional): The parameter setting for the prod_scenario column. Defaults to 'Utility plant, dedicated VREs'.
        currency_conversion_factor (float, optional): The currency conversion factor that converts one currency to another. Defaults to None.
        price_per_gj (bool, optional): A boolean flag that converts the price metric from per kg to per gigajoule. Defaults to False.
        emissions_per_gj (bool, optional): A boolean flag that converts the emissions metric from kg to gigajoule. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame of the subsetted model.
    """
    if scenario_dict:
        cost_scenario = COST_SCENARIO_MAPPER[scenario_dict["hydrogen_cost_scenario"]]
    h2df_c = h2df.copy()
    h2df_c = h2df_c[
        (h2df_c["Cost scenario"] == cost_scenario)
        & (h2df_c["Production scenario"] == prod_scenario)
    ]
    if prices:
        h2df_c = h2df_c[(h2df_c["Variable"] == variable)]
    years = [year_col for year_col in h2df_c.columns if isinstance(year_col, int)]
    h2df_c = h2df_c.melt(
        id_vars=["Region", "Unit"],
        value_vars=years,
        var_name="year",
        value_name="value",
    ).copy()
    h2df_c.columns = [col.lower().strip() for col in h2df_c.columns]
    if currency_conversion_factor:
        h2df_c = convert_currency_col(h2df_c, "value", currency_conversion_factor)
    if price_per_gj:
        h2df_c["value"] = (
            h2df_c["value"] / HYDROGEN_ENERGY_DENSITY_MJ_PER_KG
        ) * GIGAJOULE_TO_MEGAJOULE_FACTOR
    if emissions_per_gj:
        h2df_c["value"] = (h2df_c["value"] / HYDROGEN_ENERGY_DENSITY_MJ_PER_KG) * (
            GIGAJOULE_TO_MEGAJOULE_FACTOR / TON_TO_KILOGRAM_FACTOR
        )
    h2df_c = extend_df_years(h2df_c, "year", MODEL_YEAR_END)
    return h2df_c[["year", "region", "unit", "value"]].set_index(["year", "region"])


def subset_fossil_fuel_prices(
    fossil_fuel_prices: pd.DataFrame,
    scenario_dict: dict = None,
    currency_conversion_factor: float = None,
    price_per_gj: bool = False
) -> pd.DataFrame:
    """Subsets the hydrogen model according to scenario parameters passed from the scenario dict.

    Args:
        fossil_fuel_prices (pd.DataFrame): The full reference for Fossil Fuel prices.
        scenario_dict (dict, optional): The scenario_dict containing the full scenario setting for the current model run. Defaults to None.
        currency_conversion_factor (float, optional): The currency conversion factor that converts one currency to another. Defaults to None.
        price_per_gj (bool, optional): A boolean flag that converts the price metric from per kg to per gigajoule. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame of the subsetted model.
    """
    def format_prices(df_c, resource_type, conversion_variable):
        result = df_c[df_c["variable"] == resource_type].copy()
        result["value"] = result["value"] / conversion_variable
        result["unit"] = "USD/GJ"
        return result
    df_c = fossil_fuel_prices.copy()
    df_c['Variable'] = df_c['Variable'].replace({'Coal':'Thermal coal'})
    if scenario_dict:
        price_scenario = FOSSIL_FUEL_SCENARIOS[scenario_dict["fossil_fuel_scenario"]]
        df_c = df_c[(df_c["Scenario"] == price_scenario)]
    years = [year_col for year_col in df_c.columns if year_col.isnumeric()]
    df_c = df_c.melt(
        id_vars=["Region", "Variable", "Unit"],
        value_vars=years,
        var_name="year",
        value_name="value",
    ).copy()
    df_c.columns = [col.lower().strip() for col in df_c.columns]
    df_c['year'] = df_c['year'].astype('int')
    met_coal = df_c[df_c['variable'] == 'Thermal coal'].copy()
    met_coal['variable'] = met_coal['variable'].replace({'Thermal coal':'Met coal'})
    mc_regions = met_coal['region'].unique()
    df_list = []
    for region in mc_regions:
        mc_r = met_coal[met_coal['region'] == region].copy()
        mc_r_values = mc_r['value'].values
        growth_series = mc_r_values / mc_r_values[0]
        new_values = INITIAL_MET_COAL_PRICE_USD_PER_GJ * growth_series
        mc_r['value'] = new_values
        df_list.append(mc_r)
    met_coal = pd.concat(df_list)
    df_c = pd.concat([df_c, met_coal])
    if currency_conversion_factor:
        df_c = convert_currency_col(df_c, "value", currency_conversion_factor)
    if price_per_gj:
        natural_gas = format_prices(df_c, "Natural gas", MMBTU_TO_GJ)
        thermal_coal = format_prices(df_c, "Thermal coal", THERMAL_COAL_ENERGY_DENSITY)
        met_coal = format_prices(df_c, "Met coal", 1)
        final_df = pd.concat([natural_gas, thermal_coal, met_coal])
    final_df = extend_df_years(final_df, "year", MODEL_YEAR_END)
    return final_df[["year", "region", "variable", "unit", "value"]].set_index(["year", "region"])


def subset_bio_prices(
    bdf: pd.DataFrame,
    scenario_dict: dict = None,
    cost_scenario: str = "Medium",
    growth_scenario: str = "Stable",
    feedstock_type: str = "Weighted Average",
    currency_conversion_factor: float = None,
) -> pd.DataFrame:
    """Subsets the Biomass prices model according to scenario parameters passed from the scenario dict.

    Args:
        bdf (pd.DataFrame): The biomass prices model
        scenario_dict (dict, optional): The scenario_dict containing the full scenario setting for the current model run. Defaults to None.
        cost_scenario (str, optional): The parameter setting for the cost_scenario column. Defaults to 'Medium'.
        growth_scenario (str, optional): The parameter setting for the growth_scenario column. Defaults to 'Stable'.
        feedstock_type (str, optional): The parameter setting for the column feedstock type. Defaults to 'Weighted average'.
        currency_conversion_factor (float, optional): The currency conversion factor that converts one currency to another. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame of the subsetted model.
    """
    if scenario_dict:
        cost_scenario = BIOMASS_SCENARIOS[scenario_dict["biomass_cost_scenario"]]
    bdf_c = bdf[
        (bdf["Price scenario"] == cost_scenario)
        & (bdf["Growth scenario"] == growth_scenario)
        & (bdf["Feedstock type"] == feedstock_type)
    ].copy()
    year_pairs = [(2020, 2030), (2030, 2040), (2040, 2050)]
    bdf_c = expand_melt_and_sort_years(bdf_c, year_pairs)
    bdf_c.columns = [col.lower().strip() for col in bdf_c.columns]
    if currency_conversion_factor:
        bdf_c = convert_currency_col(bdf_c, "value", currency_conversion_factor)
    bdf_c = extend_df_years(bdf_c, "year", MODEL_YEAR_END)
    return bdf_c[["year", "region", "unit", "value"]].set_index(["year", "region"])


def subset_bio_constraints(
    bdf: pd.DataFrame,
    sector: str = "Steel",
    const_scenario: str = "Prudent",
    as_gj: bool = False,
) -> pd.DataFrame:
    """Subsets the Biomass constraints model according to scenario parameters passed from the scenario dict.

    Args:
        bdf (pd.DataFrame): A DataFrame of the Bio Constraint model.
        sector (str, optional): The sector to get constraint values for. Defaults to "Steel".
        const_scenario (str, optional): The constraint scenario: 'Prudent, MaxPotential'. Defaults to "Prudent".
        as_gj (bool, optional): A boolean flag that converts a metric from exajoules to gigajoules. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame of the subsetted model.
    """
    bdf_c = bdf[(bdf["Sector"] == sector) & (bdf["Scenario"] == const_scenario)].copy()
    year_pairs = [(2020, 2030), (2030, 2040), (2040, 2050)]
    bdf_c = expand_melt_and_sort_years(bdf_c, year_pairs)
    bdf_c.columns = [col.lower().strip() for col in bdf_c.columns]
    if as_gj:
        bdf_c["value"] = bdf_c["value"] * EXAJOULE_TO_GIGAJOULE
    bdf_c = extend_df_years(bdf_c, "year", MODEL_YEAR_END)
    return bdf_c[["year", "unit", "value"]].set_index(["year"])


def subset_ccs_transport(
    cdf: pd.DataFrame,
    scenario_dict: dict,
    cost_scenario: str = "low",
    currency_conversion_factor: float = None,
) -> pd.DataFrame:
    """Subsets the CCS transport model according to scenario parameters passed from the scenario dict.

    Args:
        cdf (pd.DataFrame): A DataFrame of the CCS Transport model.
        scenario_dict (dict): The scenario_dict containing the full scenario setting for the current model run. Defaults to None.
        cost_scenario (str, optional): The parameter setting for the cost_scenario column. Defaults to 'low'.
        currency_conversion_factor (float, optional): The currency conversion factor that converts one currency to another. Defaults to None.
    Returns:
        pd.DataFrame: A DataFrame containing the subset of the model.
    """
    if scenario_dict:
        cost_scenario = CCS_SCENARIOS[scenario_dict["ccs_cost_scenario"]]
    cdf_c = cdf.copy()
    if cost_scenario == "low":
        cost_scenario_input = "Low"
        transport_type_input = "Shipping"
        capacity_input = 6
        t_cost_number = 2

    elif cost_scenario == "high":
        cost_scenario_input = "BaseCase"
        transport_type_input = "Shipping"
        capacity_input = 6
        t_cost_number = 2

    cdf_c = cdf_c[
        (cdf_c["Cost Estimate"] == cost_scenario_input)
        & (cdf_c["Transport Type"] == transport_type_input)
        & (cdf_c["Capacity"] == capacity_input)
    ]
    cdf_c.columns = [
        col.lower().strip().replace(" ", "_").replace("__", "_")
        for col in cdf_c.columns
    ]
    value_colname = f"transport_costs_node_{t_cost_number}"
    cdf_c.rename(
        {"unit_capacity": "unit", value_colname: "value"}, axis=1, inplace=True
    )
    if currency_conversion_factor:
        cdf_c = convert_currency_col(cdf_c, "value", currency_conversion_factor)
    return cdf_c[["region", "unit", "value"]].set_index(["region"])


def subset_ccs_storage(
    cdf: pd.DataFrame,
    scenario_dict: dict,
    cost_scenario: str = "low",
    currency_conversion_factor: float = None,
) -> pd.DataFrame:
    """Subsets the CCS storage model according to scenario parameters passed from the scenario dict.

    Args:
        cdf (pd.DataFrame): A DataFrame of the CCS Storage model.
        scenario_dict (dict): The scenario_dict containing the full scenario setting for the current model run. Defaults to None.
        cost_scenario (str, optional): The parameter setting for the cost_scenario column. Defaults to 'low'.
        currency_conversion_factor (float, optional): The currency conversion factor that converts one currency to another. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the subset of the model.
    """

    if scenario_dict:
        cost_scenario = CCS_SCENARIOS[scenario_dict["ccs_cost_scenario"]]
    cdf_c = cdf.copy()
    if cost_scenario == "low":
        storage_location_input = "Onshore"
        storage_type_input = "Depleted O&G field"
        reusable_lw_input = "Yes"
        value_input = "Medium"
    elif cost_scenario == "high":
        storage_location_input = "Offshore"
        storage_type_input = "Saline aquifers"
        reusable_lw_input = "No"
        value_input = "Medium"
    cdf_c = cdf_c[
        (cdf_c["Storage location"] == storage_location_input)
        & (cdf_c["Storage type"] == storage_type_input)
        & (cdf_c["Reusable legacy wells"] == reusable_lw_input)
        & (cdf_c["Value"] == value_input)
    ]
    cdf_c.columns = [
        col.lower().strip().replace(" ", "_").replace("__", "_")
        for col in cdf_c.columns
    ]
    cdf_c.drop("value", axis=1, inplace=True)
    cdf_c.rename({"costs_-_capacity_5": "value"}, axis=1, inplace=True)
    if currency_conversion_factor:
        cdf_c = convert_currency_col(cdf_c, "value", currency_conversion_factor)
    return cdf_c[["region", "unit", "value"]].set_index(["region"])


def subset_ccs_constraint(
    cdf: pd.DataFrame, scenario_dict: dict, as_gt: bool = False
) -> pd.DataFrame:
    """Subsets the CCS transport model according to scenario parameters passed from the scenario dict.

    Args:
        cdf (pd.DataFrame): A DataFrame of the CCS Constraint model.
        scenario_dict (dict): The scenario_dict containing the full scenario setting for the current model run. Defaults to None.
        as_gt (bool, optional): Converts the constraint price from per megaton to gigaton. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the subset of the model.
    """

    ccs_capacity_scenario = CCS_CAPACITY_SCENARIOS[
        scenario_dict["ccs_capacity_scenario"]
    ]
    df = cdf[cdf["Scenario"] == ccs_capacity_scenario].reset_index(drop=True).copy()
    df = df.melt(
        id_vars=["Scenario", "Year", "Variable", "Unit"],
        var_name="Region",
        value_name="value",
    )
    if as_gt:
        df["value"] = df["value"] / GIGATON_TO_MEGATON_FACTOR
        df["unit"] = "Gton"
    df.columns = [col.lower() for col in df.columns]
    df = extend_df_years(df, "year", MODEL_YEAR_END)
    return df.set_index(["year", "region"])


@timer_func
def format_pe_data(
    scenario_dict: dict, serialize: bool = False, standardize_units: bool = True
) -> dict:
    """Full process flow for the Power & Energy data.
    Inputs the the import data, subsets the data, then creates a model reference dictionary for the model.

    Args:
        scenario_dict (dict): The scenario_dict containing the full scenario setting for the current model run.
        serialize (bool, optional): Serializes the Power & Energy data. Defaults to False.
        standarside_units (bool, optional): Optional flag to determine whether to automatically standardise all units to their desired units for further use downstream. Defaults to True.

    Returns:
        dict: Dictionary of the Power & Energy data.
    """
    logger.info("Initiating full format flow for all models")

    # LOAD PKL FILES
    h2_prices = read_pickle_folder(PKL_DATA_IMPORTS, "hydrogen_model", "df")["Prices"]
    h2_emissions = read_pickle_folder(PKL_DATA_IMPORTS, "hydrogen_model", "df")[
        "Emissions"
    ]
    power_grid_prices = read_pickle_folder(PKL_DATA_IMPORTS, "power_model", "df")[
        "GridPrice"
    ]
    power_grid_emissions = read_pickle_folder(PKL_DATA_IMPORTS, "power_model", "df")[
        "GridEmissions"
    ]
    bio_model_prices = read_pickle_folder(PKL_DATA_IMPORTS, "bio_model", "df")[
        "Feedstock_Prices"
    ]
    bio_model_constraints = read_pickle_folder(PKL_DATA_IMPORTS, "bio_model", "df")[
        "Biomass_constraint"
    ]
    ccs_model = read_pickle_folder(PKL_DATA_IMPORTS, "ccs_model", "df")
    ccs_model_transport = ccs_model["Transport"]
    ccs_model_storage = ccs_model["Storage"]
    ccs_model_constraints = ccs_model["Constraint"]
    fossil_fuel_model = read_pickle_folder(PKL_DATA_IMPORTS, "fossil_fuel_model", "df")
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref", "df")

    # FORMAT INITIAL DATA
    h2_prices_f = subset_hydrogen(
        h2_prices, scenario_dict, prices=True, price_per_gj=standardize_units
    )  # from usd / kg to usd / gj
    h2_emissions_f = subset_hydrogen(
        h2_emissions, scenario_dict, prices=False, emissions_per_gj=standardize_units
    )  # from kg / kg to t / gj
    power_grid_prices_f = subset_power(
        power_grid_prices, scenario_dict, per_gj=standardize_units
    )  # from usd / mwh to usd / gj
    power_grid_emissions_f = subset_power(
        power_grid_emissions, scenario_dict, per_gj=standardize_units
    )  # from tco2 / mwh to tco2 / gj
    bio_model_prices_f = subset_bio_prices(
        bio_model_prices, scenario_dict
    )  # no conversion required
    bio_model_constraints_f = subset_bio_constraints(
        bio_model_constraints, as_gj=standardize_units
    )  # ej to gj
    ccs_model_transport_f = subset_ccs_transport(
        ccs_model_transport, scenario_dict
    )  # no conversion required
    ccs_model_storage_f = subset_ccs_storage(
        ccs_model_storage, scenario_dict
    )  # no conversion required
    ccs_model_constraints_f = subset_ccs_constraint(
        ccs_model_constraints, scenario_dict, as_gt=False
    )  # from Mt to Gt
    fossil_fuel_prices_f = subset_fossil_fuel_prices(
        fossil_fuel_model, scenario_dict, price_per_gj=standardize_units
    )
    # CREATE REFERENCE_DFs
    power_hydrogen_regions = power_hydrogen_region_reference_generator(country_ref)
    power_grid_prices_ref = final_mapper(
        power_grid_prices_f, power_hydrogen_regions, MODEL_YEAR_RANGE
    )
    power_grid_emissions_ref = final_mapper(
        power_grid_emissions_f, power_hydrogen_regions, MODEL_YEAR_RANGE
    )
    h2_prices_ref = final_mapper(
        h2_prices_f, power_hydrogen_regions, MODEL_YEAR_RANGE
    )
    h2_emissions_ref = final_mapper(
        h2_emissions_f, power_hydrogen_regions, MODEL_YEAR_RANGE
    )
    bio_regions = bio_region_reference_generator(country_ref)
    bio_model_prices_ref = final_mapper(
        bio_model_prices_f, bio_regions, MODEL_YEAR_RANGE
    )
    ccs_regions = ccs_region_reference_generator(country_ref)
    ccs_model_storage_ref = final_mapper(ccs_model_storage_f, ccs_regions)
    ccs_model_transport_ref = final_mapper(ccs_model_transport_f, ccs_regions)
    ccs_model_constraints_ref = model_reference_generator(
        ccs_model_constraints_f,
        country_ref,
        CCS_CAPACITY_REGION_MAPPER,
        MODEL_YEAR_RANGE,
    )
    fossil_fuel_regions = fossil_fuel_region_reference_generator(country_ref)
    fossil_fuel_ref = fossil_fuel_mapper(fossil_fuel_prices_f, fossil_fuel_regions, MODEL_YEAR_RANGE)
    data_dict = {
        "hydrogen_prices": h2_prices_f,
        "hydrogen_emissions": h2_emissions_f,
        "power_grid_prices": power_grid_prices_f,
        "power_grid_emissions": power_grid_emissions_f,
        "bio_price": bio_model_prices_f,
        "bio_constraint": bio_model_constraints_f,
        "ccs_transport": ccs_model_storage_f,
        "ccs_storage": ccs_model_transport_f,
        "ccs_constraints": ccs_model_constraints_f,
        "fossil_fuel_prices": fossil_fuel_prices_f
    }

    if serialize:
        intermediate_path = get_scenario_pkl_path(
            scenario_dict["scenario_name"], "intermediate"
        )
        # DataFrames
        serialize_file(h2_prices_f, intermediate_path, "hydrogen_prices_formatted")
        serialize_file(
            h2_emissions_f, intermediate_path, "hydrogen_emissions_formatted"
        )
        serialize_file(
            power_grid_prices_f, intermediate_path, "power_grid_prices_formatted"
        )
        serialize_file(
            power_grid_emissions_f, intermediate_path, "power_grid_emissions_formatted"
        )
        serialize_file(
            bio_model_prices_f, intermediate_path, "bio_price_model_formatted"
        )
        serialize_file(
            bio_model_constraints_f, intermediate_path, "bio_constraint_model_formatted"
        )
        serialize_file(
            ccs_model_storage_f, intermediate_path, "ccs_transport_model_formatted"
        )
        serialize_file(
            ccs_model_transport_f, intermediate_path, "ccs_storage_model_formatted"
        )
        serialize_file(
            ccs_model_constraints_f,
            intermediate_path,
            "ccs_constraints_model_formatted",
        )
        serialize_file(
            fossil_fuel_prices_f, intermediate_path, "fossil_fuel_model_formatted"
        )
        # Dictionaries
        serialize_file(h2_prices_ref, intermediate_path, "h2_prices_ref")
        serialize_file(h2_emissions_ref, intermediate_path, "h2_emissions_ref")
        serialize_file(
            power_grid_prices_ref, intermediate_path, "power_grid_prices_ref"
        )
        serialize_file(
            power_grid_emissions_ref, intermediate_path, "power_grid_emissions_ref"
        )
        serialize_file(bio_model_prices_ref, intermediate_path, "bio_model_prices_ref")
        serialize_file(
            ccs_model_storage_ref, intermediate_path, "ccs_model_storage_ref"
        )
        serialize_file(
            ccs_model_transport_ref, intermediate_path, "ccs_model_transport_ref"
        )
        serialize_file(
            ccs_model_constraints_ref, intermediate_path, "ccs_model_constraints_ref"
        )
        serialize_file(
            fossil_fuel_ref, intermediate_path, "fossil_fuel_ref"
        )
    return data_dict
