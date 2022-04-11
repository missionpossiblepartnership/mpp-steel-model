"""Formats Price & Emissions Model Data and defines getter functions"""

import pandas as pd
import pandera as pa

from mppsteel.config.model_config import (
    PKL_DATA_IMPORTS,
    MODEL_YEAR_END,
)
from mppsteel.config.model_scenarios import (
    COST_SCENARIO_MAPPER,
    GRID_DECARBONISATION_SCENARIOS,
    BIOMASS_SCENARIOS,
    CCUS_SCENARIOS,
)
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import (
    expand_melt_and_sort_years,
    convert_currency_col
)
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)

from mppsteel.utility.log_utility import get_logger
from mppsteel.validation.shared_inputs_tests import (
    BIO_CONSTRAINT_MODEL_SCHEMA,
)

# Create logger
logger = get_logger(__name__)

RE_DICT = {
    "solar": "Price of onsite solar",
    "wind": "Price of onsite wind",
    "wind_and_solar": "Price of onsite wind + solar",
    "gas": "Price of onsite gas + ccs",
}

POWER_HYDROGEN_COUNTRY_MAPPER = {
    "China": "China",
    "Europe": "EU",
    "NAFTA": "US",
    "India": "India",
    "Japan, South Korea, Taiwan": "China",
    "Japan, South Korea, and Taiwan": "China",
    "South and central Americas": "India",
    "South and Central America": "India",
    "Middle East": "India",
    "Africa": "India",
    "CIS": "EU",
    "Southeast Asia": "China",
    "Other Asia": "China",
    "RoW": "India",
}

BIO_COUNTRY_MAPPER = {
    "China": "China",
    "Europe": "Europe",
    "NAFTA": "US",
    "India": "India",
    "Japan, South Korea, Taiwan": "China",
    "Japan, South Korea, and Taiwan": "China",
    "South and central Americas": "US",
    "South and Central America": "US",
    "Middle East": "India",
    "Africa": "US",
    "CIS": "Europe",
    "Southeast Asia": "China",
    "RoW": "RoW",
}

CCUS_COUNTRY_MAPPER = {
    'RoW': 'Global',
    'NAFTA': 'US',
    'NAFTA': 'Mexico',
    'NAFTA': 'Canada',
    'Europe': 'Europe',
    'China': 'China',
    'India': 'India',
    'Africa': 'Africa',
    'Middle East': 'Middle East',
    'CIS': 'Russia',
    'Southeast Asia': 'Indonesia',
    'South and Central America': 'Brazil',
    'South and Central America': 'Other Latin America',
    'CIS': 'Other Eurasia ',
    'Southeast Asia': 'Dynamic Asia',
    'Southeast Asia': 'Other East Asia',
    'Japan, South Korea, and Taiwan': 'Japan',
    'Japan, South Korea, and Taiwan': 'Korea'
}


def subset_power(
    pdf,
    scenario_dict: dict = None,
    customer: str = 'Industry',
    grid_scenario: str = 'Central',
    cost_scenario: str = 'Baseline',
    currency_conversion_factor: float = None,
    as_gj: bool = False
):
    if scenario_dict:
        cost_scenario = COST_SCENARIO_MAPPER[scenario_dict['electricity_cost_scenario']]
        grid_scenario = GRID_DECARBONISATION_SCENARIOS[scenario_dict['grid_scenario']]
    pdf_c = pdf.copy()
    pdf_c = pdf_c[(pdf_c['Customer'] == customer) & (pdf_c['Grid scenario'] == grid_scenario) & (pdf_c['Cost scenario '] == cost_scenario)]
    years = [year_col for year_col in pdf_c.columns if isinstance(year_col, int)]
    pdf_c = pdf_c.melt(id_vars=['Region', 'Unit'], value_vars=years, var_name='year', value_name='value')
    pdf_c.columns = [col.lower().strip() for col in pdf_c.columns]
    if currency_conversion_factor:
        pdf_c = convert_currency_col(pdf_c, 'value', currency_conversion_factor)
    if as_gj:
        pdf_c['value'] = pdf_c['value'] / 3.6
    return pdf_c[['year', 'region', 'unit', 'value']].set_index(['year', 'region'])


def subset_hydrogen(
    h2df,
    scenario_dict: dict = None,
    prices: bool = False,
    variable: str = 'H2 price',
    cost_scenario: str = 'Baseline',
    prod_scenario: str = 'On-site, dedicated VREs',
    currency_conversion_factor: float = None
):
    if scenario_dict:
        cost_scenario = COST_SCENARIO_MAPPER[scenario_dict['hydrogen_cost_scenario']]
    h2df_c = h2df.copy()
    h2df_c = h2df_c[(h2df_c['Cost scenario'] == cost_scenario) & (h2df_c['Production scenario'] == prod_scenario)]
    if prices:
        h2df_c = h2df_c[ (h2df_c['Variable'] == variable)]
    years = [year_col for year_col in h2df_c.columns if isinstance(year_col, int)]
    h2df_c = h2df_c.melt(id_vars=['Region', 'Unit '], value_vars=years, var_name='year', value_name='value')
    h2df_c.columns = [col.lower().strip() for col in h2df_c.columns]
    if currency_conversion_factor:
        h2df_c = convert_currency_col(h2df_c, 'value', currency_conversion_factor)
    return h2df_c[['year', 'region', 'unit', 'value']].set_index(['year', 'region'])

def subset_bio_prices(
    bdf: pd.DataFrame,
    scenario_dict: dict = None,
    cost_scenario: str = 'Medium',
    feedstock_type: str = 'Weighted average',
    currency_conversion_factor: float = None,
) -> pd.DataFrame:
    if scenario_dict:
        cost_scenario = BIOMASS_SCENARIOS[scenario_dict['biomass_cost_scenario']]
    bdf_c = bdf[(bdf["Price scenario"] == cost_scenario) & (bdf["Feedstock type"] == feedstock_type)].copy()
    year_pairs = [(2020, 2030), (2030, 2040), (2040, 2050)]
    bdf_c = expand_melt_and_sort_years(bdf_c, year_pairs)
    bdf_c.columns = [col.lower().strip() for col in bdf_c.columns]
    if currency_conversion_factor:
        bdf_c = convert_currency_col(bdf_c, 'value', currency_conversion_factor)
    return bdf_c[['year', 'region', 'unit', 'value']].set_index(['year', 'region'])


def subset_bio_constraints(
    bdf: pd.DataFrame,
    sector: str = "Steel",
    const_scenario: str = "Prudent",
) -> float:
    """A getter function for the formatted Bio Constraints model.

    Args:
        df (pd.DataFrame): A DataFrame of the Bio Constraint model.
        year (int): The year you want to retrieve a value.
        sector (str, optional): The sector you would like to get constraint values for. Defaults to "Steel".
        const_scenario (str, optional): The constraint scenario: 'Prudent, MaxPotential'. Defaults to "Prudent".

    Returns:
        float: A value based on the parameter settings inputted.
    """
    bdf_c = bdf[(bdf["Sector"] == sector) & (bdf["Scenario"] == const_scenario)].copy()
    year_pairs = [(2020, 2030), (2030, 2040), (2040, 2050)]
    bdf_c = expand_melt_and_sort_years(bdf_c, year_pairs)
    bdf_c.columns = [col.lower().strip() for col in bdf_c.columns]
    return bdf_c[['year', 'unit', 'value']].set_index(['year'])



def subset_ccus_transport(
    cdf: pd.DataFrame,
    scenario_dict: dict,
    cost_scenario: str = "low",
    currency_conversion_factor: float = None,
):
    if scenario_dict:
        cost_scenario = CCUS_SCENARIOS[scenario_dict['ccus_cost_scenario']]
    cdf_c = cdf.copy()
    if cost_scenario == "low":
        cost_scenario_input = "BaseCase"
        transport_type_input = "Onshore Pipeline"
        capacity_input = 5
        t_cost_number = 1

    elif cost_scenario == "high":
        cost_scenario_input = "BaseCase"
        transport_type_input = "Shipping"
        capacity_input = 5
        t_cost_number = 2

    cdf_c = cdf_c[
        (cdf_c["Cost Estimate"] == cost_scenario_input)
        & (cdf_c["Transport Type"] == transport_type_input)
        & (cdf_c["Capacity"] == capacity_input)
    ]
    cdf_c.columns = [col.lower().strip().replace(' ', '_').replace('__', '_') for col in cdf_c.columns]
    value_colname = f'transport_costs_node_{t_cost_number}'
    cdf_c.rename({'unit_capacity': 'unit', value_colname: 'value'}, axis=1, inplace=True)
    if currency_conversion_factor:
        cdf_c = convert_currency_col(cdf_c, 'value', currency_conversion_factor)

    return cdf_c[['region', 'unit', 'value']].set_index(['region'])


def subset_ccus_storage(
    cdf: pd.DataFrame,
    scenario_dict: dict,
    cost_scenario: str = "low",
    currency_conversion_factor: float = None,
):
    if scenario_dict:
        cost_scenario = CCUS_SCENARIOS[scenario_dict['ccus_cost_scenario']]
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
    cdf_c.columns = [col.lower().strip().replace(' ', '_').replace('__', '_') for col in cdf_c.columns]
    cdf_c.drop('value', axis=1, inplace=True)
    cdf_c.rename({'costs_-_capacity_5': 'value'}, axis=1, inplace=True)
    if currency_conversion_factor:
        cdf_c = convert_currency_col(cdf_c, 'value', currency_conversion_factor)
    return cdf_c[['region', 'unit', 'value']].set_index(['region'])


@timer_func
def format_pe_data(scenario_dict: dict, serialize: bool = False) -> dict:
    """Full process flow for the Power & Energy data.

    Args:
        serialize (bool, optional): Serializes the Power & Energy data. Defaults to False.

    Returns:
        dict: Dictionary of the Power & Energy data.
    """
    logger.info("Initiating full format flow for all models")
    h2_prices = read_pickle_folder(PKL_DATA_IMPORTS, 'hydrogen_model', 'df')['Prices']
    h2_emissions = read_pickle_folder(PKL_DATA_IMPORTS, 'hydrogen_model', 'df')['Emissions']
    power_grid_prices = read_pickle_folder(PKL_DATA_IMPORTS, 'power_model', 'df')['GridPrice']
    power_grid_emissions = read_pickle_folder(PKL_DATA_IMPORTS, 'power_model', 'df')['GridEmissions']
    bio_model_prices = read_pickle_folder(PKL_DATA_IMPORTS, 'bio_model', 'df')['Feedstock_Prices']
    bio_model_constraints = read_pickle_folder(PKL_DATA_IMPORTS, 'bio_model', 'df')['Biomass_constraint']
    ccus_model_transport = read_pickle_folder(PKL_DATA_IMPORTS, 'ccus_model', 'df')['Transport']
    ccus_model_storage = read_pickle_folder(PKL_DATA_IMPORTS, 'ccus_model', 'df')['Storage']

    h2_prices_f = subset_hydrogen(h2_prices, scenario_dict, prices=True)
    h2_emissions_f = subset_hydrogen(h2_emissions, scenario_dict, prices=False)
    power_grid_prices_f = subset_power(power_grid_prices, scenario_dict, as_gj=True)
    power_grid_emissions_f = subset_power(power_grid_emissions, scenario_dict)
    bio_model_prices_f = subset_bio_prices(bio_model_prices, scenario_dict)
    bio_model_constraints_f = subset_bio_constraints(bio_model_constraints)
    ccus_model_storage_f = subset_ccus_storage(ccus_model_storage, scenario_dict)
    ccus_model_transport_f = subset_ccus_transport(ccus_model_transport, scenario_dict)

    data_dict = {
        "hydrogen_prices": h2_prices_f,
        "hydrogen_emissions": h2_emissions_f,
        "power_grid_prices": power_grid_prices_f,
        "power_grid_emissions": power_grid_emissions_f,
        "bio_price": bio_model_prices_f,
        "bio_constraint": bio_model_constraints_f,
        "ccus_transport": ccus_model_storage_f,
        "ccus_storage": ccus_model_transport_f,
    }

    if serialize:
        intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
        serialize_file(h2_prices_f, intermediate_path, "hydrogen_prices_formatted")
        serialize_file(h2_emissions_f, intermediate_path, "hydrogen_emissions_formatted")
        serialize_file(power_grid_prices_f, intermediate_path, "power_grid_prices_formatted")
        serialize_file(power_grid_emissions_f, intermediate_path, "power_grid_emissions_formatted")
        serialize_file(bio_model_prices_f, intermediate_path, "bio_price_model_formatted")
        serialize_file(bio_model_constraints_f, intermediate_path, "bio_constraint_model_formatted")
        serialize_file(ccus_model_storage_f, intermediate_path, "ccus_transport_model_formatted")
        serialize_file(ccus_model_transport_f, intermediate_path, "ccus_storage_model_formatted")

    return data_dict


def pe_model_data_getter(
    df: pd.DataFrame,
    country_mapper: dict = None,
    model_region_mapper: dict = None,
    year: int = None,
    region_code: str = None
    ):
    
    if year:
        # Cap year at 2050
        year = min(MODEL_YEAR_END, year)

    if region_code and year:
        region = model_region_mapper[country_mapper[region_code]]
        return df.loc[year, region]['value']

    if year and not region_code:
        return df.loc[year]['value']
    
    if region_code and not year:
        region = model_region_mapper[country_mapper[region_code]]
        return df.loc[region]['value']
