"""Script to determine the variable plant cost types dependent on regions."""

import pandas as pd
from tqdm import tqdm

from mppsteel.model_config import (
    MODEL_YEAR_END, PKL_DATA_IMPORTS, MODEL_YEAR_START, PKL_DATA_INTERMEDIATE
)

from mppsteel.model_scenarios import (
    COST_SCENARIO_MAPPER, GRID_DECARBONISATION_SCENARIOS, BIOMASS_SCENARIOS, CCUS_SCENARIOS
)

from mppsteel.model.solver import load_business_cases


from mppsteel.utility.utils import (
    enumerate_iterable, cast_to_float
)
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file
)
from mppsteel.utility.log_utility import get_logger

from mppsteel.data_loading.data_interface import (
    commodity_data_getter, static_energy_prices_getter,
)

from mppsteel.data_loading.pe_model_formatter import (
    power_data_getter, hydrogen_data_getter, bio_price_getter, ccus_data_getter, RE_DICT
)

# Create logger
logger = get_logger("Variable Plant Cost Archetypes")

def generate_feedstock_dict() -> dict:
    """[summary]

    Returns:
        [type]: [description]
    """
    commodities_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'commodities_df', 'df')
    feedstock_prices = read_pickle_folder(PKL_DATA_IMPORTS, 'feedstock_prices', 'df')
    commodities_dict = commodity_data_getter(commodities_df)
    commodity_dictname_mapper = {'plastic': 'Plastic waste', 'ethanol': 'Ethanol', 'charcoal': 'Charcoal'}
    for key in commodity_dictname_mapper.keys():
        commodities_dict[commodity_dictname_mapper[key]] = commodities_dict.pop(key)
    return {**commodities_dict, **dict(zip(feedstock_prices['Metric'], feedstock_prices['Value']))}


def plant_variable_costs(
    year_end: int, electricity_cost_scenario: str, grid_decarb_scenario: str,
    hydrogen_cost_scenario: str, biomass_cost_scenario: str, ccus_cost_scenario: str) -> pd.DataFrame:
    """[summary]

    Args:
        year_end (int): [description]

    Returns:
        [type]: [description]
    """
    df_list = []

    steel_plants = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'steel_plants_processed', 'df')
    steel_plant_country_codes = list(steel_plants['country_code'].unique())
    steel_plant_region_ng_dict = steel_plants[['country_code', 'cheap_natural_gas']].set_index('country_code').to_dict()['cheap_natural_gas']
    power_model_formatted = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'power_model_formatted', 'df')
    hydrogen_model_formatted = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'hydrogen_model_formatted', 'df')
    bio_price_model_formatted = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'bio_price_model_formatted', 'df')
    ccus_model_formatted = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'ccus_model_formatted', 'df')
    static_energy_prices = read_pickle_folder(PKL_DATA_IMPORTS, 'static_energy_prices', 'df')[['Metric', 'Year', 'Value']]
    feedstock_dict = generate_feedstock_dict()
    business_cases = load_business_cases()

    for country_code in tqdm(steel_plant_country_codes, total=len(steel_plant_country_codes), desc='Plant country_codes'):
        ng_flag = steel_plant_region_ng_dict[country_code]
        df = generate_variable_costs(
            business_cases_df=business_cases,
            country_code=country_code,
            ng_flag=ng_flag,
            year_end=year_end,
            feedstock_dict=feedstock_dict,
            static_energy_df=static_energy_prices,
            power_df=power_model_formatted,
            hydrogen_df=hydrogen_model_formatted,
            bio_df=bio_price_model_formatted,
            ccus_df=ccus_model_formatted,
            electricity_cost_scenario=electricity_cost_scenario,
            grid_decarb_scenario=grid_decarb_scenario,
            hydrogen_cost_scenario=hydrogen_cost_scenario,
            biomass_cost_scenario=biomass_cost_scenario,
            ccus_cost_scenario=ccus_cost_scenario
        )
        df['country_code'] = country_code
        df_list.append(df)

    return pd.concat(df_list).reset_index(drop=True)

def generate_variable_costs(
    business_cases_df: pd.DataFrame,
    country_code: str,
    ng_flag: int,
    year_end: int = None,
    feedstock_dict: dict = None,
    static_energy_df: pd.DataFrame = None,
    power_df: pd.DataFrame = None,
    hydrogen_df: pd.DataFrame = None,
    bio_df: pd.DataFrame = None,
    ccus_df: pd.DataFrame = None,
    electricity_cost_scenario: str = '',
    grid_decarb_scenario: str = '',
    hydrogen_cost_scenario: str = '',
    biomass_cost_scenario: str = '',
    ccus_cost_scenario: str = ''
) -> pd.DataFrame:
    """[summary]

    Args:
        business_cases_df (pd.DataFrame): [description]
        plant_iteration (list, optional): [description]. Defaults to None.
        year_end (int, optional): [description]. Defaults to None.
        feedstock_dict (dict, optional): [description]. Defaults to None.
        static_energy_df (pd.DataFrame, optional): [description]. Defaults to None.
        electricity_df (pd.DataFrame, optional): [description]. Defaults to None.
        hydrogen_df (pd.DataFrame, optional): [description]. Defaults to None.

    Returns:
        pd.DataFrame: [description]
    """

    df_list = []
    # Create resources reference list
    country_ref_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "country_reference_dict", "df")
    FEEDSTOCK_LIST = ['Iron Ore', 'Scrap', 'DRI', 'Coal']
    FOSSIL_FUELS = ['Met coal', 'Coke', 'Thermal coal', 'BF gas', 'BOF gas', 'Plastic Waste']
    BIO_FUELS = ['Biomass', 'Biomethane']
    CCS_LIST = ['Captured CO2']
    OTHER_OPEX = ['Steam', 'BF slag']


    def value_mapper(row, enum_dict: dict):
        resource = row[enum_dict['material_category']]

        if resource in FOSSIL_FUELS:
            row[enum_dict['Fossil Fuels']] = row[enum_dict['value']] * static_energy_prices_getter(static_energy_df, resource, static_year)

        if resource in FEEDSTOCK_LIST:
            row[enum_dict['Feedstock']] = row[enum_dict['value']] * feedstock_dict[resource]

        if resource in BIO_FUELS:
            row[enum_dict['Bio Fuels']] = row[enum_dict['value']] * bio_price

        if resource == 'Natural gas':
            if ng_flag == 1:
                row[enum_dict['Fossil Fuels']] = row[enum_dict['value']] * natural_gas_low
            elif ng_flag == 0:
                row[enum_dict['Fossil Fuels']] = row[enum_dict['value']] * natural_gas_high

        if resource == 'Electricity':
            row[enum_dict['Electricity']] = row[enum_dict['value']] * electricity_price

        if resource == 'Hydrogen':
            row[enum_dict['Hydrogen']] = row[enum_dict['value']] * hydrogen_price

        if resource in OTHER_OPEX:
            if resource == 'BF slag':
                price = feedstock_dict[resource]
            if resource == 'Steam':
                price = row[enum_dict['value']] * static_energy_prices_getter(static_energy_df, resource, static_year)
            row[enum_dict['Other Opex']] = row[enum_dict['value']] * price

        if resource in CCS_LIST:
            row[enum_dict['CCS']] = row[enum_dict['value']] * (ccus_storage_price + ccus_transport_price)

        return row

    # Create a year range
    year_range = range(MODEL_YEAR_START, tuple({year_end+1 or 2021})[0])
    for year in year_range:
        df_c = business_cases_df.copy()
        df_c['Feedstock'] = ''
        df_c['Fossil Fuels'] = ''
        df_c['Bio Fuels'] = ''
        df_c['Hydrogen'] = ''
        df_c['Electricity'] = ''
        df_c['Other Opex'] = ''
        df_c['CCS'] = ''

        static_year = min(2026, year)
        dynamic_year = min(MODEL_YEAR_END, year)
        electricity_price = power_data_getter(
            power_df, 'grid', dynamic_year, country_code, country_ref_dict, RE_DICT,
            default_country='USA', grid_scenario=GRID_DECARBONISATION_SCENARIOS[grid_decarb_scenario],
            cost_scenario=COST_SCENARIO_MAPPER[electricity_cost_scenario])
        hydrogen_price = hydrogen_data_getter(
            hydrogen_df, 'prices', dynamic_year, country_code, country_ref_dict,
            default_country='USA', variable='Total price premium ',
            cost_scenario=COST_SCENARIO_MAPPER[hydrogen_cost_scenario],
        )
        bio_price = bio_price_getter(
            bio_df, dynamic_year, country_code, country_ref_dict,
            default_country='USA', feedstock_type='Weighted average',
            cost_scenario=BIOMASS_SCENARIOS[biomass_cost_scenario],
        )
        ccus_transport_price = ccus_data_getter(
            ccus_df, 'transport', country_code, default_country='GBL',
            cost_scenario=CCUS_SCENARIOS[ccus_cost_scenario]
        )
        ccus_storage_price = ccus_data_getter(
            ccus_df, 'storage', country_code, default_country='GBL',
            cost_scenario=CCUS_SCENARIOS[ccus_cost_scenario]
        )
        natural_gas_high = static_energy_prices_getter(static_energy_df, 'Natural gas - high', static_year)
        natural_gas_low = static_energy_prices_getter(static_energy_df, 'Natural gas - low', static_year)
        enumerated_cols = enumerate_iterable(df_c.columns)
        df_c = df_c.apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
        df_c['year'] = year
        df_list.append(df_c)
    return pd.concat(df_list)

def format_variable_costs(variable_cost_df: pd.DataFrame, group_data: bool = True) -> pd.DataFrame:
    """[summary]

    Args:
        variable_cost_df (pd.DataFrame): [description]

    Returns:
        [type]: [description]
    """

    df_c = variable_cost_df.copy()
    df_c = df_c.melt(id_vars=['country_code', 'technology', 'year', 'material_category', 'unit', 'value'],var_name=['cost_type'], value_name='cost')
    df_c['cost'] = df_c['cost'].replace('', 0)
    df_c = df_c[(df_c['material_category'] != '0…') & (df_c['cost'] != 0)].reset_index(drop=True)
    if group_data:
        df_c.drop(['material_category', 'unit', 'cost_type', 'value'], axis=1, inplace=True)
        df_c = df_c.groupby(by=['country_code', 'year', 'technology']).sum().sort_values(by=['country_code', 'year', 'technology'])
        df_c['cost'] = df_c['cost'].apply(lambda x: cast_to_float(x))
        return df_c
    return df_c

@timer_func
def generate_variable_plant_summary(scenario_dict: dict, serialize: bool = False) -> pd.DataFrame:
    """[summary]

    Args:
        serialize (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """
    electricity_cost_scenario = scenario_dict['electricity_cost_scenario']
    grid_scenario = scenario_dict['grid_scenario']
    hydrogen_cost_scenario = scenario_dict['hydrogen_cost_scenario']
    biomass_cost_scenario = scenario_dict['biomass_cost_scenario']
    ccus_cost_scenario = scenario_dict['ccus_cost_scenario']
    variable_costs = plant_variable_costs(
        MODEL_YEAR_END, electricity_cost_scenario, grid_scenario,
        hydrogen_cost_scenario, biomass_cost_scenario, ccus_cost_scenario)
    variable_costs_summary = format_variable_costs(variable_costs)
    variable_costs_summary_material_breakdown = format_variable_costs(variable_costs, group_data=False)

    if serialize:
        logger.info(f'-- Serializing dataframes')
        serialize_file(variable_costs_summary, PKL_DATA_INTERMEDIATE, "variable_costs_regional")
        serialize_file(variable_costs_summary_material_breakdown, PKL_DATA_INTERMEDIATE, "variable_costs_regional_material_breakdown")
    return variable_costs_summary
