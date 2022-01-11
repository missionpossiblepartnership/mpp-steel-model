"""Script to determine the variable plant cost types dependent on regions."""

import pandas as pd
from tqdm import tqdm

from mppsteel.model_config import (
    MODEL_YEAR_END, PKL_DATA_IMPORTS, MODEL_YEAR_START, PKL_DATA_INTERMEDIATE, 
    COST_SCENARIO_MAPPER, GRID_DECARBONISATION_SCENARIOS,
)

from mppsteel.model.solver import load_business_cases


from mppsteel.utility.utils import (
    serialize_file, get_logger, read_pickle_folder, timer_func
)

from mppsteel.data_loading.data_interface import (
    commodity_data_getter, static_energy_prices_getter,
)

from mppsteel.model.emissions_reference_tables import dynamic_energy_price_getter


from mppsteel.data_loading.pe_model_formatter import (
    power_data_getter, hydrogen_data_getter, ccus_data_getter, RE_DICT
)

# Create logger
logger = get_logger("Variable Plant Cost Archetypes")

def generate_feedstock_dict():
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


def plant_variable_costs(year_end: int, electricity_cost_scenario: str, grid_decarb_scenario: str, hydrogen_cost_scenario: str):
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
    ccus_model_formatted = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'ccus_model_formatted', 'df')

    static_energy_prices = read_pickle_folder(PKL_DATA_IMPORTS, 'static_energy_prices', 'df')[['Metric', 'Year', 'Value']]
    feedstock_dict = generate_feedstock_dict()
    business_cases = load_business_cases()

    for country_code in tqdm(steel_plant_country_codes, total=len(steel_plant_country_codes), desc='Plant country_codes'):
        ng_flag = steel_plant_region_ng_dict[country_code]
        df = generate_variable_costs(
            business_cases_df=business_cases,
            plant_country_ref=country_code,
            ng_flag=ng_flag,
            year_end=year_end,
            feedstock_dict=feedstock_dict,
            static_energy_df=static_energy_prices,
            power_df=power_model_formatted,
            hydrogen_df=hydrogen_model_formatted,
            ccus_df=ccus_model_formatted,
            electricity_cost_scenario=electricity_cost_scenario,
            grid_decarb_scenario=grid_decarb_scenario,
            hydrogen_cost_scenario=hydrogen_cost_scenario,
        )
        df['plant_country_ref'] = country_code
        df_list.append(df)

    return pd.concat(df_list).reset_index(drop=True)

def generate_variable_costs(
    business_cases_df: pd.DataFrame,
    plant_country_ref: str,
    ng_flag: int,
    year_end: int = None,
    feedstock_dict: dict = None,
    static_energy_df: pd.DataFrame = None,
    power_df: pd.DataFrame = None,
    hydrogen_df: pd.DataFrame = None,
    ccus_df: pd.DataFrame = None,
    electricity_cost_scenario: str = '',
    grid_decarb_scenario: str = '',
    hydrogen_cost_scenario: str = '',
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
    static_energy_list = static_energy_df['Metric'].unique().tolist()
    feedstock_list = list(feedstock_dict.keys())

    # Create a year range
    year_range = range(MODEL_YEAR_START, tuple({year_end+1 or 2021})[0])

    for year in tqdm(year_range, desc='Variable years'):
        df_c = business_cases_df.copy()

        df_c['Static'] = ''
        df_c['Feedstock'] = ''
        df_c['Electricity'] = ''
        df_c['Hydrogen'] = ''
        df_c['Natural gas'] = ''

        static_year = year
        if year > 2026:
            static_year = 2026
        dynamic_year = year
        if year > 2050:
            dynamic_year = 2050
        
        electricity_price = power_data_getter(
            power_df, 'grid', dynamic_year, plant_country_ref, RE_DICT,
            default_country='USA', grid_scenario=GRID_DECARBONISATION_SCENARIOS[grid_decarb_scenario],
            cost_scenario=COST_SCENARIO_MAPPER[electricity_cost_scenario])
        
        hydrogen_price = hydrogen_data_getter(
            hydrogen_df, 'prices', dynamic_year, plant_country_ref,
            default_country='USA', variable='Total price premium ',
            cost_scenario=COST_SCENARIO_MAPPER[hydrogen_cost_scenario],
        )

        natural_gas_high = static_energy_prices_getter(static_energy_df, 'Natural gas - high', static_year)
        natural_gas_low = static_energy_prices_getter(static_energy_df, 'Natural gas - low', static_year)

        for row in df_c.itertuples():
            resource = row.material_category
            resource_consumed = row.value

            if resource in static_energy_list:
                price_unit_value = static_energy_prices_getter(static_energy_df, resource, static_year)
                df_c.loc[row.Index, 'Static'] = resource_consumed * price_unit_value

            if resource in feedstock_list:
                price_unit_value = feedstock_dict[resource]
                df_c.loc[row.Index, 'Feedstock'] = resource_consumed * price_unit_value

            if resource == 'Natural gas':
                if ng_flag == 1:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * natural_gas_low
                elif ng_flag == 0:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * natural_gas_high

            if resource == 'Electricity':
                df_c.loc[row.Index, 'Electricity'] = resource_consumed * electricity_price

            if resource == 'Hydrogen':
                df_c.loc[row.Index, 'Hydrogen'] = resource_consumed * hydrogen_price

        df_c['year'] = year
        df_list.append(df_c)

    combined_df = pd.concat(df_list)
    return combined_df

def format_variable_costs(variable_cost_df: pd.DataFrame):
    """[summary]

    Args:
        variable_cost_df (pd.DataFrame): [description]

    Returns:
        [type]: [description]
    """    

    df_c = variable_cost_df.copy()
    df_c.drop(labels=['value'], axis=1, inplace=True)
    df_c = df_c.melt(id_vars=['plant_country_ref', 'technology', 'year', 'material_category', 'unit'],var_name=['cost_type'], value_name='cost')
    df_c['cost'] = df_c['cost'].replace('', 0)
    return df_c.groupby(by=['plant_country_ref', 'year', 'technology']).sum().sort_values(by=['plant_country_ref', 'year', 'technology'])


@timer_func
def generate_variable_plant_summary(scenario_dict: dict, serialize_only: bool = False):
    """[summary]

    Args:
        serialize_only (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """
    electricity_cost_scenario = scenario_dict['electricity_cost_scenario']
    grid_decarb_scenario = scenario_dict['grid_decarb_scenario']
    hydrogen_cost_scenario = scenario_dict['hydrogen_cost_scenario']

    all_plant_variable_costs = plant_variable_costs(MODEL_YEAR_END, electricity_cost_scenario, grid_decarb_scenario, hydrogen_cost_scenario)
    all_plant_variable_costs_summary = format_variable_costs(all_plant_variable_costs)

    if serialize_only:
        logger.info(f'-- Serializing dataframes')
        serialize_file(all_plant_variable_costs_summary, PKL_DATA_INTERMEDIATE, "all_plant_variable_costs_summary")
    return all_plant_variable_costs_summary
