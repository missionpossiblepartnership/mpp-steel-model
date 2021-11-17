"""Functions to access data sources"""

from collections import namedtuple

# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from utils import get_logger, read_pickle_folder

# Get model parameters
from model_config import PKL_FOLDER, EMISSIONS_FACTOR_SLAG, ENERGY_DENSITY_MET_COAL

# Create logger
logger = get_logger('Data Interface')

def format_scope3_ef_2(df: pd.DataFrame, emissions_factor_slag: float) -> pd.DataFrame:
    df_c = df.copy()
    df_c = df_c.drop(['Unnamed: 1'], axis=1).loc[0:0]
    df_c = df_c.melt(id_vars=['Year']) 
    df_c.rename(columns={'Year': 'metric', 'variable': 'year'}, inplace=True)
    df_c['value'] = df_c['value'].astype(float)
    df_c['value'] = df_c['value'].apply(lambda x: x*emissions_factor_slag)
    return df_c

def modify_scope3_ef_1(df: pd.DataFrame, slag_values: np.array, met_coal_density: float) -> pd.DataFrame:
    df_c = df.copy()
    scope3df_index = df_c.set_index(['Category', 'Fuel', 'Unit'])
    scope3df_index.loc['Scope 3 Emissions Factor', 'Slag', 'ton CO2eq / ton slag'] = slag_values
    met_coal_values = scope3df_index.loc['Scope 3 Emissions Factor', 'Met coal', 'MtCO2eq / PJ'].apply(lambda x: x*met_coal_density)
    scope3df_index.loc['Scope 3 Emissions Factor', 'Met coal', 'MtCO2eq / PJ'] = met_coal_values
    scope3df_index.reset_index(inplace=True)
    scope3df_index.head()
    return scope3df_index.melt(id_vars=['Category', 'Fuel', 'Unit'], var_name='Year')


def melt_and_index(df: pd.DataFrame, id_vars: list, var_name: str, index: list) -> pd.DataFrame:
    """Make the dataframes tabular and create a multiindex

    Args:
        df (pd.DataFrame): The data import of the capex tables

    Returns:
        pd.DataFrame: A datframe of the tabular dataframe
    """
    df_c = df.copy()
    df_c = pd.melt(frame=df_c, id_vars=id_vars, var_name=var_name)
    df_c.set_index(index, inplace=True)
    return df_c

def capex_generator(
    capex_dict: dict, technology: str, year: int, output_type: str = 'all'):
    """Creates an interface to the tabular capex data.

    Args:
        capex_dict (dict): A capex dictionary with each capex type as a DataFrame
        technology (str): The technology that you want to access
        year (int): The year that you want to access
        output_type (str, optional): Flag whether to access all the the capex values or whichever you specify. Defaults to 'all'.

    Returns:
        A (dict) if output_type is set to 'all'.
        Otherwise returns the specific output_type specified (as float).
    """

    greenfield = capex_dict['greenfield'].loc[technology, year].value
    brownfield = capex_dict['brownfield'].loc[technology, year].value
    other_opex = capex_dict['other_opex'].loc[technology, year].value

    capex_dict = {
        'greenfield': greenfield,
        'brownfield': brownfield,
        'other_opex': other_opex
    }

    if output_type == 'all':
        logger.info(f'Creating capex values dictionary for {technology}')
        return capex_dict
    logger.info(f'Creating capex value: {output_type} for: {technology}')
    return capex_dict[output_type]

def capex_dictionary_generator(
    greenfield_df: pd.DataFrame, brownfield_df: pd.DataFrame, other_df: pd.DataFrame) -> dict:
    """[summary]

    Args:
        greenfield_df (pd.DataFrame): A dataframe of greenfield capex
        brownfield_df (pd.DataFrame): A dataframe of brownfield capex
        other_df (pd.DataFrame): A dataframe of other opex

    Returns:
        dict: A dictionary of the formatted capex and opex dataframes
    """

    brownfield_df.drop(
        ['Available from', 'Available until', 'Technology type'], axis=1, inplace=True)
    return {
        'greenfield': melt_and_index(
            greenfield_df, ['Technology'], 'Year', ['Technology', 'Year']),
        'brownfield': melt_and_index(
            brownfield_df, ['Technology'], 'Year', ['Technology', 'Year']),
        'other_opex': melt_and_index(
            other_df, ['Technology'], 'Year', ['Technology', 'Year'])
    }


def create_data_tuples(df: pd.DataFrame, namedtuple_name: str):
    """Generic function that dynamically create namedtuples from a dataframe.
    *** WARNING *** You won't see these namedtuples defined prior to creation so you have to be careful.

    Args:
        df (pd.DataFrame): DataFrame containing the data to translate into the namedtuples
        namedtuple_name (str): The name of the namedtuple that you want to create
    """
    df_c = df.copy()
    df_c.columns = [colname.lower() for colname in df_c.columns]
    class_names = [metric.upper().replace(' ', '_') for metric in df_c['metric'].to_list()]
    globals()[namedtuple_name] = namedtuple('namedtuple_name', ['metric', 'unit', 'year', 'value'])
    ticker = 0
    logger.info(f'Creating new namtedtuples with the following names: {class_names}')
    for row in df_c.itertuples():
        globals()[class_names[ticker]] = globals()[namedtuple_name](row.metric, row.unit, row.year, row.value)
        ticker+=1

def steel_demand_getter(df: pd.DataFrame, steel_type: str, scenario: str, year: str) -> float:
    df_c = df.copy()
    metric_names = df_c['Steel Type'].unique()
    scenarios = df_c['Scenario'].unique()
    logger.info(
        f'''Creating scope 1 emissions getter with the following metrics: {metric_names} and scenarios: {scenarios}''')
    df_c.set_index(['Steel Type', 'Scenario', 'Year'], inplace=True)
    logger.info(f'Getting Steel Demand value for: {steel_type} - {scenario} - {year}')
    value = df_c.loc[steel_type, scenario, year]['Value']
    return value

def carbon_tax_getter(df: pd.DataFrame, year: str) -> float:
    df_c = df.copy()
    df_c.set_index(['Year'], inplace=True)
    logger.info(f'Getting Carbon Tax value for: {year}')
    value = df_c.loc[year]['Value']
    return value

def scope1_emissions_getter(df: pd.DataFrame, metric: str) -> float:
    df_c = df.copy()
    metric_names = df_c['Metric'].to_list()
    logger.info(f'Creating scope 1 emissions getter with the following metrics: {metric_names}')
    df_c.set_index(['Metric'], inplace=True)
    logger.info(f'Getting Scope 1 emissions value for: {metric}')
    value = df_c.loc[metric]['Value']
    return value

def ccs_co2_getter(df: pd.DataFrame, metric: str, year: str) -> float:
    df_c = df.copy()
    metric_names = df_c['Metric'].unique()
    logger.info(f'Creating CCS CO2 getter with the following metrics: {metric_names}')
    df_c.set_index(['Metric', 'Year'], inplace=True)
    logger.info(f'Getting {metric} value for: {year}')
    value = df_c.loc[metric, year]['Value']
    return value

def static_energy_prices_getter(df: pd.DataFrame, metric: str, year: str) -> float:
    df_c = df.copy()
    metric_names = df_c['Metric'].unique()
    logger.info(f'Creating Static Energy getter with the following metrics: {metric_names}')
    df_c.set_index(['Metric', 'Year'], inplace=True)
    logger.info(f'Getting {metric} value for: {year}')
    value = df_c.loc[metric, year]['Value']
    return value

def technology_availability_getter(df: pd.DataFrame, technology: str) -> tuple:
    df_c = df.copy()
    metric_names = df_c['Technology'].unique()
    logger.info(f'Creating Technology getter with the following metrics: {metric_names}')
    df_c.set_index(['Technology'], inplace=True)
    logger.info(f'Getting {technology} availability')
    year_available_from = df_c.loc[technology]['Year available from']
    year_available_until = df_c.loc[technology]['Year available until']
    return year_available_from, year_available_until

def grid_emissivity_getter(df: pd.DataFrame, year: str) -> float:
    df_c = df.copy()
    df_c.set_index(['Year'], inplace=True)
    logger.info(f'Getting Grid Emissivity value for: {year}')
    value = df_c.loc[year]['Value']
    return value

def format_commodities_data(df: pd.DataFrame, material_mapper: dict) -> pd.DataFrame:
    df_c = df.copy()
    logger.info(f'Formatting the ethanol_plastics_charcoal data')
    columns_of_interest = ['Year', 'Reporter', 'Commodity Code', 'Netweight (kg)', 'Trade Value (US$)']
    df_c = df_c[columns_of_interest]
    df_c.columns = ['year', 'reporter', 'commodity_code', 'netweight', 'trade_value']
    df_c['commodity_code'] = df_c['commodity_code'].apply(lambda x: material_mapper[str(x)])
    df_c['implied_price'] = ''
    df_c['netweight'].fillna(0, inplace=True)
    for row in df_c.itertuples():
        if row.netweight == 0:
            df_c.loc[row.Index, 'implied_price'] = 0
        else:
            df_c.loc[row.Index, 'implied_price'] = row.trade_value / row.netweight
    return df_c

def commodity_data_getter(df: pd.DataFrame, material_mapper: dict, commodity: str = ''):
    df_c = df.copy()
    if commodity:
        logger.info(f'Getting the weighted average price for {commodity}')
        df_c = df_c[df_c['commodity_code'] == commodity]
        value_productsum = sum(df_c['netweight'] * df_c['implied_price']) / df_c['netweight'].sum()
        return value_productsum
    else:
        logger.info(f'Getting the weighted average price for all commodities: Ethanol, Plastic, Charcoal')
        values_dict = {}
        for commodity_ref in list(material_mapper.values()):
            new_df = df_c[df_c['commodity_code'] == commodity_ref]
            value_productsum = sum(new_df['netweight'] * new_df['implied_price']) / new_df['netweight'].sum()
            values_dict[commodity_ref] = value_productsum
    return values_dict

def scope3_ef_getter(df: pd.DataFrame, fuel: str, year: str) -> float:
    df_c = df.copy()
    fuel_names = df_c['Fuel'].unique()
    logger.info(f'Creating Scope 3 Emission Factor getter with the following metrics: {fuel_names}')
    df_c.set_index(['Fuel', 'Year'], inplace=True)
    logger.info(f'Getting {fuel} value for: {year}')
    value = df_c.loc[fuel, year]['value']
    return value

greenfield_capex_df = read_pickle_folder(PKL_FOLDER, 'greenfield_capex')
brownfield_capex_df = read_pickle_folder(PKL_FOLDER, 'brownfield_capex')
other_opex_df = read_pickle_folder(PKL_FOLDER, 'other_opex')
capex_dict = capex_dictionary_generator(greenfield_capex_df, brownfield_capex_df, other_opex_df)

# Example Loading Data with necessary subsets
feedstock_prices = read_pickle_folder(
    PKL_FOLDER, 'feedstock_prices')

steel_demand = read_pickle_folder(
    PKL_FOLDER, 'steel_demand')[['Steel Type', 'Scenario', 'Year', 'Value']]

carbon_tax_assumptions = read_pickle_folder(
    PKL_FOLDER, 'carbon_tax_assumptions')[['Year', 'Value']]

s1_emissions_factors = read_pickle_folder(
    PKL_FOLDER, 's1_emissions_factors')[['Metric', 'Value']]

s3_emissions_factors_1 = read_pickle_folder(
    PKL_FOLDER, 's3_emissions_factors_1')

s3_emissions_factors_2 = read_pickle_folder(
    PKL_FOLDER, 's3_emissions_factors_2')

grid_emissivity = read_pickle_folder(
    PKL_FOLDER, 'grid_emissivity')[['Year', 'Value']]

static_energy_prices = read_pickle_folder(
    PKL_FOLDER, 'static_energy_prices')[['Metric', 'Year', 'Value']]

ccs_co2 = read_pickle_folder(
    PKL_FOLDER, 'ccs_co2')[['Metric', 'Year', 'Value']]

tech_availability = read_pickle_folder(
    PKL_FOLDER, 'tech_availability')[['Technology', 'Year available from', 'Year available until']]

ethanol_plastic_charcoal = read_pickle_folder(
    PKL_FOLDER, 'ethanol_plastic_charcoal')

COMMODITY_MATERIAL_MAPPER = {
    '4402': 'charcoal',
    '220710': 'ethanol',
    '391510': 'plastic'
}

commodities_df = format_commodities_data(ethanol_plastic_charcoal, COMMODITY_MATERIAL_MAPPER)

# Example Use of Functions
print(capex_generator(capex_dict, 'DRI-EAF', 2020))

print(steel_demand_getter(steel_demand, 'Crude', 'BAU', 2030))

print(carbon_tax_getter(carbon_tax_assumptions, 2040))

print(scope1_emissions_getter(s1_emissions_factors, 'Biomass'))

print(ccs_co2_getter(ccs_co2, 'Steel CO2 use market', 2040))

print(static_energy_prices_getter(static_energy_prices, 'BF gas', 2026))

print(technology_availability_getter(tech_availability, 'BAT BF-BOF'))

print(grid_emissivity_getter(grid_emissivity, 2026))

print(commodity_data_getter(commodities_df, COMMODITY_MATERIAL_MAPPER))

create_data_tuples(feedstock_prices, 'FeedstockPrices')

scope3df_2_formatted = format_scope3_ef_2(s3_emissions_factors_2, EMISSIONS_FACTOR_SLAG)
slag_new_values = scope3df_2_formatted['value'].values
final_scope3_ef_df = modify_scope3_ef_1(s3_emissions_factors_1, slag_new_values, ENERGY_DENSITY_MET_COAL)

print(scope3_ef_getter(final_scope3_ef_df, 'Slag', 2030))
