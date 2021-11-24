"""Minimodel for calculating Hydrogen prices"""
# For system level operations
from collections import namedtuple

# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from utils import get_logger, read_pickle_folder, serialize_df

from model_config import (
    PKL_FOLDER, EUR_USD_CONVERSION,
    HYDROGEN_PRICE_START_YEAR, HYDROGEN_PRICE_END_YEAR
    )

from hydrogen_assumptions import (
    VRE_PRICE_FAVORABLE_START, VRE_PRICE_FAVORABLE_END,
    VRE_PRICE_AVERAGE_START, VRE_PRICE_AVERAGE_END, ELECTROLYZER_LIFETIME,
    STACK_LIFETIME_START, STACK_LIFETIME_END,
    STACK_CAPEX_START, STACK_CAPEX_END,
    ENERGY_CONSUMPTION_START, ENERGY_CONSUMPTION_END,
    CAPACITY_UTILIZATION_FACTOR, FIXED_OPEX,
    LEVELIZED_H2_STORAGE_COST_FAVORABLE, LEVELIZED_H2_STORAGE_COST_AVERAGE,
    HYDROGEN_LHV, CAPITAL_RECOVERY_FACTOR
)

# Create logger
logger = get_logger('Hydrogen Minimodel')

def timeseries_generator(
    timeseries_type: str,
    start_year: int,
    end_year: int,
    units: str = '',
    **kwargs
) -> pd.DataFrame:
    """The timeseries generator function that calculates the hydrogen assumption timeseries

    Args:
        timeseries_type (str): Defines the timeseries to generate which runs the specific value column logic
        start_year (int): The start year of the timeseries
        end_year (int): The end year of the timeseries
        units (str, optional): The units of the value column. Defaults to ''.

    Returns:
        pd.DataFrame: [description]
    """

    # Define schema for the DataFrame
    df_schema = {
        'year': int,
        'value': float,
        'units': str
    }

    # Define the year range for the df
    year_range = range(start_year, end_year+1)
    year_range_length = len(year_range)-1

    # Create the DataFrame
    df = pd.DataFrame(
        index=pd.RangeIndex(0, len(year_range)),
        columns= [key.lower() for key in df_schema.keys()]
    )

    # Define and format year and units columns
    df['year'] = year_range
    df['units'] = units
    df['units'] = df['units'].apply(lambda x: x.lower())

    hydrogen_scenario = ''
    if kwargs:
        hydrogen_scenario = kwargs['scenario']

    def vre_price_selector(scenario: str, year: int) -> float:
        if (scenario == 'favorable') & (year == HYDROGEN_PRICE_START_YEAR):
            return VRE_PRICE_FAVORABLE_START.value
        if (scenario == 'favorable') & (year == HYDROGEN_PRICE_END_YEAR):
            return VRE_PRICE_FAVORABLE_END.value
        if (scenario == 'average') & (year == HYDROGEN_PRICE_START_YEAR):
            return VRE_PRICE_AVERAGE_START.value
        if (scenario == 'average') & (year == HYDROGEN_PRICE_END_YEAR):
            return VRE_PRICE_AVERAGE_END.value

    # Define the one generic timeseries function to rule them all
    def generic_value_logic(
        df: pd.DataFrame,
        value_start_price: float,
        value_end_price: float,
        mid_values_function
    ) -> pd.DataFrame:
        """A generic function for creating the values of column of a timeseries dataframe.

        Args:
            df (pd.DataFrame): The timeseries dataframe with an empty values column
            value_start_price (float): The start price for the values column
            value_end_price (float): The end price for the values column
            mid_values_function ([type]): The function that calculates the middle values in the values column

        Returns:
            pd.DataFrame: A DataFrame with the calculated timeseries.
        """

        df_c = df.copy()
        start_price = value_start_price
        end_price = value_end_price
        for row in df_c.itertuples():
            # first year
            if row.Index == 0:
                df_c.loc[row.Index, 'value'] = start_price
            # middle years
            elif row.Index < len(year_range)-1:
                previous_value = df_c.loc[row.Index-1, 'value']
                df_c.loc[row.Index, 'value'] = mid_values_function(start_price, end_price, previous_value)
            # last year
            else:
                df_c.loc[row.Index, 'value'] = end_price
        return df_c

    # Define middle values logic for each type of timeseries
    def vre_price_logic_mid_values(start_price, end_price, previous_value):
        return ((end_price/start_price)**(1/year_range_length))*previous_value

    def stack_lifetime_logic_mid_values(start_price, end_price, previous_value):
        return previous_value+((end_price-start_price)/year_range_length)

    def stack_capex_logic_mid_values(start_price, end_price, previous_value):
        return ((end_price/start_price)**(1/year_range_length))*previous_value

    def energy_consumption_logic_mid_values(start_price, end_price, previous_value):
        return ((end_price/start_price)**(1/year_range_length))*previous_value

    # Apply business logic to each type of timeseries
    logger.info(f'creating {timeseries_type} timeseries')
    if timeseries_type == 'vre_price':
        df = generic_value_logic(
            df=df,
            value_start_price=vre_price_selector(
                hydrogen_scenario, HYDROGEN_PRICE_START_YEAR)*EUR_USD_CONVERSION/1000,
            value_end_price=vre_price_selector(
                hydrogen_scenario, HYDROGEN_PRICE_END_YEAR)*EUR_USD_CONVERSION/1000,
            mid_values_function=vre_price_logic_mid_values
        )
    if timeseries_type == 'stack_lifetime':
        df = generic_value_logic(
            df=df,
            value_start_price=STACK_LIFETIME_START.value / 365 / 24 / CAPACITY_UTILIZATION_FACTOR.value,
            value_end_price=STACK_LIFETIME_END.value / 365 / 24 / CAPACITY_UTILIZATION_FACTOR.value,
            mid_values_function=stack_lifetime_logic_mid_values
        )
    if timeseries_type == 'stack_capex':
        df = generic_value_logic(
            df=df,
            value_start_price=STACK_CAPEX_START.value*EUR_USD_CONVERSION,
            value_end_price=STACK_CAPEX_END.value*EUR_USD_CONVERSION,
            mid_values_function=stack_capex_logic_mid_values
        )
    if timeseries_type == 'energy_consumption':
        df = generic_value_logic(
            df=df,
            value_start_price=ENERGY_CONSUMPTION_START.value,
            value_end_price=ENERGY_CONSUMPTION_END.value,
            mid_values_function=energy_consumption_logic_mid_values
        )

    # change the column types
    for key in df_schema.keys():
        df[key].astype(df_schema[key])

    # Change the metric name and remove the trailing underscore
    metric_name = f'{timeseries_type}_{hydrogen_scenario}'
    if metric_name[-1:] == '_':
        metric_name = metric_name[:-1]
    df['metric'] = metric_name

    return df

def create_df_grid(df_list: list) -> pd.DataFrame:
    """Combines the dataframes passed to it as a combined dataframe with a year index.

    Args:
        df_list (list): A list of DataFrames

    Returns:
        pd.DataFrame: A dataframe with all of the dataframe columns named as the metric
    """
    logger.info(f'creating a combined dataframe of the hydrogen timeseries')
    new_df_list = []
    for df in df_list:
        df_c = df.copy()
        col_name = df_c.loc[0, 'metric']
        df_c.rename(columns={'value': f'{col_name}'}, inplace=True)
        df_c.drop(['metric', 'units'], axis=1, inplace=True)
        df_c.set_index('year', drop=True, inplace=True)
        new_df_list.append(df_c)
    combined_df = pd.concat(new_df_list, axis=1, join='inner')
    return combined_df

def create_green_h2_prices(df: pd.DataFrame, scenario: str, as_gj: bool = False) -> pd.DataFrame:
    """Create green hydrogren prices on hydrogen assumption timseries

    Args:
        df (pd.DataFrame): Grid reference dataframe timeseries
        scenario (str): favorable or average
        as_gj (bool, optional): calculate values as gigajoule (GJ). Defaults to False.

    Returns:
        DataFrame: A DataFrame with just green hydrogen values
    """
    logger.info(f'creating dataframe for green hydrogen for the {scenario} scenario')
    df_c = df.copy()
    new_colname = f'green_h2_price_{scenario}'
    df_c[new_colname] = ''

    def vre_and_storage_prices(df_row: namedtuple) -> tuple:
        if scenario == 'average':
            vre_price = df_row.vre_price_average
            storage_price = LEVELIZED_H2_STORAGE_COST_AVERAGE.value
        if scenario == 'favorable':
            vre_price = df_row.vre_price_favorable
            storage_price = LEVELIZED_H2_STORAGE_COST_FAVORABLE.value
        return vre_price, storage_price

    logger.info('| calculating green_h2 prices')
    for row in df_c.itertuples():
        vre_price, storage_price = vre_and_storage_prices(row)
        energy_cons = row.energy_consumption*vre_price
        capex = row.electrolyzer_capex*row.energy_consumption*CAPITAL_RECOVERY_FACTOR
        opex = row.electrolyzer_capex*row.energy_consumption*FIXED_OPEX.value
        replacements = row.required_stack_replacements*row.stack_capex*row.energy_consumption/ELECTROLYZER_LIFETIME.value
        utilization = 365 * 24 * CAPACITY_UTILIZATION_FACTOR.value
        storage_costs = storage_price*EUR_USD_CONVERSION
        value = energy_cons + ((capex + opex + replacements) / utilization) + storage_costs
        df_c.loc[row.Index, new_colname] = value

    df_c.reset_index(inplace=True)
    df_c.rename(columns={new_colname: 'value'}, inplace=True)
    df_c['metric'] = new_colname
    df_c['scenario'] = scenario
    df_c['unit'] = 'EUR / kg'

    if as_gj:
        df_c['unit'] = 'EUR / GJ'
        df_c['value'] = df_c['value'].apply(lambda x: x*1000/HYDROGEN_LHV.value)

    return df_c[['metric', 'scenario', 'year', 'unit', 'value']]

def create_required_stack_replacements_df(stack_df: pd.DataFrame) -> pd.DataFrame:
    """Creates the hydrogen assumptions timeseries based on the stack lifetime dataframe.

    Args:
        stack_df (pd.DataFrame): The stack lifetime dataframe

    Returns:
        pd.DataFrame: The required stack replacements dataframe
    """
    stack_df_c = stack_df.copy()
    stack_df_c['value'] = stack_df_c['value'].apply(
        lambda x: np.floor(ELECTROLYZER_LIFETIME.value/x))
    stack_df_c['metric'] = 'required_stack_replacements'
    stack_df_c['units'] = ''
    return stack_df_c

# Creating hydrogen assumptions timeseries
vre_price_average = timeseries_generator(
    'vre_price', HYDROGEN_PRICE_START_YEAR, HYDROGEN_PRICE_END_YEAR, units='EUR / MWh', scenario='average')
vre_price_favorable = timeseries_generator(
    'vre_price', HYDROGEN_PRICE_START_YEAR, HYDROGEN_PRICE_END_YEAR, units='EUR / MWh', scenario='favorable')
stack_lifetime = timeseries_generator(
    'stack_lifetime', HYDROGEN_PRICE_START_YEAR, HYDROGEN_PRICE_END_YEAR, units='years')
required_stack_replacements = create_required_stack_replacements_df(stack_lifetime)
stack_capex = timeseries_generator(
    'stack_capex', HYDROGEN_PRICE_START_YEAR, HYDROGEN_PRICE_END_YEAR, units='EUR / kW')
energy_consumption = timeseries_generator(
    'energy_consumption', HYDROGEN_PRICE_START_YEAR, HYDROGEN_PRICE_END_YEAR, units='kWh / kg H2')

logger.info('Reading electrolyzer capex timeseries')
electrolyzer_capex_timeseries = read_pickle_folder(PKL_FOLDER, 'hydrogen_electrolyzer_capex')[['metric', 'year', 'units', 'value']]
electrolyzer_capex_timeseries['metric'] = 'electrolyzer_capex'

# Combining all dataframes into one
df_grid_reference = create_df_grid([
    vre_price_average, vre_price_favorable, electrolyzer_capex_timeseries,
    stack_lifetime, required_stack_replacements, stack_capex, energy_consumption
    ])

# Calculating green hydrogen prices
green_h2_prices_average_gj = create_green_h2_prices(df_grid_reference, 'average', as_gj=True)
green_h2_prices_favorable_gj = create_green_h2_prices(df_grid_reference, 'favorable', as_gj=True)
hydrogen_minimodel_timeseries = pd.concat([green_h2_prices_favorable_gj, green_h2_prices_average_gj])

# Serialize timeseries
serialize_df(hydrogen_minimodel_timeseries, PKL_FOLDER, 'hydrogen_minimodel_timeseries')
