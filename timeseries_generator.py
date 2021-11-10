"""Module that generates a timeseries for various purposes"""
from collections import namedtuple

# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from utils import get_logger, read_pickle_folder, serialize_df

# Get model parameters
from model_config import (
    PKL_FOLDER,
    BIOMASS_AV_TS_END_VALUE, BIOMASS_AV_TS_END_YEAR,
    BIOMASS_AV_TS_START_YEAR, CARBON_TAX_END_VALUE,
    CARBON_TAX_END_YEAR, CARBON_TAX_START_VALUE,
    CARBON_TAX_START_YEAR, ELECTRICITY_PRICE_START_YEAR,
    ELECTRICITY_PRICE_END_YEAR)

# Create logger
logger = get_logger('Timeseries generator')

PowerGridTuple = namedtuple('Power_Grid_Assumptions', ['metric', 'unit', 'year', 'value'])
grid_electricity_price_favorable_2035 = PowerGridTuple(metric='grid_electricity_price_favorable', unit='USD/MWh', year=2035, value=29)
grid_electricity_price_avg_2035 = PowerGridTuple(metric='grid_electricity_price_avg', unit='USD/MWh', year=2035, value=57)
deeply_decarbonised_power_system_price_avg = PowerGridTuple(metric='deeply_decarbonised_power_system_price_avg', unit='percentage', year=2050, value=0.19)
deeply_decarbonised_power_system_price_increase = PowerGridTuple(metric='deeply_decarbonised_power_system_price_increase', unit='USD/MWh', year=2050, value=15)
grid_electricity_price_favorable_2020 = PowerGridTuple(metric='grid_electricity_price_favorable', unit='USD/MWh', year=202, value=29)
eur_usd_conversion = 0.877

def get_grid_refs(df: pd.DataFrame, geography: str, metrics: list) -> pd.DataFrame:
    return df[(df['Geography (NRG_PRC)'] == geography) & (df['Metric'].isin(metrics))]['Value'].tolist()
power_grid_assumptions = read_pickle_folder(PKL_FOLDER)['power_grid_assumptions']
grid_electricity_price_sweden = sum(get_grid_refs(power_grid_assumptions, 'Sweden', ['Energy and supply','Network costs']))*1000
grid_electricity_price_eu = sum(get_grid_refs(power_grid_assumptions, 'European Union A', ['Energy and supply','Network costs']))*1000
t_and_d_premium = sum(get_grid_refs(power_grid_assumptions, 'European Union A', ['Network costs'])) / sum(get_grid_refs(power_grid_assumptions, 'European Union A', ['Energy and supply','Network costs']))
diff_in_price_between_mid_and_large_business = 1 - ( sum(get_grid_refs(power_grid_assumptions, 'European Union A', ['Energy and supply'])) / sum(get_grid_refs(power_grid_assumptions, 'European Union B', ['Energy and supply'])) )

def grid_price_selector(year:int, scenario: str):
    if (scenario == 'favorable') & (year == 2020):
        return grid_electricity_price_sweden
    elif (scenario == 'average') & (year == 2020):
        return grid_electricity_price_eu
    elif (scenario == 'favorable') & (year == 2035):
        return grid_electricity_price_favorable_2035.value
    elif (scenario == 'average') & (year == 2035):
        return grid_electricity_price_avg_2035.value

def grid_price_2035(scenario: str):
    return grid_price_selector(2035, scenario)*eur_usd_conversion*(1+t_and_d_premium)*(1-diff_in_price_between_mid_and_large_business)

def grid_price_last_year(scenario: str):
    if scenario == 'favorable':
        return grid_price_2035(scenario)+(deeply_decarbonised_power_system_price_increase.value*eur_usd_conversion)
    elif scenario == 'average':
        return grid_price_selector(2020, scenario)*(1-deeply_decarbonised_power_system_price_avg.value)

def timeseries_generator(
    timeseries_type: str,
    start_year: int,
    end_year: int,
    end_value: float,
    start_value: float = 0,
    units: str = '',
    **kwargs
    ) -> pd.DataFrame:
    """Function that generates a timeseries based on particular logic

    Args:
        timeseries_type (str): Defines the timeseries to produce. Options: Biomass, Carbon Tax
        start_year (int): Defines the start date of the timeseries
        end_year (int): Defines the end date of the timeseries
        end_value (float): Defines the terminal value of the timeseries.
        start_value (float, optional): Defines the starting value of the timeseries. Defaults to 0.
        units (str, optional): [description]. Define units of the timeseries values. Defaults to ''.

    Returns:
        DataFrame: A DataFrame of the timeseries.
    """
    # Define schema for the DataFrame
    df_schema = {
        'year': int,
        'value': float,
        'units': str
    }
    # Define the year range for the df
    year_range = range(start_year, end_year+1)
    # Create the DataFrame
    df = pd.DataFrame(
        index=pd.RangeIndex(0, len(year_range)),
        columns= [key.lower() for key in df_schema.keys()]
    )
    # Define the year columns
    df['year'] = year_range
    df['units'] = units
    df['units'] = df['units'].apply(lambda x: x.lower())

    power_projection_type = ''
    if kwargs:
        power_projection_type = kwargs['projection_type']

    def biomass_logic(df: pd.DataFrame) -> pd.DataFrame:
        """Applies logic to generate biomass timeseries

        Args:
            df (pd.DataFrame): A dataframe with empty values column.

        Returns:
            pd.DataFrame: A dataframe with the value logic applied.
        """
        df_c = df.copy()
        for row in df_c.itertuples():
            if row.Index < 2: # skip first 2 years
                df_c.loc[row.Index, 'value'] = 0
            elif row.Index < len(year_range)-1: # logic for remaining years except last year
                df_c.loc[row.Index, 'value'] = end_value / (1+(np.exp(-0.45 * (row.year - 2035))))
            else:
                df_c.loc[row.Index, 'value'] = end_value # logic for last year
        return df_c
    def carbon_tax_logic(df: pd.DataFrame) -> pd.DataFrame:
        """Applies logic to generate carbon tax timeseries

        Args:
            df (pd.DataFrame): A dataframe with empty values column.

        Returns:
            pd.DataFrame: A dataframe with the value logic applied.
        """
        df_c = df.copy()
        for row in df_c.itertuples():
            if row.Index == 0: # skip first year
                df_c.loc[row.Index, 'value'] = start_value
            elif row.Index < len(year_range)-1:
                # logic for remaining years except last year
                df_c.loc[row.Index, 'value'] = (end_value/len(year_range)) * (row.year - start_year)
            else:
                df_c.loc[row.Index, 'value'] = end_value # logic for last year
        return df_c

    def power_grid_logic(df: pd.DataFrame, projection_type: str = power_projection_type, lowest_price_year: int = 2035):
        df_c = df.copy()
        for row in df_c.itertuples():
            # skip first x years
            if row.Index == 0:
                df_c.loc[row.Index, 'value'] = grid_price_selector(2020, projection_type)
            # first half years
            elif row.Index < lowest_price_year-start_year:
                df_c.loc[row.Index, 'value'] = ((grid_price_2035(projection_type)/grid_price_selector(2020, projection_type))**(1/(lowest_price_year-start_year)))*df_c.loc[row.Index-1, 'value']
            # middle year
            elif row.Index == lowest_price_year-start_year:
                df_c.loc[row.Index, 'value'] = grid_price_2035(projection_type)
            # second half years    
            elif row.Index > lowest_price_year-start_year:
                df_c.loc[row.Index, 'value'] = ((grid_price_last_year(projection_type)/grid_price_2035(projection_type))**(1/(end_year-lowest_price_year)))*df_c.loc[row.Index-1, 'value']
            # final years
            else:
                df_c.loc[row.Index, 'value'] = grid_price_last_year(projection_type)
        # create a column
        df_c['category'] = 'grid electricity price'
        df_c['scenario'] = f'{projection_type}'
        return df_c
    
    # Setting values: BUSINESS LOGIC
    logger.info(f'Running {timeseries_type} timeseries generator')
    if timeseries_type == 'biomass':
        df = biomass_logic(df)
    if timeseries_type == 'carbon_tax':
        df = carbon_tax_logic(df)
    if timeseries_type == 'power':
        df = power_grid_logic(df)
    # change the column types
    for key in df_schema.keys():
        df[key].astype(df_schema[key])
    logger.info(f'{timeseries_type} timeseries complete')
    return df

biomass_availability = timeseries_generator(
    'biomass', BIOMASS_AV_TS_START_YEAR, BIOMASS_AV_TS_END_YEAR, BIOMASS_AV_TS_END_VALUE)

carbon_tax = timeseries_generator(
    'carbon_tax', CARBON_TAX_START_YEAR, CARBON_TAX_END_YEAR,
    CARBON_TAX_END_VALUE, CARBON_TAX_START_VALUE
)

favorable_ts = timeseries_generator('power', ELECTRICITY_PRICE_START_YEAR, ELECTRICITY_PRICE_END_YEAR, 0, units='USD', projection_type='favorable')
average_ts = timeseries_generator('power', ELECTRICITY_PRICE_START_YEAR, ELECTRICITY_PRICE_END_YEAR, 0, units='USD', projection_type='average')
electricity_minimodel_timeseries = pd.concat([favorable_ts, average_ts])
serialize_df(electricity_minimodel_timeseries, PKL_FOLDER, 'electricity_minimodel_timeseries')
