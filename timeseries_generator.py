"""Module that generates a timeseries for various purposes"""
# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from utils import get_logger

# Get model parameters
from model_config import (
    BIOMASS_AV_TS_END_VALUE, BIOMASS_AV_TS_END_YEAR, 
    BIOMASS_AV_TS_START_YEAR, CARBON_TAX_END_VALUE, 
    CARBON_TAX_END_YEAR, CARBON_TAX_START_VALUE, 
    CARBON_TAX_START_YEAR)

# Create logger
logger = get_logger('Timeseries generator')

def timeseries_generator(
    timeseries_type: str,
    start_year: int,
    end_year: int,
    end_value: float,
    start_value: float = 0,
    units: str = '') -> pd.DataFrame:
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
    # Setting values: BUSINESS LOGIC
    logger.info(f'Running {timeseries_type} timeseries generator')
    if timeseries_type == 'biomass':
        df = biomass_logic(df)
    if timeseries_type == 'carbon_tax':
        df = carbon_tax_logic(df)
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
