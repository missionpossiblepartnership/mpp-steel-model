"""Function that extends a timeseries beyond its boundaries flexible assumptions."""
#%%
import pandas as pd
import numpy as np

# Get model parameters
from model_config import PKL_FOLDER
from utils import read_pickle_folder, get_logger

# Create logger
logger = get_logger('Timeseries Extender')

def create_timeseries_extension_components(
    df: pd.DataFrame, year_colname: str, value_colname: str, new_last_year: int):
    """
    Generates the materials

    Args:
        df (pd.DataFrame): A dataframe containing a years and values column
        year_colname (str): The name of the column containing the years
        value_colname (str): The name of the column containing the values
        new_last_year (int): The year the dataframe should extend towards.

    Raises:
        ValueError: Error handling for the new_last_year parameter.

    Returns:
        df_c (pd.DataFrame): The dataframe with the Year column a a dateimte column.
        full_date_range (pd.DatetimeIndex): Generates a full date range including the additional years
        extended_date_range (pd.DatetimeIndex): Generates a partial date range based on just the additional years
        static_columns (list): Generates a list of the static columns in the DataFrame, based on the columns that don't change
        last_value (float): Generates the last value in the original Dataframe based on the last year
    """
    logger.info('Creating the timeseries extension components')

    df_c = df.copy()
    start_year = df_c[year_colname].iloc[0]
    if new_last_year <= start_year:
        raise ValueError(f'Your last year value: {new_last_year} is equal to or less than the current start year {start_year}.')

    time_series_values = [year_colname, value_colname]
    static_columns = list(set(df_c.columns.to_list()) - set(time_series_values))

    current_last_year = df_c[year_colname].iloc[-1]
    last_value = df_c.iloc[-1].value

    df_c[year_colname] = pd.to_datetime(df_c[year_colname],format='%Y')

    df_c.plot(x=year_colname, y=value_colname)
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    full_date_range = pd.date_range(
        start=str(start_year),
        end=str(new_last_year),
        freq='YS',
        normalize=True,
        closed=None
    )

    extended_date_range = pd.date_range(
        start=str(current_last_year),
        end=str(new_last_year),
        freq='YS',
        normalize=True,
        closed='right'
    )

    return df_c, full_date_range, extended_date_range, static_columns, last_value

def create_timeseries_extension_array(
    series_length: int, start_value: float, series_type: str,
    growth_type: 'str', value_change: float = 0,
    first_value: bool = False) -> np.array:
    """Creates a numpy array that represents the values in the new portion of the timeseries.

    Args:
        series_length (int): The number of intervals between the start value and the terminal value.
        start_value (float): The initial value of the series, this should be the end value of the previous timeseries.
        series_type (str): The shape of the growth from the initial value to the terminal value: 'geometric', 'logarithmic', 'linear'
        growth_type (str): The way that the array grow from the initial value: 'pct': a percentage change. 'fixed': up to a specified value. 'flat': no growth.
        value_change (float, optional): The value that determines the terminal value. Defaults to 0.
        first_value (bool, optional): Boolean that determines whether to include the first value. Defaults to False.

    Returns:
        np.array: The array of values that will form the extended time series portion.
    """
    logger.info(f'Creating the timeseries extension array. Series Type: {series_type} | Growth Type: {growth_type} | Value Change: {value_change}')
    
    # See https://numpy.org/doc/stable/reference/routines.array-creation.html for array generators
    def percentage_change(x: float, pct=float):
        return start_value + (x*pct/100)

    series_length += 1

    terminal_value = 0 # empty variable for real value
    generated_series = np.array([]) # empty variable for real series

    if growth_type == 'pct':
        terminal_value = percentage_change(start_value, value_change)
    if growth_type == 'fixed':
        terminal_value = value_change
    if growth_type == 'flat':
        terminal_value = 0

    if series_type == 'linear': # straight line growth
        generated_series = np.linspace(
            start=start_value, stop=terminal_value, num=series_length, endpoint=True)

    if series_type == 'geometric': # faster growth
        generated_series = np.geomspace(
            start=start_value, stop=terminal_value, num=series_length, endpoint=True)

    if series_type == 'logarithmic': # slowest growth / transforms into actual logs - FIX!
        generated_series = np.logspace(
            start=start_value, stop=terminal_value, num=series_length, endpoint=True)
    if first_value:
        return generated_series.round(3)
    return generated_series[1:].round(3)

def create_dict_mapper(
    original_df: pd.DataFrame,
    static_column_list: list,
    values_to_override: dict) -> dict:
    """Creates a dictionary that maps the static values of the original data to new values.
    Also accepts an input with a mapper for the values you can override.

    Args:
        original_df (pd.DataFrame): The dataframe you want to source the static values from.
        static_column_list (list): The list of columns with static values that you don't want to change.
        values_to_override (dict): A dictionary mapping of the columns with values you want to change.

    Returns:
        dict: A dictionary containing the new static values you want to use for the extended part of the timeseries.
    """
    logger.info(f'Creating a static value mapper with the following overrides:  {values_to_override}')
    new_dict_column_mapper = {}
    current_dict_mapping = dict(original_df.iloc[0]) # gets first row for mapping
    for value in static_column_list:
        if value in current_dict_mapping.keys():
            new_dict_column_mapper[value] = current_dict_mapping[value]
        if value in values_to_override.keys():
            new_dict_column_mapper[value] = values_to_override[value]
    return new_dict_column_mapper

def combine_timeseries(
    df: pd.DataFrame, added_date_range: pd.DatetimeIndex,
    values: np.array, static_col_mapper: dict) -> pd.DataFrame:
    """Produces a combined dataframe with the old timeseries and the new timeseries.

    Args:
        df (pd.DataFrame): The original timeseries
        added_date_range (pd.DatetimeIndex): The daterange for the new timeseries
        values (np.array): The values for the new timeseries
        static_col_mapper (dict): The dictionary containing the static value mapping for the new timeseries

    Returns:
        pd.DataFrame: A new combined timeseries
    """
    logger.info(f'Combining the original and extended timeseries')
    df_c = df.copy()
    new_df = pd.DataFrame(index=range(len(added_date_range)), columns=df_c.columns)
    new_df['year'] = added_date_range
    new_df['value'] = values
    for col_val in static_col_mapper.items():
        new_df[col_val[0]] = col_val[1]
    return pd.concat([df_c, new_df])

def generate_timeseries_plots(df_list: list, year_colname: str, value_colname: str):
    """Produces a plot of each timeseries.

    Args:
        df_list (list): A list of timeseries to plot.
        year_colname (str): The name of the column containing the years.
        value_colname (str): The name of the column containing the values.
    """
    logger.info(f'Generating plots for the original and extended timeseries')
    for df in df_list:
        df.plot(x=year_colname, y=value_colname)

def full_model_flow(
    df: pd.DataFrame, year_value_col_dict: dict,
    static_value_override_dict: dict,
    new_end_year: int, series_type: str,
    growth_type: str, value_change: float = 0,
    plot_dfs: bool = False
) -> pd.DataFrame:
    """A full run through the complete cycle to produce an extended timeseries.

    Args:
        df (pd.DataFrame): The original timeseries you want to extend.
        year_value_col_dict (dict): The mapping for the year and value columns
        new_end_year (int): The end value for the method.
        series_type (str): See create_timeseries_extension_array function method description.
        growth_type (str): See create_timeseries_extension_array function method description.
        value_change (float, optional): See create_timeseries_extension_array function method description.
        plot_dfs (bool, optional): Determines whether to plot the original and extended timeseries. Defaults to False.

    Returns:
        pd.DataFrame: A dataframe containing the new extended timeseries.
    """
    logger.info(f'Running through the complete timeseries generation flow.')
    df_f, full_date_range, extended_date_range, static_columns, last_value = create_timeseries_extension_components(
        df, year_value_col_dict['year'], year_value_col_dict['value'], new_end_year)

    extra_series = create_timeseries_extension_array(
        series_length=len(extended_date_range),
        start_value=last_value,
        series_type=series_type,
        growth_type=growth_type,
        value_change=value_change,
        first_value=False
    )

    dict_mapper = create_dict_mapper(df_f, static_columns, static_value_override_dict)
    combined_df = combine_timeseries(df_f, extended_date_range, extra_series, dict_mapper)
    if plot_dfs:
        generate_timeseries_plots(
            [df_f ,combined_df], year_value_col_dict['year'], year_value_col_dict['value'])

    return combined_df

# MODEL TEST
hydrogen_data_raw = read_pickle_folder(PKL_FOLDER, 'hydrogen_electrolyzer_capex')

func_dict_cols = {
    'year': 'year',
    'value': 'value'
}

new_dict_override = {
    'source': 'Model',
    'excel_tab': 'Extended from excel'
}

full_model_flow(
    df=hydrogen_data_raw,
    year_value_col_dict=func_dict_cols,
    static_value_override_dict=new_dict_override,
    new_end_year=2070,
    series_type='geometric',
    growth_type='fixed',
    value_change=50)

# %%
