"""Utility script to manipulate DataFrames"""

from typing import Iterable, Sequence

import pandas as pd
import numpy as np
from mppsteel.utility.log_utility import get_logger

from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.config.model_config import RESULTS_REGIONS_TO_MAP, PKL_DATA_IMPORTS

logger = get_logger(__name__)


def create_line_through_points(
    year_value_dict: dict, line_shape: str = "straight"
) -> pd.DataFrame:
    """A function that returns a dataframe based on a few data points.

    Args:
        year_value_dict (dict): A dictionary with year, value pairings, put as many as you want, minimum two.
        line_shape (str, optional): The shape of the fitting betwene points. Defaults to 'straight'.

    Returns:
        pd.DataFrame: A dataframe with an index as year and one value column.
    """
    logger.info(f"Creating line through points {year_value_dict}")
    # Creates a pairing for all elements based on location
    def create_value_pairings(iterable: list) -> list:
        value_pairings = []
        it = iter(iterable)
        for x in it:
            try:
                value_pairings.append((x, next(it)))
            except StopIteration:
                value_pairings.append((iterable[-2], iterable[-1]))
        return value_pairings

    # Create pairings for years and values
    years = [int(year) for year in year_value_dict]
    values = list(year_value_dict.values())
    year_pairs = create_value_pairings(years)
    value_pairs = create_value_pairings(values)

    # Create dataframes for every pairing
    df_list = []
    for year_pair, value_pair in zip(year_pairs, value_pairs):
        year_range = range(year_pair[0], year_pair[1] + 1)
        start_value = value_pair[0]
        end_value = value_pair[1] + 1
        if line_shape == "straight":
            values = np.linspace(start=start_value, stop=end_value, num=len(year_range))
        df = pd.DataFrame(data={"year": year_range, "values": values})
        df_list.append(df)
    # Combine pair DataFrames into one DataFrame
    combined_df = pd.concat(df_list)
    return combined_df.set_index("year")


def move_cols_to_front(df: pd.DataFrame, cols_at_front: list[str]) -> list:
    """Function that changes the order of columns based on a list of columns you
    want at the front of a DataFrame.

    Args:
        df (pd.DataFrame): A DataFrame containing the column names you want to reorder.
        cols_at_front (list): The columns you would like at the front of the DataFrame

    Returns:
        list: A list of reordered column names.
    """
    non_abatement_columns = list(set(df.columns).difference(set(cols_at_front)))
    return cols_at_front + non_abatement_columns


def expand_dataset_year_pairs(
    df: pd.DataFrame, year_pairs: Sequence[tuple[int, int]]
) -> pd.DataFrame:
    """Expands the number of years contained in a DataFrame where the current timeseries is in intervals.

    Args:
        df (pd.DataFrame): The DataFrame timeseries you want to expand.
        year_pairs (list): A list of year pairings tuples that constitutes the lower and upper boundaries of each interval in the original data.

    Returns:
        pd.DataFrame: The expanded DataFrame Timeseries.
    """
    df_c = df.copy()
    for year_pair in year_pairs:
        start_year, end_year = year_pair
        year_range = range(start_year + 1, end_year + 1)
        interval = (df_c[end_year] - df_c[start_year]) / len(year_range)
        for year in year_range:
            df_c[year] = df_c[year - 1] + interval
    return df_c


def column_sorter(
    df: pd.DataFrame, col_to_sort: str, col_order: Iterable[str]
) -> pd.DataFrame:
    """Sorts a DataFrames values according to a specified column and the column value order.

    Args:
        df (pd.DataFrame): The DataFrame you would like to sort.
        col_to_sort (str): A string containing the name of the column you would like to sort.
        col_order (list): A list containing the order of values (descending).

    Returns:
        pd.DataFrame: A DataFrame with values sorted according to col_to_sort, ordered by 'col_order'
    """

    def sorter(column):
        correspondence = {val: order for order, val in enumerate(col_order)}
        return column.map(correspondence)

    return df.copy().sort_values(by=col_to_sort, key=sorter)


def add_scenarios(
    df: pd.DataFrame,
    scenario_dict: dict,
    single_line: bool = False,
    scenario_name: bool = True,
) -> pd.DataFrame:
    """Adds scenario metadata column(s) with metadata to each row in a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame you want to modify.
        scenario_dict (dict): A metadata dictionary with scenario information.
        single_line (bool, optional): A boolean flag to flatten the scenario dictionary into one line or one column for each dictionart item. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with additional scenario metadata column(s).
    """
    df_c = df.copy()
    if scenario_name:
        df_c["scenario"] = scenario_dict["scenario_name"]
    if single_line:
        df_c["scenarios"] = str(scenario_dict)
    else:
        for key in scenario_dict:
            df_c[f"scenario_{key}"] = scenario_dict[key]
    return df_c


def add_regions(
    df: pd.DataFrame, country_ref: dict, region_schema: str
) -> pd.DataFrame:
    """Adds regional metadata column(s) to each row in a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame you want to modify.
        country_mapper (dict): A dictionary containing the mapping of country codes to regions.
        country_ref_col (str): The column containing the country codes you want to map.
        region_schema (str): The name of the schema you want to map.

    Returns:
        pd.DataFrame: A DataFrame with additional regional metadata column(s).
    """
    df_c = df.copy()
    country_mapper = create_country_mapper(region_schema)
    df_c[f"region_{region_schema}"] = df_c["country_code"].apply(
        lambda country: country_mapper[country]
    )
    return df_c


def add_results_metadata(
    df: pd.DataFrame,
    scenario_dict: dict,
    include_regions: bool = True,
    single_line: bool = False,
    scenario_name: bool = True,
) -> pd.DataFrame:
    """Adds scenario and (optionally) regional metadata column(s) to each row in a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame you want to modify.
        scenario_dict (dict): A metadata dictionary with scenario information.
        include_regions (bool, optional): Boolean flag that optionally adds regional metadata information. Defaults to True.
        single_line (bool, optional): A boolean flag to flatten the scenario dictionary into one line or one column for each dictionart item. Defaults to False.

    Returns:
        pd.DataFrame: The name of the schema you want to map.
    """
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref", "df")
    df_c = df.copy()
    df_c = add_scenarios(df_c, scenario_dict, single_line, scenario_name)
    if include_regions:
        for schema in RESULTS_REGIONS_TO_MAP:
            df_c = add_regions(df_c, country_ref, schema)
    return df_c


def return_furnace_group(furnace_dict: dict, tech: str) -> str:
    """Returns the Furnace Group of a technology if the technology is in a furnace group list of technologies.

    Args:
        furnace_dict (dict): A mapping of each technology to a furnace group.
        tech (str): The technology you would like to map.

    Returns:
        str: The Furnace Group of the technology
    """
    for key, value in furnace_dict.items():
        if tech in furnace_dict[key]:
            return value


def melt_and_index(
    df: pd.DataFrame, id_vars: list[str], var_name: str, index: list[str]
) -> pd.DataFrame:

    """Transform a DataFrame by making it tabular and creating a multiindex.

    Args:
        df (pd.DataFrame): The Data you would like to Transform.
        id_vars (list): A list of column names you would not like to melt.
        var_name (list): The name of the variable you are melting.
        index (list): The column(s) you would like to use a MultiIndex.

    Returns:
        pd.DataFrame: The melted / tabular Dataframe.
    """
    df_c = df.copy()
    df_c = pd.melt(frame=df_c, id_vars=id_vars, var_name=var_name)
    return df_c.set_index(index)


def expand_melt_and_sort_years(
    df: pd.DataFrame, year_pairs: Sequence[tuple[int, int]]
) -> pd.DataFrame:
    """Expands a DataFrame's years according to the year pairings passed. Also melts the DataFrame based on all columns that aren't years.
    Finally Sorts the DataFrame in ascending order of the years.

    Args:
        df (pd.DataFrame): The DataFrame you want to modify.
        year_pairs (list): A list of year pairings tuples that constitutes the lower and upper boundaries of each interval in the original data.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    df_c = df.copy()
    df_c = expand_dataset_year_pairs(df_c, year_pairs)
    years = [year_col for year_col in df_c.columns if isinstance(year_col, int)]
    df_c = df_c.melt(id_vars=set(df_c.columns).difference(set(years)), var_name="year")
    return df_c.sort_values(by=["year"], axis=0)


def convert_currency_col(
    df: pd.DataFrame, curr_col: str, conversion_rate: float
) -> pd.DataFrame:
    """Converts a column currency - curr_col - using a parameter conversion_rate.

    Args:
        df (pd.DataFrame): The DataFrame containing the currency column to convert.
        curr_col (str): The name of the currency column.
        conversion_rate (float): The conversion rate to multiply the currency column with.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    df_c = df.copy()
    df_c[curr_col] = df_c[curr_col] * conversion_rate
    return df_c


def change_cols_to_numeric(df: pd.DataFrame, numeric_cols: list) -> pd.DataFrame:
    """Converts columns specified to numeric columns.

    Args:
        df (pd.DataFrame): The DataFrame with the columns to be converted.
        numeric_cols (list): A list containing the columns to convert.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    df_c = df.copy()
    for col in numeric_cols:
        df_c[col] = pd.to_numeric(df[col])
    return df_c

def change_col_type(
    df: pd.DataFrame, columns_to_change: list, new_col_type: str
) -> pd.DataFrame:
    """Converts columns specified to a certain type.

    Args:
        df (pd.DataFrame): The DataFrame with the columns to be converted.
        columns_to_change (list): The columns to change in the df.
        new_col_type (str): The new type for the columns.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    df_c = df.copy()
    for col in columns_to_change:
        df_c[col] = df_c[col].astype(new_col_type)
    return df_c



def extend_df_years(
    df: pd.DataFrame, year_column: str, year_end: int, index: list = None
) -> pd.DataFrame:
    """Extends a DataFrame with a year column by taking its final year and projecting the data columns into the future until a specified end year.

    Args:
        df (pd.DataFrame): The DataFrame to extend.
        year_column (str): The column containing the year timeseries.
        year_end (int): The year that the modified DataFrame should end.
        index (list, optional): The index column for the modified DataFrame. Defaults to None.

    Returns:
        pd.DataFrame: The modified DataFrame with the expanded years.
    """
    df_c = df.reset_index().copy() if index else df.copy()
    max_year = df_c[year_column].max()
    new_rows_full_df = pd.DataFrame().reindex_like(df_c)
    if max_year == year_end:
        return df
    if max_year > year_end:
        logger.warn(
            f"You entered {year_end}, but it is smaller than the max year of the model {max_year}. Returning original DataFrame"
        )
        return df
    if max_year != year_end:
        new_row_container = []
        for extend_year in range(max_year + 1, year_end + 1):
            new_rows = df_c[df_c[year_column] == max_year].copy()
            new_rows[year_column] = extend_year
            new_row_container.append(new_rows)
        new_rows_full_df = pd.concat(new_row_container)
        test_dataframes_equal(df_c, new_rows_full_df, year_column, max_year, year_end)
    final_df = pd.concat([df_c, new_rows_full_df]).reset_index(drop=True)
    return final_df.set_index(index) if index else final_df


def test_dataframes_equal(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    common_col: str,
    common_col_value1: int,
    common_col_value2: int,
):
    """Tests two DataFrames and checks for equality.

    Args:
        df1 (pd.DataFrame): A DataFrame to compare to df2.
        df2 (pd.DataFrame): A DataFrame to compare to df1.
        common_col (str): The common column to compare 
        common_col_value1 (int): The rows with a value to be matched to df1.
        common_col_value2 (int): The rows with a value to be matched to df2.
    """
    df1_f = df1[df1[common_col] == common_col_value1].drop(common_col, axis=1)
    df2_f = df2[df2[common_col] == common_col_value2].drop(common_col, axis=1)
    assert df1_f.equals(
        df2_f
    ), f"DataFrames not equal | common column: {common_col} | Indexes: {common_col_value1} and {common_col_value2} -> {df1_f} {df2_f}"


def move_elements_to_front_of_list(full_list: list, elements_to_move: list) -> list:
    """Move elements in a list to the front of the list

    Args:
        full_list (list): The full list you want to move.
        elements_to_move (list): The (ordered) elements to move to the front of the list

    Returns:
        list: The reordered list.
    """
    non_moved_columns = list(set(full_list).difference(set(elements_to_move)))
    return elements_to_move + non_moved_columns
