"""Utility script to manipulate DataFrames"""

import pandas as pd
import numpy as np
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.location_utility import get_region_from_country_code

from mppsteel.utility.file_handling_utility import read_pickle_folder

from mppsteel.model_config import (
    PKL_DATA_INTERMEDIATE, RESULTS_REGIONS_TO_MAP,
)

logger = get_logger("DataFrame Utility")

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
    logger.info(f'Creating line through points {year_value_dict}')
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
    years = [int(year) for year in year_value_dict.keys()]
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

def move_cols_to_front(df: pd.DataFrame, cols_at_front: list):
    non_abatement_columns = list(set(df.columns).difference(set(cols_at_front)))
    return cols_at_front + non_abatement_columns

def expand_dataset_years(df: pd.DataFrame, year_pairs: list):
    df_c = df.copy()
    for year_pair in year_pairs:
        start_year, end_year = year_pair
        year_range = range(start_year+1, end_year)
        ticker = 1
        for year in year_range:
            df_c[year] = df_c[year-1] + ((df_c[end_year] / len(year_range)) * (ticker/len(year_range)))
            ticker += 1
    return df_c

def column_sorter(df: pd.DataFrame, col_to_sort: str, col_order: list):
    def sorter(column):
        correspondence = {val: order for order, val in enumerate(col_order)}
        return column.map(correspondence)
    return df.copy().sort_values(by=col_to_sort, key=sorter)


def add_scenarios(df: pd.DataFrame, scenario_dict: dict, single_line: bool = False):
    df_c = df.copy()
    if single_line:
        df_c['scenarios'] = str(scenario_dict)
    else:
        for key in scenario_dict.keys():
            df_c[f'scenario_{key}'] = scenario_dict[key]
    return df_c


def add_regions(df: pd.DataFrame, country_ref_dict: dict, country_ref_col: str, region_schema: str):
    df_c = df.copy()
    df_c[f'region_{region_schema}'] = df_c[country_ref_col].apply(lambda country: get_region_from_country_code(country, region_schema, country_ref_dict))
    return df_c


def add_results_metadata(df: pd.DataFrame, scenario_dict: dict, include_regions: bool = True, single_line: bool = False):
    country_reference_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'country_reference_dict', 'dict')
    df_c = df.copy()
    df_c = add_scenarios(df_c, scenario_dict, single_line)
    if include_regions:
        for schema in RESULTS_REGIONS_TO_MAP:
            df_c = add_regions(df_c, country_reference_dict, 'country_code', schema)
    return df_c

def return_furnace_group(furnace_dict: dict, tech:str):
    for key in furnace_dict.keys():
        if tech in furnace_dict[key]:
            return furnace_dict[key]
