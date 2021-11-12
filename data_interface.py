"""Functions to access data sources"""
# For Data Manipulation
import pandas as pd

# For logger and units dict
from utils import get_logger, read_pickle_folder

# Get model parameters
from model_config import PKL_FOLDER

# Create logger
logger = get_logger('Data Interface')

def capex_melt_and_format(df: pd.DataFrame) -> pd.DataFrame:
    """Make the capex dataframes tabular and create a multiindex

    Args:
        df (pd.DataFrame): The data import of the capex tables

    Returns:
        pd.DataFrame: A datframe of the tabular dataframe
    """
    df_c = df.copy()
    df_c = pd.melt(frame=df_c, id_vars=['Technology'], var_name='Year')
    df_c.set_index(['Technology', 'Year'], inplace=True)
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

    greenfield = capex_dictionary['greenfield'].loc[technology, year].value
    brownfield = capex_dictionary['brownfield'].loc[technology, year].value
    other_opex = capex_dictionary['other_opex'].loc[technology, year].value

    capex_dict = {
        'greenfield': greenfield,
        'brownfield': brownfield,
        'other_opex': other_opex
    }

    if output_type == 'all':
        return capex_dict
    return capex_dict[output_type]

greenfield_capex_df = read_pickle_folder(PKL_FOLDER, 'greenfield_capex')
brownfield_capex_df = read_pickle_folder(PKL_FOLDER, 'brownfield_capex')
other_opex_df = read_pickle_folder(PKL_FOLDER, 'other_opex')

brownfield_capex_df.drop(
    ['Available from', 'Available until', 'Technology type'], axis=1, inplace=True)

capex_dictionary = {
    'greenfield': capex_melt_and_format(greenfield_capex_df),
    'brownfield': capex_melt_and_format(brownfield_capex_df),
    'other_opex': capex_melt_and_format(other_opex_df)
}

example_result = capex_generator(capex_dictionary, 'DRI-EAF', 2025)

print(example_result)
