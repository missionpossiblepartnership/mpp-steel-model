"""Script for the tco and abatament optimisation functions."""
from functools import lru_cache
import random
from typing import Tuple

import pandas as pd
import numpy as np

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.utils import create_bin_rank_dict, return_bin_rank
from mppsteel.utility.dataframe_utility import change_cols_to_numeric

from mppsteel.config.model_config import (
    TCO_RANK_1_SCALER,
    TCO_RANK_2_SCALER,
    ABATEMENT_RANK_2,
    ABATEMENT_RANK_3,
)

# Create logger
logger = get_logger(__name__)


def normalise_data(arr: np.array) -> np.array:
    """Given an array, normalise it by subtracting the minimum value and dividing by the range.

    Args:
        arr (np.array): The array to normalise.

    Returns:
        np.array: The normalised data.
    """
    return arr / np.linalg.norm(arr)


def scale_data(df: pd.DataFrame, reverse: bool = False) -> pd.DataFrame:
    """It normalises the data.

    Args:
        df (pd.DataFrame): The dataframe to be scaled
        reverse (bool, optional): If True, reverse the normalization. Defaults to False.

    Returns:
        pd.DataFrame: A dataframe with the same shape as the input dataframe, but with values scaled between 0 and 1.
    """
    df_c = df.copy()
    if reverse:
        return 1 - normalise_data(df_c.values)
    return normalise_data(df_c.values)


@lru_cache(maxsize=250000)
def tco_ranker_logic(x: float, min_value: float) -> int:
    """If the value is greater than the minimum value times a scaler, return a rank of 3. If the value is
    greater than the minimum value times another scaler, return a rank of 2. Otherwise, return a rank of 1.

    Args:
        x (float): The value to be ranked.
        min_value (float): The minimum value of the metric.

    Returns:
        int: A number between 1 and 3.
    """

    if min_value is None:
        return 1
    if x > min_value * TCO_RANK_2_SCALER:
        return 3
    elif x > min_value * TCO_RANK_1_SCALER:
        return 2
    else:
        return 1


@lru_cache(maxsize=250000)
def abatement_ranker_logic(x: float) -> int:
    """Given a value, return a rank.

    Args:
        x (float): The value of the parameter to be ranked.

    Returns:
        int: The abatement rank for each row.
    """

    if x < ABATEMENT_RANK_3:
        return 3
    elif x < ABATEMENT_RANK_2:
        return 2
    else:
        return 1


def min_ranker(
    df: pd.DataFrame,
    value_col: str,
    data_type: str,
    year: int,
    country_code: str,
    start_tech: str,
    rank: bool = False,
) -> pd.DataFrame:
    """Sorts (and optionally ranks) each technology from a given list for the purpose of choosing a best technology.

    Args:
        df (pd.DataFrame): A DataFrame containing either tco values or emission abatement values.
        value_col (str): The column name containing the values of the DataFrame provided in `df`.
        data_type (str): The type of data contained in `df`.
        year (int): The year you want to rank the technologies for.
        country_code (str): The country code of the plant you want to rank technologies for.
        start_tech (str): The starting technology for the plant.
        rank (bool, optional): Decide whether to assign custom ranking logic to the technologies. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the sorted list of each technology for a given plant and technology.
    """
    df_subset = df.loc[year, country_code, start_tech].copy()
    if len(df_subset) == 1:
        df_subset = df_subset.reset_index().set_index("switch_tech")
        if rank:
            data_type_col_mapper = {
                "tco": "tco_rank_score",
                "abatement": "abatement_rank_score",
            }
            df_subset[data_type_col_mapper[data_type]] = 1
        return df_subset
    df_subset = df_subset.reset_index().set_index("switch_tech")
    df_subset.sort_values(value_col, ascending=True, inplace=True)

    if rank:
        if data_type == "tco":
            min_value = df_subset[value_col].min()
            df_subset["tco_rank_score"] = df_subset[value_col].apply(
                lambda x: tco_ranker_logic(x, min_value)
            )
        elif data_type == "abatement":
            df_subset["abatement_rank_score"] = df_subset[value_col].apply(
                lambda x: abatement_ranker_logic(x)
            )
        return df_subset
    return df_subset


def get_tco_and_abatement_values(
    tco_df: pd.DataFrame, emissions_df: pd.DataFrame, 
    cost_value_col: str, year: int, country_code: str, 
    start_tech: str, technology_list: list, rank: bool
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Sends both the TCO DataFrame and the Emissions Abatement DataFrame through a minimum ranking function and filters the list based on the `technology_list`.

    Args:
        tco_df (pd.DataFrame): The TCO DataFrame
        emissions_df (pd.DataFrame): The emissions DataFrame
        cost_value_col (str): The cost metric you want to use for TCO DataFrame. `tco_gf_capex` or `tco_regular_capex`
        year (int): The current model cycle year.
        country_code (str): The country_code that you want to run the calculation for.
        start_tech (str): The starting technology that you want to run the calculation for.
        technology_list (list): The technology list that you want to filter the values for.
        rank (bool): A scenario boolean for the ranking logic switch.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Two DataFrames in a tuple, TCO Values and Abatement Values
    """
    tco_values = min_ranker(
        df=tco_df,
        data_type="tco",
        value_col=cost_value_col,
        year=year,
        country_code=country_code,
        start_tech=start_tech,
        rank=rank,
    )
    abatement_values = min_ranker(
        df=emissions_df,
        data_type="abatement",
        value_col="abated_combined_emissivity",
        year=year,
        country_code=country_code,
        start_tech=start_tech,
        rank=rank,
    )
    # Remove unavailable techs
    tco_values = tco_values.filter(items=technology_list, axis=0)
    abatement_values = abatement_values.filter(items=technology_list, axis=0)
    return tco_values, abatement_values

def get_best_choice(
    tco_df: pd.DataFrame,
    emissions_df: pd.DataFrame,
    country_code: str,
    year: int,
    start_tech: str,
    solver_logic: str,
    weighting_dict: dict,
    technology_list: list,
    transitional_switch_mode: bool
) -> str:
    """Returns the best technology choice from a list of potential logic according to the paramter settings provided in the function.

    Args:
        tco_df (pd.DataFrame): The TCO reference DataFrame.
        emissions_df (pd.DataFrame): The emissions abatement reference DataFrame.
        country_code (str): The country code you want to select a DataFrame for.
        year (int): The year you want to pick the best technology for.
        start_tech (str): The starting technology for the plant in the given year.
        solver_logic (str): Determines the algorithm used to pick the best technology.
        weighting_dict (dict): A dictionary containing the weighting scenario of lowest cost vs. emission abatement.
        technology_list (list): A list of technologies that represent valid technology switches.
        transitional_switch_mode (bool): determines the column to use for TCO values 

    Returns:
        str: The best technology choice for a given year.
    """
    cost_value_col = 'tco_gf_capex' if transitional_switch_mode else 'tco_regular_capex'
    # Scaling algorithm
    if solver_logic in {"scaled", "scaled_bins"}:
        # Calculate minimum scaled values
        tco_values, abatement_values = get_tco_and_abatement_values(
            tco_df,
            emissions_df,
            cost_value_col,
            year,
            country_code,
            start_tech,
            technology_list,
            rank=False
        )
        # Simply return current technology if no other options
        if len(tco_values) < 2:
            return start_tech
        # Scale the data
        tco_values_scaled = tco_values.copy()

        if solver_logic == 'scaled':
            tco_values_scaled["tco_scaled"] = scale_data(tco_values_scaled[cost_value_col])
            tco_values_scaled.drop(columns=tco_values_scaled.columns.difference(["tco_scaled"]), axis=1, inplace=True)
            abatement_values_scaled = abatement_values.copy()
            abatement_values_scaled["abatement_scaled"] = scale_data(
                abatement_values_scaled["abated_combined_emissivity"], reverse=True
            )
            abatement_values_scaled.drop(columns=
                abatement_values_scaled.columns.difference(["abatement_scaled"]), axis=1, inplace=True
            )

        elif solver_logic == 'scaled_bins':
            binned_rank_dict = create_bin_rank_dict(tco_values_scaled[cost_value_col], len(technology_list))
            tco_values_scaled["tco_scaled"] = tco_values_scaled[cost_value_col].apply(
                lambda x: return_bin_rank(x, bin_dict=binned_rank_dict))
            tco_values_scaled.drop(columns=tco_values_scaled.columns.difference(["tco_scaled"]), axis=1, inplace=True)
            abatement_values_scaled = abatement_values.copy()
            binned_rank_dict = create_bin_rank_dict(tco_values_scaled['abated_combined_emissivity'], len(technology_list), reverse=True)
            tco_values_scaled["abatement_scaled"] = tco_values_scaled['abated_combined_emissivity'].apply(
                lambda x: return_bin_rank(x, bin_dict=binned_rank_dict))
            abatement_values_scaled.drop(columns=
                abatement_values_scaled.columns.difference(["abatement_scaled"]), axis=1, inplace=True
            )
    
        # Join the abatement and tco data and calculate an overall score using the weightings
        combined_scales = tco_values_scaled.join(abatement_values_scaled)
        combined_scales["overall_score"] = (
            combined_scales["tco_scaled"] * weighting_dict["tco"]
        ) + (combined_scales["abatement_scaled"] * weighting_dict["emissions"])
        combined_scales.sort_values("overall_score", axis=0, inplace=True)

        if solver_logic == 'scaled':
            return combined_scales.idxmin()["overall_score"]
        
        elif solver_logic == 'scaled_bins':
            min_value = combined_scales["overall_score"].min()
            best_values = combined_scales[combined_scales["overall_rank"] == min_value]

            # pick the only option if there is one option
            if len(best_values) == 1:
                return best_values.index.values[0]
            # pick random choice if there is more than one option
            elif len(best_values) > 1:
                potential_techs = best_values.index.to_list()
                return random.choice(potential_techs)

    # Ranking algorithm
    if solver_logic == "ranked":

        tco_values, abatement_values = get_tco_and_abatement_values(
            tco_df,
            emissions_df,
            cost_value_col,
            year,
            country_code,
            start_tech,
            technology_list,
            rank=True
        )
        tco_values = tco_values.filter(items=technology_list, axis=0)
        abatement_values = abatement_values.filter(items=technology_list, axis=0)
        tco_values.drop(columns=
            tco_values.columns.difference(["tco_rank_score"]), axis=1, inplace=True
        )
        abatement_values.drop(columns=
            abatement_values.columns.difference(["abatement_rank_score"]),
            axis=1,
            inplace=True,
        )
        combined_ranks = tco_values.join(abatement_values)
        combined_ranks["overall_rank"] = (
            combined_ranks["tco_rank_score"] * weighting_dict["tco"]
        ) + (combined_ranks["abatement_rank_score"] * weighting_dict["emissions"])
        combined_ranks.sort_values("overall_rank", axis=0, inplace=True)
        min_value = combined_ranks["overall_rank"].min()
        best_values = combined_ranks[combined_ranks["overall_rank"] == min_value]

        # pick random choice if there is more than one option
        if len(best_values) > 1:
            potential_techs = best_values.index.to_list()
            return random.choice(potential_techs)
        # pick initial tech if there are no options
        elif len(best_values) == 0:
            return start_tech
        # pick the only option if there is one option
        elif len(best_values) == 1:
            return best_values.index.values[0]


def subset_presolver_df(df: pd.DataFrame, subset_type: str = False) -> pd.DataFrame:
    """Subsets and formats the TCO or Emissions Abatement DataFrame prior to being used in the solver flow.

    Args:
        df (pd.DataFrame): The TCO or Emissions Abatement DataFrame.
        subset_type (str, optional): Determines the subsetting logic. Either `tco_summary` or `abatement`. Defaults to False.

    Returns:
        pd.DataFrame: The subsetted DataFrame.
    """
    df_c = df.copy()
    tco_cols = [
        "year",
        "base_tech",
        "switch_tech",
        "country_code",
        "tco_regular_capex",
        "tco_gf_capex",
        "capex_value",
    ]
    emissions_cols = [
        "year",
        "base_tech",
        "switch_tech",
        "country_code",
        "abated_combined_emissivity"
    ]
    index_cols = ["year", "country_code", "base_tech"]
    if subset_type == 'tco_summary':
        df_c = change_cols_to_numeric(df_c, ['tco_regular_capex', 'tco_gf_capex', 'capex_value'])
        df_c.rename({'start_technology': 'base_tech', 'end_technology': 'switch_tech'}, axis=1, inplace=True)
        df_c = df_c[tco_cols].set_index(index_cols)
        return df_c.sort_index(ascending=True)
    elif subset_type == 'abatement':
        df_c = change_cols_to_numeric(df_c, ['abated_combined_emissivity'])
        df_c = df_c[emissions_cols].set_index(index_cols)
        return df_c.sort_index(ascending=True)
