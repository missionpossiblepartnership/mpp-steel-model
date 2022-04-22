"""Script for the tco and abatament optimisation functions."""
from functools import lru_cache

import pandas as pd
import numpy as np

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.utils import create_bin_rank_dict, return_bin_rank

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
    plant_name: str = None,
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
        plant_name (str, optional): The name of the plant. Defaults to None.
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


def get_best_choice(
    tco_df: pd.DataFrame,
    emissions_df: pd.DataFrame,
    country_code: str,
    plant_name: str,
    year: int,
    start_tech: str,
    solver_logic: str,
    weighting_dict: dict,
    technology_list: list
) -> str:
    """Returns the best technology choice from a list of potential logic according to the paramter settings provided in the function.

    Args:
        tco_df (pd.DataFrame): The TCO reference DataFrame.
        emissions_df (pd.DataFrame): The emissions abatement reference DataFrame.
        country_code (str): The country code you want to select a DataFrame for.
        plant_name (str): The name of the plant you want to optimise your selection for.
        year (int): The year you want to pick the best technology for.
        start_tech (str): The starting technology for the plant in the given year.
        solver_logic (str): Determines the algorithm used to pick the best technology.
        weighting_dict (dict): A dictionary containing the weighting scenario of lowest cost vs. emission abatement.
        technology_list (list): A list of technologies that represent valid technology switches.

    Returns:
        str: The best technology choice for a given year.
    """
    # Scaling algorithm
    if solver_logic in {"scaled", "scaled_bins"}:
        # Calculate minimum scaled values
        tco_values = min_ranker(
            df=tco_df,
            data_type="tco",
            value_col="tco",
            year=year,
            country_code=country_code,
            start_tech=start_tech,
            plant_name=plant_name,  # only for tco
            rank=False,
        )
        abatement_values = min_ranker(
            df=emissions_df,
            data_type="abatement",
            value_col="abated_combined_emissivity",
            year=year,
            country_code=country_code,
            start_tech=start_tech,
            rank=False,
        )
        # Remove unavailable techs
        tco_values = tco_values.filter(items=technology_list, axis=0)
        abatement_values = abatement_values.filter(items=technology_list, axis=0)
        # Simply return current technology if no other options
        if len(tco_values) < 2:
            return start_tech
        # Scale the data
        tco_values_scaled = tco_values.copy()

        if solver_logic == 'scaled':
            tco_values_scaled["tco_scaled"] = scale_data(tco_values_scaled["tco"])
        elif solver_logic == 'scaled_bins':
            binned_rank_dict = create_bin_rank_dict(tco_values_scaled['tco'], len(technology_list))
            tco_values_scaled["tco_scaled"] = tco_values_scaled['tco'].apply(
                lambda x: return_bin_rank(x, bin_dict=binned_rank_dict))

        tco_values_scaled.drop(columns=tco_values_scaled.columns.difference(["tco_scaled"]), axis=1, inplace=True)

        abatement_values_scaled = abatement_values.copy()

        if solver_logic == 'scaled':
            abatement_values_scaled["abatement_scaled"] = scale_data(
                abatement_values_scaled["abated_combined_emissivity"], reverse=True
            )
        elif solver_logic == 'scaled_bins':
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
        return combined_scales.idxmin()["overall_score"]

    # Ranking algorithm
    if solver_logic == "ranked":
        tco_values = min_ranker(
            df=tco_df,
            data_type="tco",
            value_col="tco",
            year=year,
            country_code=country_code,
            start_tech=start_tech,
            plant_name=plant_name,  # only for tco
            rank=True,
        )
        abatement_values = min_ranker(
            df=emissions_df,
            data_type="abatement",
            value_col="abated_combined_emissivity",
            year=year,
            country_code=country_code,
            start_tech=start_tech,
            rank=True,
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
        # pick the value with the least tco if ranked scores are tied
        if len(best_values) > 1:
            tco_values_best_tech = tco_df[tco_df["switch_tech"].isin(best_values.index)]
            tco_values_min = min_ranker(
                df=tco_values_best_tech,
                data_type="tco",
                value_col="tco",
                year=year,
                country_code=country_code,
                start_tech=start_tech,
                plant_name=plant_name,  # only for tco
                rank=False,
            )
            return tco_values_min[["tco"]].idxmin().values[0]
        elif len(best_values) == 0:
            return start_tech
        elif len(best_values) == 1:
            return best_values.index.values[0]

def change_cols_to_numeric(df: pd.DataFrame, numeric_cols: list):
    df_c = df.copy()
    for col in numeric_cols:
        df_c[col] = pd.to_numeric(df[col])
    return df_c

def subset_presolver_df(df: pd.DataFrame, subset_type: str = False):
    df_c = df.copy()
    tco_cols = [
        "year",
        "base_tech",
        "switch_tech",
        "country_code",
        "tco",
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
        df_c = change_cols_to_numeric(df_c, ['tco', 'capex_value'])
        df_c.rename({'start_technology': 'base_tech', 'end_technology': 'switch_tech'}, axis=1, inplace=True)
        df_c = df_c[tco_cols].set_index(index_cols)
        return df_c.sort_index(ascending=True)
    elif subset_type == 'abatement':
        df_c = change_cols_to_numeric(df_c, ['abated_combined_emissivity'])
        df_c = df_c[emissions_cols].set_index(index_cols)
        return df_c.sort_index(ascending=True)
