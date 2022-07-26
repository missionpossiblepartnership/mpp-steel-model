"""Script for the tco and abatament optimisation functions."""
from copy import deepcopy
from functools import lru_cache
import random
from typing import Tuple

import pandas as pd
import numpy as np
from mppsteel.plant_classes.plant_choices_class import PlantChoices
from mppsteel.model_solver.material_usage_class import MaterialUsage, create_material_usage_dict

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.utils import create_bin_rank_dict, join_list_as_string, return_bin_rank
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
def tco_ranker_logic(x: float, ref_value: float) -> int:
    """If the value is greater than the minimum value times a scaler, return a rank of 3. If the value is
    greater than the minimum value times another scaler, return a rank of 2. Otherwise, return a rank of 1.

    Args:
        x (float): The value to be ranked.
        ref_value (float): The minimum value of the metric.

    Returns:
        int: A number between 1 and 3.
    """
    if x > ref_value * TCO_RANK_2_SCALER:
        return 3
    elif x > ref_value * TCO_RANK_1_SCALER:
        return 2
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
    technology_list: list,
    rank: bool = False,
    transitional_switch_mode: bool = False,
) -> Tuple[pd.DataFrame, str]:
    """Sorts (and optionally ranks) each technology from a given list for the purpose of choosing a best technology.

    Args:
        df (pd.DataFrame): A DataFrame containing either tco values or emission abatement values.
        value_col (str): The column name containing the values of the DataFrame provided in `df`.
        data_type (str): The type of data contained in `df`.
        year (int): The year you want to rank the technologies for.
        country_code (str): The country code of the plant you want to rank technologies for.
        start_tech (str): The starting technology for the plant.
        technology_list (list): A list of technologies that represent valid technology switches.
        rank (bool, optional): Decide whether to assign custom ranking logic to the technologies. Defaults to False.
        transitional_switch_mode (bool, optional): Boolean flag that determines if transitional switch logic is active. Defaults to False.

    Returns:
        Tuple[pd.DataFrame, str]: A DataFrame containing the sorted list of each technology for a given plant and technology.
    """
    # subsetting the dataframe
    df_c = df.loc[year, country_code, start_tech].copy()
    # subset switch_technology based on technology_list
    if transitional_switch_mode and start_tech not in technology_list:
        technology_list.append(start_tech)
    df_subset = df_c[df_c["switch_tech"].isin(technology_list)].copy()
    # set index as switch_tech
    df_subset = df_subset.reset_index().set_index("switch_tech")
    # sort the dataframe according to the value column
    df_subset.sort_values(value_col, ascending=True, inplace=True)
    # default ref: empty string
    tco_reference_tech = ''
    data_type_col_mapper = {
        "tco": "tco_rank_score",
        "abatement": "abatement_rank_score",
    }
    # handle case where there is only one tech option available - most likely the start_tech
    if len(df_subset) == 1:
        if rank:
            tco_reference_tech = df_subset[value_col].idxmin()
            df_subset[data_type_col_mapper[data_type]] = 1
        return df_subset, tco_reference_tech

    if rank:
        if data_type == "tco":
            if transitional_switch_mode:
                # transitionary switch case
                tco_reference_tech = start_tech
                ref_val = df_subset.loc[tco_reference_tech, value_col]
            elif "EAF" in list(df_subset.index):
                df_subset_no_eaf = df_subset.drop("EAF").copy()
                # eaf ref case: cheapest tech minus eaf
                tco_reference_tech = df_subset_no_eaf[value_col].idxmin()
                ref_val = df_subset_no_eaf[value_col].min()
            else:
                # identify the minimum value
                tco_reference_tech = df_subset[value_col].idxmin()
                ref_val = df_subset[value_col].min()
            df_subset[data_type_col_mapper[data_type]] = df_subset[value_col].apply(
                lambda x: tco_ranker_logic(x, ref_val)
            )
        elif data_type == "abatement":
            df_subset[data_type_col_mapper[data_type]] = df_subset[value_col].apply(
                lambda x: abatement_ranker_logic(x)
            )
    
    return df_subset, tco_reference_tech


def get_tco_and_abatement_values(
    tco_df: pd.DataFrame,
    emissions_df: pd.DataFrame,
    cost_value_col: str,
    year: int,
    country_code: str,
    start_tech: str,
    technology_list: list,
    rank: bool,
    transitional_switch_mode: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
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
        transitional_switch_mode (bool, optional): Boolean flag that determines if transitional switch logic is active.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, str]: Two DataFrames in a tuple, TCO Values and Abatement Values and a ref point
    """
    # Remove unavailable techs
    tco_values, tco_reference_tech = min_ranker(
        df=tco_df,
        data_type="tco",
        value_col=cost_value_col,
        year=year,
        country_code=country_code,
        start_tech=start_tech,
        technology_list=technology_list,
        rank=rank,
        transitional_switch_mode=transitional_switch_mode
    )
    abatement_values, _ = min_ranker(
        df=emissions_df,
        data_type="abatement",
        value_col="abated_combined_emissivity",
        year=year,
        country_code=country_code,
        start_tech=start_tech,
        technology_list=technology_list,
        rank=rank,
        transitional_switch_mode=transitional_switch_mode
    )
    return tco_values, abatement_values, tco_reference_tech


def record_ranking(
    combined_ranks: pd.DataFrame,
    availability_included_techs: list,
    constraint_included_techs: list,
    plant_choice_container: PlantChoices,
    year: int,
    region: str,
    plant_name: str,
    start_tech: str,
    tco_reference_tech: str,
    solver_logic: str,
    weighting_dict: dict,
    scenario_name: str,
    transitional_switch_mode: bool,
) -> None:
    """Formats the combined rank dataframe and adds it to a plant choice container class instance.

    Args:
        combined_ranks (pd.DataFrame): A DataFrame showing the ranking.
        constraint_included_techs (list): Contains technologies excluded due to resource constraints.
        plant_choice_container (PlantChoices): The PlantChoices Instance containing each plant's choices.
        year (int): The current model cycle year.
        region (str): The plant's region.
        plant_name (str): The name of the plant.
        start_tech (str): The base technonlogy for the ranking.
        tco_reference_tech (str): Reference technology for tco rank logic.
        solver_logic (str): Determines the algorithm used to pick the best technology.
        weighting_dict (dict): Weighting for tco and abatement data.
        scenario_name (str): The current scenario of the model_run.
        transitional_switch_mode (bool): Boolean flag that determines if transitional switch logic is active.
    """
    def boolean_check(row) -> bool:
        return not row.excluded_due_to_availability and row.excluded_due_to_constraints

    if not combined_ranks.empty:
        records = combined_ranks.reset_index().copy()
        records["year"] = year
        records["region"] = region
        records["plant_name"] = plant_name
        records["start_tech"] = start_tech
        records["tco_reference_tech"] = tco_reference_tech
        records["solver_logic"] = solver_logic
        records["weighting"] = str(weighting_dict)
        records["scenario_name"] = scenario_name
        records["switch_type"] = "transitional switch" if transitional_switch_mode else "main cycle switch"
        records["excluded_due_to_availability"] = records["switch_tech"].apply(lambda switch_tech: switch_tech not in availability_included_techs)
        records["excluded_due_to_constraints"] = records["switch_tech"].apply(lambda switch_tech: switch_tech not in constraint_included_techs)
        records["excluded_for_any_reason"] = records.apply(boolean_check, axis=1)

        if solver_logic == "rank":
            records = records[
                [
                    "year",
                    "region",
                    "plant_name",
                    "start_tech",
                    "tco_reference_tech",
                    "solver_logic",
                    "weighting",
                    "switch_type",
                    "scenario_name",
                    "switch_tech",
                    "tco_rank_score",
                    "abatement_rank_score",
                    "overall_rank",
                    "excluded_due_to_constraints"
                ]
            ]
        elif solver_logic in {"scaled", "scaled_bins"}:
            records = records[
                [
                    "year",
                    "region",
                    "plant_name",
                    "start_tech",
                    "tco_reference_tech",
                    "solver_logic",
                    "weighting",
                    "switch_type",
                    "scenario_name",
                    "switch_tech",
                    "tco_scaled",
                    "abatement_scaled",
                    "overall_score",
                    "excluded_due_to_constraints"
                ]
            ]
        records = records.set_index(
            [
                "year", "region", "plant_name", "solver_logic", 
                "weighting", "switch_type", "scenario_name", "start_tech"
            ]
        ).sort_index(ascending=True)
        plant_choice_container.update_records("rank", records)


def return_best_choice(best_values: list, start_tech: str, potential_techs: list):
    # pick random choice if there is more than one option
    if len(best_values) > 1:
        potential_techs = best_values.index.to_list()
        return random.choice(potential_techs)
    # pick the only option if there is one option
    elif len(best_values) == 1:
        return best_values.index.values[0]
    # pick initial tech if there are no options
    elif len(best_values) == 0:
        # print(F"**** NO BEST CHOICES!!! REVERTING TO {start_tech} *****")
        # print(join_list_as_string(potential_techs))
        return start_tech


def get_best_choice(
    tco_df: pd.DataFrame,
    emissions_df: pd.DataFrame,
    country_code: str,
    year: int,
    start_tech: str,
    solver_logic: str,
    scenario_name: str,
    weighting_dict: dict,
    technology_list: list,
    transitional_switch_mode: bool,
    regional_scrap: bool,
    plant_choice_container: PlantChoices,
    enforce_constraints: bool,
    business_case_ref: dict,
    plant_capacities: dict,
    material_usage_dict_container: dict,
    plant_name: str,
    region: str,
) -> str:
    """Returns the best technology choice from a list of potential logic according to the parameter settings provided in the function.

    Args:
        tco_df (pd.DataFrame): The TCO reference DataFrame.
        emissions_df (pd.DataFrame): The emissions abatement reference DataFrame.
        country_code (str): The country code you want to select a DataFrame for.
        year (int): The year you want to pick the best technology for.
        start_tech (str): The starting technology for the plant in the given year.
        solver_logic (str): Determines the algorithm used to pick the best technology.
        scenario_name (str): The current scenario of the model_run.
        weighting_dict (dict): A dictionary containing the weighting scenario of lowest cost vs. emission abatement.
        technology_list (list): A list of technologies that represent valid technology switches.
        transitional_switch_mode (bool): determines the column to use for TCO values
        regional_scrap (bool, optional): The scenario boolean value that determines whether there is a regional or global scrap constraints. Defaults to False.
        plant_choice_container (PlantChoices): The PlantChoices Instance containing each plant's choices.
        enforce_constraints (bool): Boolen flag to determine if constraints should affect technology availability.
        business_case_ref (dict): Standardised Business Cases.
        plant_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        material_usage_dict_container (dict, optional): Dictionary container object that is used to track the material usage within the application. Defaults to None.
        plant_name (str): The plant name.
        region (str): The plant's region.

    Returns:
        str: The best technology choice for a given year.
    """
    cost_value_col = "tco_gf_capex" if transitional_switch_mode else "tco_regular_capex"
    updated_tech_availability = technology_list
    # Scaling algorithm
    if solver_logic in {"scaled", "scaled_bins"}:
        # Calculate minimum scaled values
        tco_values, abatement_values, tco_reference_tech = get_tco_and_abatement_values(
            tco_df,
            emissions_df,
            cost_value_col,
            year,
            country_code,
            start_tech,
            technology_list,
            rank=False,
            transitional_switch_mode=transitional_switch_mode,
        )
        if enforce_constraints:
            constraint_included_techs = apply_constraints(
                business_case_ref,
                plant_capacities,
                material_usage_dict_container,
                technology_list,
                year,
                plant_name,
                region,
                start_tech,
                regional_scrap=regional_scrap,
                override_constraint=False,
                apply_transaction=False
            )
            updated_tech_availability = deepcopy(constraint_included_techs)

            if transitional_switch_mode and start_tech not in updated_tech_availability:
                updated_tech_availability.append(start_tech)

            if not updated_tech_availability:
                updated_tech_availability.append(start_tech)

        # Simply return current technology if no other options
        if len(tco_values) < 2:
            return start_tech
        # Scale the data
        tco_values_scaled = tco_values.copy()

        if solver_logic == "scaled":
            tco_values_scaled["tco_scaled"] = scale_data(
                tco_values_scaled[cost_value_col]
            )
            tco_values_scaled.drop(
                columns=tco_values_scaled.columns.difference(["tco_scaled"]),
                axis=1,
                inplace=True,
            )
            abatement_values_scaled = abatement_values.copy()
            abatement_values_scaled["abatement_scaled"] = scale_data(
                abatement_values_scaled["abated_combined_emissivity"], reverse=True
            )
            abatement_values_scaled.drop(
                columns=abatement_values_scaled.columns.difference(
                    ["abatement_scaled"]
                ),
                axis=1,
                inplace=True,
            )

        elif solver_logic == "scaled_bins":
            binned_rank_dict = create_bin_rank_dict(
                tco_values_scaled[cost_value_col],
                number_of_items=len(technology_list),
            )
            tco_values_scaled["tco_scaled"] = tco_values_scaled[cost_value_col].apply(
                lambda x: return_bin_rank(x, bin_dict=binned_rank_dict)
            )
            tco_values_scaled.drop(
                columns=tco_values_scaled.columns.difference(["tco_scaled"]),
                axis=1,
                inplace=True,
            )
            abatement_values_scaled = abatement_values.copy()
            binned_rank_dict = create_bin_rank_dict(
                tco_values_scaled["abated_combined_emissivity"],
                number_of_items=len(technology_list),
                reverse=True,
            )
            tco_values_scaled["abatement_scaled"] = tco_values_scaled[
                "abated_combined_emissivity"
            ].apply(lambda x: return_bin_rank(x, bin_dict=binned_rank_dict))
            abatement_values_scaled.drop(
                columns=abatement_values_scaled.columns.difference(
                    ["abatement_scaled"]
                ),
                axis=1,
                inplace=True,
            )

        # Join the abatement and tco data and calculate an overall score using the weightings
        combined_scales = tco_values_scaled.join(abatement_values_scaled)
        combined_scales["overall_score"] = (
            combined_scales["tco_scaled"] * weighting_dict["tco"]
        ) + (combined_scales["abatement_scaled"] * weighting_dict["emissions"])
        record_ranking(
            combined_scales,
            technology_list,
            updated_tech_availability,
            plant_choice_container,
            year,
            region,
            plant_name,
            start_tech,
            tco_reference_tech,
            solver_logic,
            weighting_dict,
            scenario_name,
            transitional_switch_mode
        )
        combined_scales.drop(
            labels=combined_scales.index.difference(updated_tech_availability),
            inplace=True,
        )
        combined_scales.sort_values("overall_score", axis=0, inplace=True)

        if solver_logic == "scaled":
            return combined_scales.idxmin()["overall_score"]

        elif solver_logic == "scaled_bins":
            min_value = combined_scales["overall_score"].min()
            best_values = combined_scales[combined_scales["overall_rank"] == min_value]

            return return_best_choice(best_values, start_tech, updated_tech_availability)

    # Ranking algorithm
    if solver_logic == "ranked":

        tco_values, abatement_values, tco_reference_tech = get_tco_and_abatement_values(
            tco_df,
            emissions_df,
            cost_value_col,
            year,
            country_code,
            start_tech,
            technology_list,
            rank=True,
            transitional_switch_mode=transitional_switch_mode
        )

        if enforce_constraints:
            constraint_included_techs = apply_constraints(
                business_case_ref,
                plant_capacities,
                material_usage_dict_container,
                technology_list,
                year,
                plant_name,
                region,
                start_tech,
                regional_scrap=regional_scrap,
                override_constraint=False,
                apply_transaction=False
            )
            updated_tech_availability = deepcopy(constraint_included_techs)
            if transitional_switch_mode and start_tech not in updated_tech_availability:
                updated_tech_availability.append(start_tech)

            if not transitional_switch_mode and 1 not in tco_values.loc[updated_tech_availability]["tco_rank_score"].values:
                updated_tech_availability.append(start_tech)

        tco_values.drop(
            columns=tco_values.columns.difference(["tco_rank_score"]),
            axis=1,
            inplace=True,
        )
        abatement_values.drop(
            columns=abatement_values.columns.difference(["abatement_rank_score"]),
            axis=1,
            inplace=True,
        )
        combined_ranks = tco_values.join(abatement_values)
        combined_ranks["overall_rank"] = (
            combined_ranks["tco_rank_score"] * weighting_dict["tco"]
        ) + (combined_ranks["abatement_rank_score"] * weighting_dict["emissions"])
        record_ranking(
            combined_ranks,
            technology_list,
            updated_tech_availability,
            plant_choice_container,
            year,
            region,
            plant_name,
            start_tech,
            tco_reference_tech,
            solver_logic,
            weighting_dict,
            scenario_name,
            transitional_switch_mode
        )
        combined_ranks.drop(
            labels=combined_ranks.index.difference(updated_tech_availability),
            inplace=True,
        )
        combined_ranks.sort_values("overall_rank", axis=0, inplace=True)
        min_value = combined_ranks["overall_rank"].min()
        best_values = combined_ranks[combined_ranks["overall_rank"] == min_value]
        return return_best_choice(best_values, start_tech, updated_tech_availability)


def subset_presolver_df(df: pd.DataFrame, subset_type: str) -> pd.DataFrame:
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
        "abated_combined_emissivity",
    ]
    index_cols = ["year", "country_code", "base_tech"]
    if subset_type == "tco_summary":
        df_c = change_cols_to_numeric(
            df_c, ["tco_regular_capex", "tco_gf_capex", "capex_value"]
        )
        df_c.rename(
            {"start_technology": "base_tech", "end_technology": "switch_tech"},
            axis=1,
            inplace=True,
        )
        df_c = df_c[tco_cols].set_index(index_cols)
        return df_c.sort_index(ascending=True)
    elif subset_type == "abatement":
        df_c = change_cols_to_numeric(df_c, ["abated_combined_emissivity"])
        df_c = df_c[emissions_cols].set_index(index_cols)
        return df_c.sort_index(ascending=True)


def apply_constraints(
    business_case_ref: dict,
    plant_capacities: dict,
    material_usage_dict_container: MaterialUsage,
    combined_available_list: list,
    year: int,
    plant_name: str,
    region: str,
    base_tech: str,
    regional_scrap: bool,
    override_constraint: bool,
    apply_transaction: bool
):
    # Constraints checks
    new_availability_list = []
    for switch_technology in combined_available_list:
        material_check_container = create_material_usage_dict(
            material_usage_dict_container,
            plant_capacities,
            business_case_ref,
            plant_name,
            region,
            year,
            switch_technology,
            regional_scrap,
            override_constraint=override_constraint,
            apply_transaction=apply_transaction
        )
        if all(material_check_container.values()):
            new_availability_list.append(switch_technology)
        failure_resources = [
            resource
            for resource in material_check_container
            if not material_check_container[resource]
        ]

        result = "PASS" if all(material_check_container.values()) else "FAIL"

        entry = {
            "plant": plant_name,
            "region": region,
            "start_technology": base_tech,
            "switch_technology": switch_technology,
            "year": year,
            "assign_case": "pre-existing plant",
            "result": result,
            "failure_resources": failure_resources,
            "pass_boolean_check": material_check_container,
        }
        material_usage_dict_container.record_results(entry)
    return new_availability_list