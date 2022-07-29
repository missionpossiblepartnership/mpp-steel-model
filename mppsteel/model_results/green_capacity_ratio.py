"""Calculates Green Technology Capacity Ratios"""

from copy import deepcopy

import pandas as pd

from mppsteel.model_solver.solver_flow_helpers import active_check_results
from mppsteel.config.model_config import MODEL_YEAR_RANGE, MODEL_YEAR_START
from mppsteel.config.reference_lists import TECHNOLOGY_PHASES
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file,
    get_scenario_pkl_path,
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def green_capacity_ratio_predata(
    plant_df: pd.DataFrame,
    tech_choices: dict,
    capacity_dict: dict,
    country_mapper: dict,
    inc_trans: bool = False,
) -> pd.DataFrame:
    """Preprocessing for the Green Capacity Ratio calculations.

    Args:
        plant_df (pd.DataFrame): The final steel plant DataFrame.
        tech_choices (dict): A dictionary of all technology choices made throughout the model.
        capacity_dict (dict): A dictionary of plant capacities in each year of the model.
        country_mapper (dict): A mapper of country_code to region.
        inc_trans (bool, optional): A boolean switch to include transitional switches in the preprocessing for green capacity ratio. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with all of the required columns to calculate the green capacity ratio.
    """

    def tech_status_mapper(tech_choice: dict, inc_trans: bool) -> bool:
        check_list = deepcopy(TECHNOLOGY_PHASES["end_state"])
        if inc_trans:
            check_list = deepcopy(
                TECHNOLOGY_PHASES["end_state"] + TECHNOLOGY_PHASES["transitional"]
            )
        if tech_choice in check_list:
            return True
        elif tech_choice in TECHNOLOGY_PHASES["initial"]:
            return False

    def fix_start_year(start_year) -> int:
        if pd.isna(start_year):
            return MODEL_YEAR_START
        elif "(anticipated)" in str(start_year):
            return int(start_year[:4])
        return int(start_year)

    df_container = []
    years = [int(key) for key in tech_choices.keys()]
    start_year_dict = dict(zip(plant_df["plant_name"], plant_df["start_of_operation"]))
    country_code_dict = dict(zip(plant_df["plant_name"], plant_df["country_code"]))
    inverse_active_plant_checker = active_check_results(
        plant_df, MODEL_YEAR_RANGE, inverse=True
    )
    for year in years:
        active_plants = [
            plant_name
            for plant_name in inverse_active_plant_checker[year]
            if inverse_active_plant_checker[year][plant_name]
        ]
        capacities = [capacity_dict[year][plant] for plant in active_plants]
        df = pd.DataFrame(
            {"year": year, "plant_name": active_plants, "capacity": capacities}
        )
        df["technology"] = df["plant_name"].apply(
            lambda plant_name: tech_choices[year][plant_name]
        )
        df_container.append(df)
    df_final = pd.concat(df_container).reset_index(drop=True)
    df_final["start_year"] = df_final["plant_name"].apply(
        lambda plant_name: fix_start_year(start_year_dict[plant_name])
    )
    df_final["green_tech"] = df_final["technology"].apply(
        lambda technology: tech_status_mapper(technology, inc_trans)
    )
    df_final["region"] = df_final["plant_name"].apply(
        lambda plant_name: country_mapper[country_code_dict[plant_name]]
    )
    return df_final


def create_gcr_df(
    green_capacity_ratio_df: pd.DataFrame, rounding: int = 1
) -> pd.DataFrame:
    """Creates the final Green Capacity Ratio DataFrame.

    Args:
        green_capacity_ratio_df (pd.DataFrame): The preprocessed DataFrame with necessary columns to create the Green Capacity Ratio DataFrame.
        rounding (int): A rounding factor for the metaresults. Defaults to 1.

    Returns:
        pd.DataFrame: The Green Capacity DataFrame with full calculations.
    """
    gcr = (
        green_capacity_ratio_df[["year", "capacity", "green_tech"]]
        .set_index(["green_tech"])
        .sort_index(ascending=True)
        .copy()
    )
    gcr_green = gcr.loc[True].reset_index().groupby(["year"]).sum()[["capacity"]].copy()
    gcr_green.rename({"capacity": "green_capacity"}, axis=1, inplace=True)
    gcr_nongreen = (
        gcr.loc[False].reset_index().groupby(["year"]).sum()[["capacity"]].copy()
    )
    gcr_nongreen.rename({"capacity": "nongreen_capacity"}, axis=1, inplace=True)
    gcr_combined = gcr_green.join(gcr_nongreen)
    gcr_combined["nongreen_capacity"] = gcr_combined["nongreen_capacity"].fillna(0)
    gcr_combined["green_capacity_ratio"] = (
        gcr_combined["green_capacity"] / gcr_combined["nongreen_capacity"]
    )
    return gcr_combined.reset_index().round(rounding)


@timer_func
def generate_gcr_df(scenario_dict: dict, serialize: bool = False, model_run: str = "") -> pd.DataFrame:
    """Complete flow to create the Green Capacity Ratio DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: The Green Capacity Ratio DataFrame with scenario metadata.
    """
    logger.info("- Starting Green Capacity Ratio")
    intermediate_path = get_scenario_pkl_path(
        scenario=scenario_dict["scenario_name"], pkl_folder_type="intermediate", model_run=model_run
    )
    final_path = get_scenario_pkl_path(
        scenario=scenario_dict["scenario_name"], pkl_folder_type="final", model_run=model_run
    )
    plant_result_df = read_pickle_folder(intermediate_path, "plant_result_df", "df")
    tech_choice_dict = read_pickle_folder(intermediate_path, "tech_choice_dict", "dict")
    rmi_mapper = create_country_mapper()
    plant_capacity_results = read_pickle_folder(
        intermediate_path, "plant_capacity_results", "df"
    )
    green_capacity_ratio_df = green_capacity_ratio_predata(
        plant_result_df, tech_choice_dict, plant_capacity_results, rmi_mapper, True
    )
    green_capacity_ratio_result = create_gcr_df(green_capacity_ratio_df)
    green_capacity_ratio_result = add_results_metadata(
        green_capacity_ratio_result,
        scenario_dict,
        include_regions=False,
        single_line=True,
        scenario_name=True,
    )
    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(green_capacity_ratio_result, final_path, "green_capacity_ratio")
    return green_capacity_ratio_result
