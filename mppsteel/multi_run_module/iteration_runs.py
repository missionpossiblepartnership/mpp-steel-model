"""Script to manage making iteration model runs"""

import itertools
import os
import re
from typing import Union

import pandas as pd
import modin.pandas as mpd
from tqdm import tqdm

from mppsteel.config.model_config import (
    NUMBER_REGEX,
    PKL_FOLDER,
    UNDERSCORE_NUMBER_REGEX,
)
from mppsteel.config.reference_lists import PKL_FILE_RESULTS_REFERENCE
from mppsteel.utility.dataframe_utility import move_columns_to_front
from mppsteel.utility.utils import reverse_dict_with_list_elements
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file,
)

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)

def make_scenario_iterations(base_scenario: dict, scenario_settings: dict):
    scenario_list = []
    product_iteration = list(itertools.product(
            scenario_settings["carbon_tax_scenario"], 
            scenario_settings["hydrogen_cost_scenario"], 
            scenario_settings["electricity_cost_scenario"], 
            scenario_settings["steel_demand_scenario"], 
            scenario_settings["grid_scenario"]
        )
    )

    for iteration, (
        carbon_cost_scenario, hydrogen_cost_scenario, electricity_cost_scenario, 
        steel_demand_scenario, grid_scenario) in enumerate(product_iteration
    ):
        new_scenario_dict = dict(base_scenario)
        new_scenario_dict["scenario_name"] = f"{base_scenario['scenario_name']}_{iteration + 1}"
        new_scenario_dict["carbon_tax_scenario"] = carbon_cost_scenario
        new_scenario_dict["electricity_cost_scenario"] = electricity_cost_scenario
        new_scenario_dict["hydrogen_cost_scenario"] = hydrogen_cost_scenario
        new_scenario_dict["steel_demand_scenario"] = steel_demand_scenario
        new_scenario_dict["grid_scenario"] = grid_scenario
        scenario_list.append(new_scenario_dict)
    return scenario_list


def generate_scenario_iterations_reference(
    scenarios_to_iterate: list, scenario_options: dict, scenario_settings: dict) -> pd.DataFrame:
    df_list = []

    for scenario in scenarios_to_iterate:
        scenario_list = make_scenario_iterations(scenario_options[scenario], scenario_settings)
        df_list.append(pd.DataFrame(scenario_list))
        
    combined_df = pd.concat(df_list)
    combined_df["base_scenario"] = combined_df["scenario_name"].apply(
        lambda scenario: re.sub(UNDERSCORE_NUMBER_REGEX, "", scenario)
    )
    combined_df["iteration_scenario"] = combined_df["scenario_name"].apply(
        lambda scenario: re.findall(NUMBER_REGEX,scenario)[0].zfill(3)
    )
    new_column_order = move_columns_to_front(
        combined_df.columns, ["scenario_name", "base_scenario", "iteration_scenario"]
    )
    return combined_df[new_column_order]

def combine_multiple_iterations(
    scenarios_to_iterate: list, results_path_ref_dict: dict, filename: str
    ) -> Union[pd.DataFrame, mpd.DataFrame]:
    file_df_list = []
    for base_scenario in tqdm(scenarios_to_iterate, total=len(scenarios_to_iterate), desc="Combination for Scenario Iteration"):
        folders = [x[0] for x in os.walk(f"{PKL_FOLDER}/iteration_runs/{base_scenario}")]
        folders = [path for path in folders if path.count("/") == 5]
        for folder_path in tqdm(folders, total=len(folders), desc="Iteration loop"):
            df = read_pickle_folder(f"{folder_path}/{results_path_ref_dict[filename]}", filename, "df")
            df["full_scenario_ref"] = os.path.basename(os.path.normpath(folder_path))
            df["iteration"] = df["full_scenario_ref"].apply(
                lambda scenario: re.findall(NUMBER_REGEX, scenario)[0].zfill(3)
            )
            df["base_scenario"] = df["full_scenario_ref"].apply(
                lambda scenario: re.sub(UNDERSCORE_NUMBER_REGEX, "", scenario)
            )
            file_df_list.append(df)
    return mpd.concat(file_df_list).reset_index(drop=True)

def consumption_summary_iterations(
    df: pd.DataFrame, grouping_cols: list, unit_list: list
) -> pd.DataFrame:
    resource_cols = [col for col in df.columns if any(ext in col for ext in unit_list)]
    combined_cols = grouping_cols + resource_cols
    return df[combined_cols]


def combine_files_iteration_run(scenarios_to_iterate: list, filenames: list, output_path: str, to_feather: bool = False) -> None:
    iterations_file_dict = reverse_dict_with_list_elements(PKL_FILE_RESULTS_REFERENCE)
    for filename in filenames:
        logger.info(f"Merging iteration runs for {filename}")
        df = combine_multiple_iterations(scenarios_to_iterate, iterations_file_dict, filename)
        if "file_df_list" in globals():
            del file_df_list
        if filename in ["production_resource_usage", "production_emissions"]:
            df = consumption_summary_iterations(
                df,
                grouping_cols=["base_scenario", "iteration", "year", "region_rmi", "technology"], 
                unit_list=["_gt", "_mt", "_gj", "_pj"]
            )
        if filename in ["production_resource_usage", "production_emissions", "investment_results", "costs_of_steelmaking"]:
            for col in ["scenario", "scenarios", "region_continent", "region_wsa"]:
                if col in df.columns:
                    df.drop(col, axis=1, inplace=True)
        if to_feather:
            logger.info(f"Writing file to feather for {filename}")
            df.to_feather(f"{output_path}/{filename}.ftr")
        else:
            serialize_file(df._to_pandas(), output_path, filename)
        logger.info(f"Writing complete for {filename}")
