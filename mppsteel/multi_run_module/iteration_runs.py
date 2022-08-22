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
from mppsteel.config.mypy_config_settings import MYPY_SCENARIO_SETTINGS_SEQUENCE, MYPY_SCENARIO_TYPE
from mppsteel.config.reference_lists import PKL_FILE_RESULTS_REFERENCE
from mppsteel.utility.dataframe_utility import change_col_type, move_elements_to_front_of_list
from mppsteel.utility.utils import reverse_dict_with_list_elements
from mppsteel.utility.file_handling_utility import (
    create_folder_if_nonexist,
    read_pickle_folder,
)

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def make_scenario_iterations(base_scenario: MYPY_SCENARIO_TYPE, scenario_settings: MYPY_SCENARIO_SETTINGS_SEQUENCE) -> list:
    """Creates a list of dictionary scenarios based on a core base scenario dict iterations of scenario settings.

    Args:
        base_scenario (dict): The base scenario dictionary that the iterations will be based on.
        scenario_settings (dict): The scenario settings that contain the iterations for each specific scenario parameter.

    Returns:
        list: A list of scenario dictionary objects.
    """
    scenario_list = []
    product_iteration = list(
        itertools.product(
            scenario_settings["carbon_tax_scenario"],
            scenario_settings["hydrogen_cost_scenario"],
            scenario_settings["electricity_cost_scenario"],
            scenario_settings["steel_demand_scenario"],
            scenario_settings["grid_scenario"],
        )
    )

    for iteration, (
        carbon_cost_scenario,
        hydrogen_cost_scenario,
        electricity_cost_scenario,
        steel_demand_scenario,
        grid_scenario,
    ) in enumerate(product_iteration):
        new_scenario_dict = dict(base_scenario)
        new_scenario_dict[
            "scenario_name"
        ] = f"{base_scenario['scenario_name']}_{iteration + 1}"
        new_scenario_dict["carbon_tax_scenario"] = carbon_cost_scenario
        new_scenario_dict["electricity_cost_scenario"] = electricity_cost_scenario
        new_scenario_dict["hydrogen_cost_scenario"] = hydrogen_cost_scenario
        new_scenario_dict["steel_demand_scenario"] = steel_demand_scenario
        new_scenario_dict["grid_scenario"] = grid_scenario
        scenario_list.append(new_scenario_dict)
    return scenario_list


def generate_scenario_iterations_reference(
    scenarios_to_iterate: list, scenario_options: dict, scenario_settings: dict
) -> pd.DataFrame:
    """Creates a DataFrame of all the scenario iterations for a list of scenarios to iterate.

    Args:
        scenarios_to_iterate (list): The list of scenarios to to create iterations from.
        scenario_options (dict): Dictionary of all the possible scenario options.
        scenario_settings (dict): The scenario settings that contain the iterations for each specific scenario parameter.

    Returns:
        pd.DataFrame: A table containing all scenario iterations.
    """
    df_list = []

    for scenario in scenarios_to_iterate:
        scenario_list = make_scenario_iterations(
            scenario_options[scenario], scenario_settings
        )
        df_list.append(pd.DataFrame(scenario_list))

    combined_df = pd.concat(df_list)
    combined_df["base_scenario"] = combined_df["scenario_name"].apply(
        lambda scenario: re.sub(UNDERSCORE_NUMBER_REGEX, "", scenario)
    )
    combined_df["iteration_scenario"] = combined_df["scenario_name"].apply(
        lambda scenario: re.findall(NUMBER_REGEX, scenario)[0].zfill(3)
    )
    new_column_order = move_elements_to_front_of_list(
        combined_df.columns, ["scenario_name", "base_scenario", "iteration_scenario"]
    )
    return combined_df[new_column_order]


def consumption_summary_iterations(
    df: pd.DataFrame, grouping_cols: list, unit_list: list
) -> pd.DataFrame:
    """Gets all columns that match specific unit extensions (value_columns) and combined them with specific grouping columns.

    Args:
        df (pd.DataFrame): A DataFrame containing results.
        grouping_cols (list): A list of grouping columns from the df.
        unit_list (list): A list of units representing value columns in the df.

    Returns:
        pd.DataFrame: A subsetted DataFrame.
    """
    resource_cols = [col for col in df.columns if any(ext in col for ext in unit_list)]
    return df[grouping_cols + resource_cols]


def combine_multiple_iterations(
    base_scenario: str, results_path_ref_dict: dict, filename: str
) -> mpd.DataFrame:
    """Combines various scenario model scenario iterations into one file.

    Args:
        base_scenario (str): The base scenario you want to combine files for.
        results_path_ref_dict (dict): The path to save the combined DataFrame for.
        filename (str): The name of the file you want to combine the iterations for.

    Returns:
        mpd.DataFrame: A modin DataFrame of all of iterations of a model run.
    """
    base_scenario_list = []
    folders = [x[0] for x in os.walk(f"{PKL_FOLDER}/iteration_runs/{base_scenario}")]
    folders = [path for path in folders if path.count("/") == 5]
    for folder_path in tqdm(folders, total=len(folders), desc="Iteration loop"):
        df: pd.DataFrame = read_pickle_folder(
            f"{folder_path}/{results_path_ref_dict[filename]}", filename, "df"
        )
        df["full_scenario_ref"] = os.path.basename(os.path.normpath(folder_path))
        df["iteration"] = df["full_scenario_ref"].apply(
            lambda scenario: re.findall(NUMBER_REGEX, scenario)[0].zfill(3)
        )
        df["base_scenario"] = df["full_scenario_ref"].apply(
            lambda scenario: re.sub(UNDERSCORE_NUMBER_REGEX, "", scenario)
        )
        if filename in {"production_resource_usage", "production_emissions"}:
            df = consumption_summary_iterations(
                df,
                grouping_cols=[
                    "year",
                    "plant_name",
                    "plant_id",
                    "technology",
                    "low_carbon_tech",
                    "base_scenario",
                    "iteration",
                    "country_code",
                    "region_rmi"
                ],
                unit_list=["capacity", "production", "_gt", "_mt", "_gj", "_pj"],
            )
        if filename in {
            "production_resource_usage",
            "production_emissions",
            "investment_results",
            "costs_of_steelmaking",
        }:
            for col in ["scenario", "scenarios", "region_continent", "region_wsa"]:
                if col in df.columns:
                    df.drop(col, axis=1, inplace=True)
        if filename in {"tco_summary_data"}:
            cols_to_drop = ["discounted_opex", "gf_capex_switch_value", "tco_gf_capex"]
            int_16_cols = ["year", "capex_value"]
            category_cols = [
                "country_code",
                "region",
                "start_technology",
                "end_technology",
            ]
            df.drop(cols_to_drop, axis=1, inplace=True)
            df = change_col_type(df, int_16_cols, "int16")
            df = change_col_type(df, category_cols, "category")
        base_scenario_list.append(df)
    return mpd.concat(base_scenario_list).reset_index(drop=True)


def serialize_iterations(
    df: mpd.DataFrame, filename: str, filename_path: str, serialize: bool
) -> None:
    """Serializes a Modin DataFrame as a feather file if the serialize flag is on.

    Args:
        df (pd.DataFrame): The modin DataFrame to serialize.
        filename (str): The name of the file.
        filename_path (str): The path the file will be serialized to.
        serialize (bool): Flag to state whether the file should be serialized.
    """
    if serialize:
        logger.info(f"Writing {filename} to feather")
        df._to_pandas().to_feather(f"{filename_path}.ftr")
        logger.info(f"Writing complete for {filename}")
    return None


def combine_files_iteration_run(
    scenarios_to_iterate: list,
    filenames: list,
    output_path: str,
    serialize: bool = False,
) -> None:
    """Flow for combining multiple scenario iterations into one file.

    Args:
        scenarios_to_iterate (list): The list of scenarios to iterate over.
        filenames (list): The list of filenames to join sequentially and independently join.
        output_path (str): The base path where the files should be serialized to.
        serialize (bool, optional): Flag to state whether the file should be serialized. Defaults to False.
    """
    iterations_file_dict = reverse_dict_with_list_elements(PKL_FILE_RESULTS_REFERENCE)
    for filename in tqdm(
        filenames, total=len(filenames), desc="Multiple Iteration Loop"
    ):
        logger.info(f"Merging iteration runs for {filename}")
        for base_scenario in tqdm(
            scenarios_to_iterate,
            total=len(scenarios_to_iterate),
            desc="Combination for Scenario Iteration",
        ):
            df = combine_multiple_iterations(
                base_scenario, iterations_file_dict, filename
            )
            scenario_path = f"{output_path}/{base_scenario}"
            create_folder_if_nonexist(scenario_path)

            if filename == "tco_summary_data":
                tco_folder_path = f"{scenario_path}/tco_folder"
                create_folder_if_nonexist(tco_folder_path)
                country_codes = df["country_code"].unique()
                for country_code in tqdm(
                    country_codes,
                    total=len(country_codes),
                    desc=f"Country code loop for {filename}",
                ):
                    df_cc = df[df["country_code"] == country_code].reset_index()
                    filename_path = f"{tco_folder_path}/{country_code}"
                    serialize_iterations(df_cc, filename, filename_path, serialize)
            else:
                filename_path = f"{scenario_path}/{filename}"
                serialize_iterations(df, filename, filename_path, serialize)
    return None
