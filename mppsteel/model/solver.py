"""Main solving script for deciding investment decisions."""
from typing import Union
from operator import itemgetter
from typing import Tuple

import pandas as pd
from tqdm import tqdm

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import return_furnace_group
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file

from mppsteel.model_config import (
    MODEL_YEAR_START,
    PKL_DATA_IMPORTS,
    PKL_DATA_INTERMEDIATE,
)

from mppsteel.model_scenarios import TECH_SWITCH_SCENARIOS, SOLVER_LOGICS

from mppsteel.utility.reference_lists import (
    SWITCH_DICT,
    TECHNOLOGY_STATES,
    FURNACE_GROUP_DICT,
    TECH_MATERIAL_CHECK_DICT,
    RESOURCE_CONTAINER_REF,
    TECHNOLOGY_PHASES,
)

from mppsteel.data_loading.data_interface import load_materials, load_business_cases

from mppsteel.model.solver_constraints import (
    tech_availability_check,
    read_and_format_tech_availability,
    plant_tech_resource_checker,
    create_plant_capacities_dict,
    material_usage_per_plant,
    load_resource_usage_dict,
)

from mppsteel.model.tco_and_abatement_optimizer import get_best_choice
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Solver Logic")


def return_best_tech(
    tco_reference_data: pd.DataFrame,
    abatement_reference_data: pd.DataFrame,
    solver_logic: str,
    proportions_dict: dict,
    steel_demand_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    biomass_df: pd.DataFrame,
    ccs_co2_df: pd.DataFrame,
    tech_availability: pd.DataFrame,
    tech_avail_from_dict: dict,
    plant_capacities: dict,
    materials_list: list,
    year: int,
    plant_name: str,
    country_code: str,
    steel_demand_scenario: str,
    base_tech: str = "",
    tech_moratorium: bool = False,
    transitional_switch_only: bool = False,
    enforce_constraints: bool = False,
    material_usage_dict_container: dict = None,
    return_material_container: bool = True,
) -> Union[str, dict]:
    # Valid Switches
    combined_available_list = [
        key for key in SWITCH_DICT.keys() if key in SWITCH_DICT[base_tech]
    ]

    # Transitional switches
    if transitional_switch_only and (base_tech not in TECHNOLOGY_STATES["end_state"]):
        # Cannot downgrade tech
        # Must be current or transitional tech
        # Must be within the furnace group
        combined_available_list = set(combined_available_list).intersection(
            set(return_furnace_group(FURNACE_GROUP_DICT, base_tech))
        )

    # Availability checks
    combined_available_list = [
        tech
        for tech in combined_available_list
        if tech_availability_check(
            tech_availability, tech, year, tech_moratorium=tech_moratorium
        )
    ]

    # Add base tech if the technology is technically unavailable but is already in use
    if (base_tech not in combined_available_list) & (
        year < tech_avail_from_dict[base_tech]
    ):
        combined_available_list.append(base_tech)

    if enforce_constraints:
        # Constraints checks
        combined_available_list = plant_tech_resource_checker(
            plant_name,
            base_tech,
            year,
            steel_demand_df,
            steel_plant_df,
            steel_demand_scenario,
            business_cases,
            biomass_df,
            ccs_co2_df,
            materials_list,
            TECH_MATERIAL_CHECK_DICT,
            RESOURCE_CONTAINER_REF,
            plant_capacities,
            combined_available_list,
            material_usage_dict_container,
            "included",
        )

    best_choice = get_best_choice(
        tco_reference_data,
        abatement_reference_data,
        country_code,
        plant_name,
        year,
        base_tech,
        solver_logic,
        proportions_dict,
        combined_available_list,
    )

    if return_material_container:
        return best_choice, material_usage_dict_container
    return best_choice


def choose_technology(
    year_end: int,
    solver_logic: str,
    tech_moratorium: bool = False,
    enforce_constraints: bool = True,
    steel_demand_scenario: str = "bau",
    trans_switch_scenario: str = True,
    tech_switch_scenario: dict = {"tco": 0.6, "emissions": 0.4},
) -> dict:
    """[summary]

    Args:
        year_end (int): [description]
        rank_only (bool, optional): [description]. Defaults to False.
        tech_moratorium (bool, optional): [description]. Defaults to False.
        error_plant (str, optional): [description]. Defaults to ''.

    Returns:
        [type]: [description]
    """

    logger.info("Creating Steel plant df")

    plant_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df")
    investment_year_ref = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "plant_investment_cycles", "df"
    )

    # Constraint data
    bio_constraint_model = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "bio_constraint_model_formatted", "df"
    )
    materials = load_materials()
    ccs_co2 = read_pickle_folder(PKL_DATA_IMPORTS, "ccs_co2", "df")
    # steel_demand_df = extend_steel_demand(MODEL_YEAR_END)
    steel_demand_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted", "df"
    )
    tech_availability = read_pickle_folder(PKL_DATA_IMPORTS, "tech_availability", "df")
    ta_dict = dict(
        zip(tech_availability["Technology"], tech_availability["Year available from"])
    )
    tech_availability = read_and_format_tech_availability(tech_availability)
    plant_capacities_dict = create_plant_capacities_dict()

    # TCO & Abatement Data
    tco_reference_data = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "tco_reference_data", "df"
    )
    tco_slim = (
        tco_reference_data[
            [
                "year",
                "plant_id",
                "plant_name",
                "base_tech",
                "switch_tech",
                "country_code",
                "tco",
                "capex_value",
            ]
        ]
        .set_index(["year", "plant_name", "base_tech"])
        .copy()
    )
    steel_plant_abatement_switches = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "emissivity_abatement_switches", "df"
    )
    abatement_slim = (
        steel_plant_abatement_switches[
            [
                "year",
                "base_tech",
                "switch_tech",
                "country_code",
                "abated_combined_emissivity",
            ]
        ]
        .set_index(["year", "country_code", "base_tech"])
        .copy()
    )

    # General Reference data
    business_cases = load_business_cases()
    all_plant_names = plant_df["plant_name"].copy()

    year_range = range(MODEL_YEAR_START, year_end + 1)
    current_plant_choices = {}

    for year in tqdm(year_range, total=len(year_range), desc="Years"):
        logger.info(f"Running investment decisions for {year}")
        current_plant_choices[str(year)] = {}

        switchers = extract_tech_plant_switchers(investment_year_ref, year)
        non_switchers = list(set(all_plant_names).difference(switchers))

        switchers_df = (
            plant_df.set_index(["plant_name"]).drop(non_switchers).reset_index()
        )
        switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        non_switchers_df = (
            plant_df.set_index(["plant_name"]).drop(switchers).reset_index()
        )
        non_switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)

        if year == 2020:
            technologies = non_switchers_df["technology_in_2020"].values

        else:
            technologies = current_plant_choices[str(year - 1)].values()

        yearly_usage = material_usage_per_plant(
            non_switchers,
            technologies,
            business_cases,
            plant_df,
            plant_capacities_dict,
            steel_demand_df,
            materials,
            year,
            steel_demand_scenario,
        )
        material_usage_dict = load_resource_usage_dict(yearly_usage)
        logger.info(f"-- Running investment decisions for Non Switching Plants")
        for plant_name in non_switchers:
            if year == 2020:
                tech_in_2020 = non_switchers_df[
                    non_switchers_df["plant_name"] == plant_name
                ]["technology_in_2020"].values[0]
                current_plant_choices[str(year)][plant_name] = tech_in_2020
            else:
                current_plant_choices[str(year)][plant_name] = current_plant_choices[
                    str(year - 1)
                ][plant_name]

        logger.info(f"-- Running investment decisions for Switching Plants")
        for plant in switchers_df.itertuples():
            plant_name = plant.plant_name
            country_code = plant.country_code

            if year == 2020:
                tech_in_2020 = switchers_df[switchers_df["plant_name"] == plant_name][
                    "technology_in_2020"
                ].values[0]
                current_tech = tech_in_2020

            else:
                current_tech = current_plant_choices[str(year - 1)][plant_name]

            if (current_tech == "Not operating") or (current_tech == "Close plant"):
                current_plant_choices[str(year)][plant_name] = "Close plant"

            else:
                switch_type = (
                    investment_year_ref.reset_index()
                    .set_index(["year", "plant_name"])
                    .loc[year, plant_name]
                    .values[0]
                )

                if switch_type == "main cycle":
                    best_choice_tech, material_usage_dict = return_best_tech(
                        tco_slim,
                        abatement_slim,
                        solver_logic,
                        tech_switch_scenario,
                        steel_demand_df,
                        plant_df,
                        business_cases,
                        bio_constraint_model,
                        ccs_co2,
                        tech_availability,
                        ta_dict,
                        plant_capacities_dict,
                        materials,
                        year,
                        plant_name,
                        country_code,
                        steel_demand_scenario,
                        current_tech,
                        tech_moratorium=tech_moratorium,
                        enforce_constraints=enforce_constraints,
                        material_usage_dict_container=material_usage_dict,
                        return_material_container=True,
                    )
                    if best_choice_tech == current_tech:
                        # print(f'No change in main investment cycle in {year} for {plant_name} | {year} -> {current_tech} to {best_choice_tech}')
                        pass
                    else:
                        # print(f'Regular change in main investment cycle in {year} for {plant_name} | {year} -> {current_tech} to {best_choice_tech}')
                        pass
                    current_plant_choices[str(year)][plant_name] = best_choice_tech
                if switch_type == "trans switch":
                    best_choice_tech, material_usage_dict = return_best_tech(
                        tco_slim,
                        abatement_slim,
                        solver_logic,
                        tech_switch_scenario,
                        steel_demand_df,
                        plant_df,
                        business_cases,
                        bio_constraint_model,
                        ccs_co2,
                        tech_availability,
                        ta_dict,
                        plant_capacities_dict,
                        materials,
                        year,
                        plant_name,
                        country_code,
                        steel_demand_scenario,
                        current_tech,
                        tech_moratorium=tech_moratorium,
                        enforce_constraints=enforce_constraints,
                        material_usage_dict_container=material_usage_dict,
                        transitional_switch_only=trans_switch_scenario,
                        return_material_container=True,
                    )
                    if best_choice_tech != current_tech:
                        # print(f'Transistional switch flipped for {plant_name} in {year} -> {current_tech} to {best_choice_tech}')
                        pass
                    else:
                        # print(f'{plant_name} kept its current tech {current_tech} in transitional year {year}')
                        pass
                    current_plant_choices[str(year)][plant_name] = best_choice_tech
    return current_plant_choices


def extract_tech_plant_switchers(
    inv_cycle_ref: pd.DataFrame, year: int, combined_output: bool = True
) -> Tuple[list, list]:
    """[summary]

    Args:
        inv_cycle_ref (pd.DataFrame): [description]
        year (int): [description]
        combined_output (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """
    main_switchers = []
    trans_switchers = []
    try:
        main_switchers = (
            inv_cycle_ref.sort_index().loc[year, "main cycle"]["plant_name"].to_list()
        )
    except KeyError:
        pass
    try:
        trans_switchers = (
            inv_cycle_ref.sort_index().loc[year, "trans switch"]["plant_name"].to_list()
        )
    except KeyError:
        pass
    if combined_output:
        return main_switchers + trans_switchers
    return main_switchers, trans_switchers


@timer_func
def solver_flow(scenario_dict: dict, year_end: int, serialize: bool = False) -> dict:
    """[summary]

    Args:
        year_end (int): [description]
        serialize (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """

    tech_choice_dict = choose_technology(
        year_end=year_end,
        solver_logic=SOLVER_LOGICS[scenario_dict["solver_logic"]],
        tech_moratorium=scenario_dict["tech_moratorium"],
        enforce_constraints=scenario_dict["enforce_constraints"],
        steel_demand_scenario=scenario_dict["steel_demand_scenario"],
        trans_switch_scenario=scenario_dict["transitional_switch"],
        tech_switch_scenario=TECH_SWITCH_SCENARIOS[
            scenario_dict["tech_switch_scenario"]
        ],
    )

    if serialize:
        logger.info(f"-- Serializing dataframes")
        serialize_file(tech_choice_dict, PKL_DATA_INTERMEDIATE, "tech_choice_dict")
    return tech_choice_dict
