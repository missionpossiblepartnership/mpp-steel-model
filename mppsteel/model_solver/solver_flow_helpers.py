"""Helper functions for the main Solver Flow"""

import pandas as pd

from typing import Tuple

from mppsteel.plant_classes.plant_investment_cycle_class import PlantInvestmentCycle
from mppsteel.plant_classes.capacity_constraint_class import PlantCapacityConstraint
from mppsteel.utility.dataframe_utility import return_furnace_group
from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    INVESTMENT_OFFCYCLE_BUFFER_TOP,
    INVESTMENT_OFFCYCLE_BUFFER_TAIL,
    TECH_MORATORIUM_DATE
)
from mppsteel.config.model_scenarios import TECH_SWITCH_SCENARIOS, SOLVER_LOGICS
from mppsteel.config.reference_lists import (
    SWITCH_DICT,
    TECH_REFERENCE_LIST,
    TECHNOLOGY_PHASES,
    FURNACE_GROUP_DICT,
)
from mppsteel.data_preprocessing.tco_calculation_functions import (
    calculate_green_premium,
)
from mppsteel.model_solver.tco_and_abatement_optimizer import get_best_choice
from mppsteel.plant_classes.plant_choices_class import PlantChoices
from mppsteel.model_solver.material_usage_class import (
    MaterialUsage,
    create_material_usage_dict,
)
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def return_best_tech(
    tco_reference_data: pd.DataFrame,
    abatement_reference_data: pd.DataFrame,
    business_case_ref: dict,
    variable_costs_df: pd.DataFrame,
    green_premium_timeseries: pd.DataFrame,
    tech_availability: pd.DataFrame,
    tech_avail_from_dict: dict,
    plant_capacities: dict,
    scenario_dict: dict,
    investment_container: PlantInvestmentCycle,
    plant_choice_container: PlantChoices,
    capacity_constraint_container: PlantCapacityConstraint,
    year: int,
    plant_name: str,
    region: str,
    country_code: str,
    base_tech: str = None,
    transitional_switch_mode: bool = False,
    material_usage_dict_container: MaterialUsage = None,
) -> str:
    """Function generates the best technology choice from a number of key data and scenario inputs.

    Args:
        tco_reference_data (pd.DataFrame): DataFrame containing all TCO components by plant, technology and year.
        abatement_reference_data (pd.DataFrame): DataFrame containing all Emissions Abatement components by plant, technology and year.
        business_case_ref (dict): Standardised Business Cases.
        variable_costs_df (pd.DataFrame): Variable Costs DataFrame.
        green_premium_timeseries (pd.DataFrame): The timeseries containing the green premium values.
        tech_availability (pd.DataFrame): Technology Availability DataFrame
        tech_avail_from_dict (dict): A condensed version of the technology availability DataFrame as a dictionary of technology as key, availability year as value.
        plant_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        scenario_dict (dict): Scenario dictionary containing the model run's scenario settings.
        investment_container (PlantInvestmentCycle): The PlantInvestmentCycle Instance containing each plant's investment cycle.
        plant_choice_container (PlantChoices): The PlantChoices Instance containing each plant's choices.
        year (int): The current model year to get the best technology for.
        plant_name (str): The plant name.
        region (str): The plant's region.
        country_code (str): The country code related to the plant.
        base_tech (str, optional): The current base technology. Defaults to None.
        transitional_switch_mode (bool, optional): Boolean flag that determines if transitional switch logic is active. Defaults to False.
        material_usage_dict_container (dict, optional): Dictionary container object that is used to track the material usage within the application. Defaults to None.

    Raises:
        ValueError: If there is no base technology selected, a ValueError is raised because this provides the foundation for choosing a switch technology.

    Returns:
        str: Returns the best technology as a string.
    """
    proportions_dict = TECH_SWITCH_SCENARIOS[scenario_dict["tech_switch_scenario"]]
    solver_logic = SOLVER_LOGICS[scenario_dict["solver_logic"]]
    tech_moratorium = scenario_dict["tech_moratorium"]
    enforce_constraints = scenario_dict["enforce_constraints"]
    green_premium_scenario = scenario_dict["green_premium_scenario"]
    scenario_name = scenario_dict["scenario_name"]
    regional_scrap = scenario_dict["regional_scrap_constraint"]

    tco_ref_data = tco_reference_data.copy()

    if green_premium_scenario != "off":
        usd_to_eur_rate = scenario_dict["usd_to_eur"]
        discounted_green_premium_values = calculate_green_premium(
            variable_costs_df,
            plant_capacities,
            green_premium_timeseries,
            country_code,
            plant_name,
            year,
            usd_to_eur_rate,
        )
        for technology in TECH_REFERENCE_LIST:
            current_tco_value = tco_ref_data.loc[year, country_code, technology]["tco"]
            tco_ref_data.loc[(year, country_code, technology), "tco"] = (
                current_tco_value - discounted_green_premium_values[technology]
            )

    if not base_tech:
        raise ValueError(
            f"Issue with base_tech not existing: {plant_name} | {year} | {base_tech}"
        )

    if not isinstance(base_tech, str):
        raise ValueError(
            f"Issue with base_tech not being a string: {plant_name} | {year} | {base_tech}"
        )

    # Valid Switches
    combined_available_list = [
        tech for tech in SWITCH_DICT if tech in SWITCH_DICT[base_tech]
    ]

    # Transitional switches
    if transitional_switch_mode and (base_tech not in TECHNOLOGY_PHASES["end_state"]):
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

    if transitional_switch_mode:
        cycle_length = investment_container.return_cycle_lengths(plant_name)
        # Adjust tco values based on transistional switch years
        tco_ref_data["tco_gf_capex"] = (
            tco_ref_data["tco_gf_capex"]
            * cycle_length
            / (
                cycle_length
                - (INVESTMENT_OFFCYCLE_BUFFER_TOP + INVESTMENT_OFFCYCLE_BUFFER_TAIL)
            )
        )

    best_choice = get_best_choice(
        tco_ref_data,
        abatement_reference_data,
        country_code,
        year,
        base_tech,
        solver_logic,
        scenario_name,
        proportions_dict,
        combined_available_list,
        transitional_switch_mode,
        regional_scrap,
        plant_choice_container,
        enforce_constraints,
        business_case_ref,
        plant_capacities,
        material_usage_dict_container,
        plant_name,
        region,
    )

    if not isinstance(best_choice, str):
        raise ValueError(
            f"Issue with get_best_choice function returning a nan: {plant_name} | {year} | {base_tech} | {combined_available_list}"
        )

    switch_type = "Trans Switch" if transitional_switch_mode else "Main Switch"

    capacity_constraint_container.update_potential_plant_switcher(
        year, plant_name, plant_capacities[plant_name], switch_type
    )

    if best_choice != base_tech:
        capacity_transaction_result = capacity_constraint_container.subtract_capacity_from_balance(
            year, plant_name
        )
        if not capacity_transaction_result:
            best_choice = base_tech

    else:
        capacity_constraint_container.remove_plant_from_waiting_list(year, plant_name)

    if enforce_constraints:
        create_material_usage_dict(
            material_usage_dict_container,
            plant_capacities,
            business_case_ref,
            plant_name,
            region,
            year,
            best_choice,
            regional_scrap=regional_scrap,
            override_constraint=True,
            apply_transaction=True
        )

    return best_choice


def active_check_results(
    steel_plant_df: pd.DataFrame, year_range: range, inverse: bool = False
) -> dict:
    """Checks whether each plant in `steel_plant_df` is active for each year in `year_range`.

    Args:
        steel_plant_df (pd.DataFrame): The Steel Plant DataFrame.
        year_range (range): The year range used run each plant check for.
        inverse (bool, optional): Boolean that determines whether the reverse the order of the dictionary. Defaults to False.

    Returns:
        dict: A dictionary with the plant names as keys and the boolean active check values as values. Or inversed if `inverse` is set to True.
    """

    def final_active_checker(row: pd.Series, year: int) -> bool:
        if year < row.start_of_operation:
            return False
        if row.end_of_operation and year >= row.end_of_operation:
            return False
        return True

    active_check = {}
    if inverse:
        for year in year_range:
            active_check[year] = {}
            for row in steel_plant_df.itertuples():
                active_check[year][row.plant_name] = final_active_checker(row, year)
        return active_check
    else:
        for row in steel_plant_df.itertuples():
            active_check[row.plant_name] = {}
            for year in year_range:
                active_check[row.plant_name][year] = final_active_checker(row, year)
        return active_check



def resort_primary_switchers(primary_switchers_df: pd.DataFrame, waiting_list_dict: dict) -> pd.DataFrame:
    waiting_list_plants = waiting_list_dict.keys()
    just_waiting_list_plant_df = primary_switchers_df[primary_switchers_df["plant_name"].isin(waiting_list_plants)]
    not_waiting_list_plant_df = primary_switchers_df[~primary_switchers_df["plant_name"].isin(waiting_list_plants)]
    return pd.concat([just_waiting_list_plant_df, not_waiting_list_plant_df]).reset_index(drop=True)


def get_current_technology(
    PlantChoiceContainer: PlantChoices,
    year: int,
    plant_name: str,
    year_founded: int,
    initial_technology: str
) -> str:
    current_tech = ""
    if (year == MODEL_YEAR_START) or (year == year_founded):
        current_tech = initial_technology
    else:
        current_tech = PlantChoiceContainer.get_choice(year - 1, plant_name)
    return current_tech


def create_solver_entry_dict(
    PlantChoiceContainer: PlantChoices, year: int, plant_name: str, current_tech: str,
    switch_tech: str, switch_type: str, update_record: bool = True, update_choice: bool = True
) -> dict:
    entry = {
        "year": year,
        "plant_name": plant_name,
        "current_tech": current_tech,
        "switch_tech": switch_tech,
        "switch_type": switch_type
    }
    if update_record:
        PlantChoiceContainer.update_records("choice", entry)
    if update_choice:
        PlantChoiceContainer.update_choice(year, plant_name, switch_tech)
    return entry

def return_initial_tech(initial_tech_ref: dict, plant_name: str) -> str:
    return initial_tech_ref[plant_name]

def split_primary_plant_switchers(
    primary_switchers_df: pd.DataFrame,
    PlantInvestmentCycleContainer: PlantInvestmentCycle,
    PlantChoiceContainer: PlantChoices,
    year: int
) -> Tuple[dict, dict, dict, dict]:
    closed_plants_current_techs = {}
    new_open_plants = {}
    main_cycle_plants = {}
    trans_switch_plants = {}
    for row in primary_switchers_df.itertuples():
        year_founded = PlantInvestmentCycleContainer.plant_start_years[row.plant_name]
        switch_type = PlantInvestmentCycleContainer.return_plant_switch_type(
            row.plant_name, year
        )
        current_tech = get_current_technology(
            PlantChoiceContainer, year, row.plant_name, year_founded, row.initial_technology
        )
        if current_tech == "Close plant":
            closed_plants_current_techs[row.plant_name] = current_tech
        elif (year == year_founded) and (row.status == "new model plant"):
            new_open_plants[row.plant_name] = current_tech
        elif switch_type == "main cycle":
            main_cycle_plants[row.plant_name] = {
                "current_tech": current_tech,
                "country_code": row.country_code,
                "region": row.rmi_region
            }
        elif switch_type == "trans switch":
            trans_switch_plants[row.plant_name] = {
                "current_tech": current_tech,
                "country_code": row.country_code,
                "region": row.rmi_region
            }
    return closed_plants_current_techs, new_open_plants, main_cycle_plants, trans_switch_plants

def map_technology_state(tech: str) -> str:
    """Returns the technology phase according to a technology phases dictionary.

    Args:
        tech (str): The technology you want to return the technology phase for.

    Returns:
        str: The technology phase of `tech`.
    """
    for tech_state in TECHNOLOGY_PHASES.keys():
        if tech in TECHNOLOGY_PHASES[tech_state]:
            return tech_state


def read_and_format_tech_availability(df: pd.DataFrame) -> pd.DataFrame:
    """Formats the technology availability DataFrame.

    Args:
        df (pd.DataFrame): A Technology availability DataFrame.

    Returns:
        pd.DataFrame: A formatted technology availability DataFrame.
    """
    df_c = df.copy()
    df_c.columns = [col.lower().replace(" ", "_") for col in df_c.columns]
    df_c = df_c[
        ~df_c["technology"].isin(
            ["Close plant", "Charcoal mini furnace", "New capacity"]
        )
    ]
    df_c["technology_phase"] = df_c["technology"].apply(
        lambda x: map_technology_state(x)
    )
    col_order = [
        "technology",
        "main_technology_type",
        "technology_phase",
        "year_available_from",
        "year_available_until",
    ]
    return df_c[col_order].set_index("technology")


def tech_availability_check(
    tech_df: pd.DataFrame,
    technology: str,
    year: int,
    tech_moratorium: bool = False,
    default_year_unavailable: int = 2200,
) -> bool:
    """Checks whether a technology is available in a given year.

    Args:
        tech_df (pd.DataFrame): The technology availability DataFrame.
        technology (str): The technology to check availability for.
        year (int): The year to check whether a specified `technology` is available or not.
        tech_moratorium (bool, optional): Boolean flag that determines whether a specified technology is available or not. Defaults to False.
        default_year_unavailable (int): Determines the default year a given technology will not be available from - will be altered according to function logic. Defaults to 2200.

    Returns:
        bool: A boolean that determines whether a specified `technology` is available in the specified `year`.
    """
    row = tech_df.loc[technology]
    year_available_from = row.loc["year_available_from"]
    technology_phase = row.loc["technology_phase"]
    year_available_until = default_year_unavailable

    if tech_moratorium and (technology_phase in ["initial", "transitional"]):
        year_available_until = TECH_MORATORIUM_DATE
    if int(year_available_from) <= int(year) < int(year_available_until):
        # Will be available
        return True
    if int(year) <= int(year_available_from):
        # Will not be ready yet
        return False
    if int(year) > int(year_available_until):
        # Will become unavailable
        return False
