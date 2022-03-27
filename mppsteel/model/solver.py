"""Main solving script for deciding investment decisions."""
from copy import deepcopy
from typing import Union
from typing import Tuple, Union

import pandas as pd
from tqdm import tqdm

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.plant_container_class import PlantIdContainer
from mppsteel.utility.dataframe_utility import return_furnace_group
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.data_loading.reg_steel_demand_formatter import extend_steel_demand
from mppsteel.model.investment_cycles import create_investment_cycle, amend_investment_dict, create_investment_cycle_reference
from mppsteel.data_loading.country_reference import country_df_formatter

from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_IMPORTS,
    PKL_DATA_INTERMEDIATE,
    INVESTMENT_CYCLE_DURATION_YEARS,
    INVESTMENT_OFFCYCLE_BUFFER_TOP,
    INVESTMENT_OFFCYCLE_BUFFER_TAIL
)

from mppsteel.config.model_scenarios import TECH_SWITCH_SCENARIOS, SOLVER_LOGICS

from mppsteel.config.reference_lists import (
    SWITCH_DICT,
    TECHNOLOGY_STATES,
    FURNACE_GROUP_DICT,
    TECH_MATERIAL_CHECK_DICT,
    RESOURCE_CONTAINER_REF,
    TECHNOLOGY_PHASES,
)

from mppsteel.data_loading.data_interface import load_materials, load_business_cases
from mppsteel.data_loading.steel_plant_formatter import create_plant_capacities_dict
from mppsteel.model.solver_constraints import (
    tech_availability_check,
    read_and_format_tech_availability,
    plant_tech_resource_checker,
    material_usage_per_plant,
    load_resource_usage_dict,
)
from mppsteel.model.tco_and_abatement_optimizer import get_best_choice, subset_presolver_df
from mppsteel.model.plant_open_close import (
    open_close_flow, return_modified_plants, 
    create_wsa_2020_utilization_dict, create_plant_capacity_dict
)
from mppsteel.model.trade import TradeBalance
from mppsteel.model.levelized_cost import generate_levelized_cost_results
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.location_utility import get_region_from_country_code

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
    base_tech: str = None,
    tech_moratorium: bool = False,
    transitional_switch_only: bool = False,
    enforce_constraints: bool = False,
    material_usage_dict_container: dict = None,
    return_material_container: bool = True,
) -> Union[str, dict]:
    """Function generates the best technology choice from a number of key data and scenario inputs.

    Args:
        tco_reference_data (pd.DataFrame): DataFrame containing all TCO components by plant, technology and year.
        abatement_reference_data (pd.DataFrame): DataFrame containing all Emissions Abatement components by plant, technology and year.
        solver_logic (str): Scenario setting that decides the logic used to choose the best technology `scale`, `ranke` or `bins`.
        proportions_dict (dict): Scenario seeting that decides the weighting given to TCO or Emissions Abatement in the technology selector part of the solver logic.
        steel_demand_df (pd.DataFrame): A Steel Demand Timeseries. 
        steel_plant_df (pd.DataFrame): A Steel Plant DataFrame.
        business_cases (pd.DataFrame): Standardised Business Cases.
        biomass_df (pd.DataFrame): The Shared Assumptions Biomass Constraints DataFrame.
        ccs_co2_df (pd.DataFrame): CCS / CO2 Constraints DataFrame
        tech_availability (pd.DataFrame): Technology Availability DataFrame
        tech_avail_from_dict (dict): _description_
        plant_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        materials_list (list): List of materials to track the usage for.
        year (int): The current model year to get the best technology for.
        plant_name (str): The plant name.
        country_code (str): The country code related to the plant.
        steel_demand_scenario (str): Scenario that determines the Steel Demand Timeseries. Defaults to "bau".
        base_tech (str, optional): The current base technology. Defaults to None.
        tech_moratorium (bool, optional): Scenario setting that determines if the tech moratorium should be active. Defaults to False.
        transitional_switch_only (bool, optional): Scenario setting that determines if transitional switches are allowed. Defaults to False.
        enforce_constraints (bool, optional): Scenario setting that determines if constraints are enforced within the model. Defaults to False.
        material_usage_dict_container (dict, optional): Dictionary container object that is used to track the material usage within the application. Defaults to None.
        return_material_container (bool, optional): Boolean switch that enables the `material_usage_dict_container` to be reused. Defaults to True.

    Raises:
        ValueError: If there is no base technology selected, a ValueError is raised because this provides the foundation for choosing a switch technology.

    Returns:
        Union[str, dict]: Returns the best technology as a string, and optionally the `material_usage_dict_container` if the `return_material_container` switch is activated.
    """
    tco_ref_data = tco_reference_data.copy()

    if not base_tech:
        raise ValueError(f'Issue with base_tech not existing: {plant_name} | {year} | {base_tech}')

    if not isinstance(base_tech, str):
        raise ValueError(f'Issue with base_tech not being a string: {plant_name} | {year} | {base_tech}')

    # Valid Switches
    combined_available_list = [
        key for key in SWITCH_DICT if key in SWITCH_DICT[base_tech]
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

    if transitional_switch_only:
        # Adjust tco values based on transistional switch years
        tco_ref_data['tco'] = tco_ref_data['tco'] * INVESTMENT_CYCLE_DURATION_YEARS / (
            INVESTMENT_CYCLE_DURATION_YEARS - (INVESTMENT_OFFCYCLE_BUFFER_TOP + INVESTMENT_OFFCYCLE_BUFFER_TAIL))

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
        tco_ref_data,
        abatement_reference_data,
        country_code,
        plant_name,
        year,
        base_tech,
        solver_logic,
        proportions_dict,
        combined_available_list,
    )

    if not isinstance(best_choice, str):
        raise ValueError(f'Issue with get_best_choice function returning a nan: {plant_name} | {year} | {combined_available_list}')

    if return_material_container:
        return best_choice, material_usage_dict_container
    return best_choice

def create_investment_cycle_ref_from_dict(inv_dict: dict, year_end: int):
    return create_investment_cycle_reference(
        list(inv_dict.keys()), 
        list(inv_dict.values()),
        year_end
    )

def choose_technology(
    year_end: int,
    solver_logic: str,
    tech_moratorium: bool = False,
    enforce_constraints: bool = True,
    steel_demand_scenario: str = "bau",
    trans_switch_scenario: str = True,
    tech_switch_scenario: dict = {"tco": 1, "emissions": 0},
    trade_scenario: bool = False
) -> dict:
    """Function containing the entire solver decision logic flow.
    1) In each year, the solver splits the plants into technology switchers, and non-switchers.
    2) The solver extracts the prior year technology of the non-switchers and assumes this is the current technology of the siwtchers.
    3) All switching plants are then sent through the `return_best_tech` function that decides the best technology depending on the switch type (main cycle or transitional switch).
    4) All results are saved to a dictionary which is outputted at the end of the year loop.

    Args:
        year_end (int): The last model run year.
        solver_logic (str): Scenario setting that decides the logic used to choose the best technology `scaled`, `ranked` or `bins`. 
        tech_moratorium (bool, optional): Scenario setting that determines if the tech moratorium should be active. Defaults to False.
        enforce_constraints (bool, optional): Scenario setting that determines if constraints are enforced within the model. Defaults to False.
        steel_demand_scenario (str): Scenario that determines the Steel Demand Timeseries. Defaults to "bau".
        trans_switch_scenario (bool, optional): Scenario setting that determines if trasnitional switches are allowed. Defaults to False.
        tech_switch_scenario (_type_, optional): _description_. Defaults to {"tco": 0.6, "emissions": 0.4}.

    Returns:
        dict: A dictionary containing the best technology resuls. Organised as year: plant: best tech.
    """

    logger.info("Creating Steel plant df")

    plant_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df")
    investment_year_ref = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "plant_investment_cycles", "df"
    )
    investment_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "investment_dict", "dict"
    )
    plant_cycle_length_mapper = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "plant_cycle_length_mapper", "dict"
    )
    variable_costs_regional = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "variable_costs_regional", "df"
    )
    country_reference_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "country_reference_dict", "df")
    country_df = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref")
    country_df_f = country_df_formatter(country_df)
    investment_year_ref_c = investment_year_ref.copy()
    investment_dict_c = deepcopy(investment_dict)
    plant_cycle_length_mapper_c = deepcopy(plant_cycle_length_mapper)
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
    capex_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "capex_dict", "dict"
    )
    # TCO & Abatement Data
    tco_summary_data = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "tco_summary_data", "df"
    )
    tco_slim = subset_presolver_df(tco_summary_data, subset_type='tco_summary')
    levelized_cost = read_pickle_folder(PKL_DATA_INTERMEDIATE, "levelized_cost", "df")
    levelized_cost["region"] = levelized_cost["country_code"].apply(
            lambda x: get_region_from_country_code(x, "rmi_region", country_reference_dict)
    )
    steel_plant_abatement_switches = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "emissivity_abatement_switches", "df"
    )
    abatement_slim = subset_presolver_df(steel_plant_abatement_switches, subset_type='abatement')

    # Initialize plant container
    PlantIDC = PlantIdContainer()
    model_plant_df = plant_df.copy()
    PlantIDC.add_steel_plant_ids(model_plant_df)

    # Instantiate Trade Container
    trade_container = TradeBalance()
    region_list = model_plant_df['rmi_region'].unique()
    trade_container.full_instantiation(MODEL_YEAR_START, MODEL_YEAR_END, region_list)
    
    # General Reference data
    business_cases = load_business_cases()

    year_range = range(MODEL_YEAR_START, year_end + 1)
    current_plant_choices = {}

    util_dict = create_wsa_2020_utilization_dict()
    util_dict_c = deepcopy(util_dict)

    capacity_results = {}

    for year in tqdm(year_range, total=len(year_range), desc="Years"):
        logger.info(f"Running investment decisions for {year}")
        if year == 2020:
            logger.info(f'Loading initial technology choices for {year}')
            current_plant_choices[str(year)] = {row.plant_name: row.technology_in_2020 for row in model_plant_df.itertuples()}
        else:
            logger.info(f'Loading plant entries for {year}')
            current_plant_choices[str(year)] = {row.plant_name: '' for row in model_plant_df.itertuples()}
        logger.info(f'Starting the open close flow for {year}')
        open_close_dict = open_close_flow(
            plant_container=PlantIDC,
            trade_container=trade_container,
            plant_df=model_plant_df,
            levelized_cost=levelized_cost,
            steel_demand_df=steel_demand_df,
            country_df=country_df_f,
            variable_costs_df=variable_costs_regional,
            capex_dict=capex_dict,
            tech_choice_dict=current_plant_choices,
            investment_dict=investment_dict_c,
            util_dict=util_dict_c,
            year=year,
            trade_scenario=trade_scenario,
            steel_demand_scenario=steel_demand_scenario
        )
        
        model_plant_df = open_close_dict['plant_df']
        current_plant_choices = open_close_dict['tech_choice_dict']
        util_dict_c = open_close_dict['util_dict']
        
        all_plant_names = model_plant_df["plant_name"].copy()
        plant_capacities_dict = create_plant_capacities_dict(model_plant_df)
        logger.info(f'Creating investment cycle for new plants')
        new_open_plants = return_modified_plants(model_plant_df, year, 'open')
        investment_dict_object = create_investment_cycle(new_open_plants)
        investment_dict_c = {**investment_dict_c, **investment_dict_object['investment_dict']}
        plant_cycle_length_mapper_c = {**plant_cycle_length_mapper_c, **investment_dict_object['plant_cycle_length_mapper']}
        investment_year_ref_c = create_investment_cycle_ref_from_dict(investment_dict_c, year_end)
        switchers = extract_tech_plant_switchers(investment_year_ref_c, year)
        non_switchers = list(set(all_plant_names).difference(switchers))

        switchers_df = (
            model_plant_df.set_index(["plant_name"]).drop(non_switchers).reset_index()
        )
        switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        non_switchers_df = (
            model_plant_df.set_index(["plant_name"]).drop(switchers).reset_index()
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
            model_plant_df,
            plant_capacities_dict,
            steel_demand_df,
            materials,
            year,
            steel_demand_scenario,
        )
        material_usage_dict = load_resource_usage_dict(yearly_usage)
        logger.info(f"-- Running investment decisions for Non Switching Plants")
        for plant_name in non_switchers:
            year_founded = non_switchers_df[non_switchers_df['plant_name'] == plant_name]['start_of_operation'].values[0]
            if (year == 2020) or (year == year_founded):
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
            year_founded = plant.start_of_operation

            if (year == 2020) or (year == year_founded):
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
                    investment_year_ref_c.reset_index()
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
                        model_plant_df,
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
                        model_plant_df,
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
                        investment_dict_c = amend_investment_dict(investment_dict_c, plant_name, year)
                    else:
                        # print(f'{plant_name} kept its current tech {current_tech} in transitional year {year}')
                        pass
                    current_plant_choices[str(year)][plant_name] = best_choice_tech
        capacity_results[str(year)] = create_plant_capacity_dict(model_plant_df, as_mt=True)
    
    trade_df = trade_container.output_trade_to_df()
    investment_year_ref_c = create_investment_cycle_ref_from_dict(investment_dict_c, year_end)
    return {
        'tech_choice_dict': current_plant_choices, 
        'plant_result_df': model_plant_df, 
        'investment_cycle_ref_result': investment_year_ref_c,
        'investment_dict_result': investment_dict_c,
        'plant_cycle_length_mapper_c': plant_cycle_length_mapper_c,
        'capacity_results': capacity_results,
        'trade_results': trade_df
        }


def extract_tech_plant_switchers(
    inv_cycle_ref: pd.DataFrame, year: int, combined_output: bool = True
) -> Union[list, Tuple[list, list]]:
    """Extracts the list of plants that are due for a main cycle switch or a transitional switch in a given year according to an investment cycle DataFrame.

    Args:
        inv_cycle_ref (pd.DataFrame): DataFrame containing the investment cycle reference for each plant.
        year (int): The year to extract the plant switchers for.
        combined_output (bool, optional): Boolean switch that determines whether to return a combined list of switching plants or a tuple of two lists. Defaults to True.

    Returns:
        Union[list, Tuple[list, list]]: Returns a single list of main cycle switchers and transitional switchers if `combined_output` if set to True, else a tuple of the two lists.
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
    """Initiates the complete solver flow and serializes the outputs. Tracks all technology choices and plant changes.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        year_end (int): The last year of the model run.
        serialize (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        dict: A dictionary containing the best technology results and the resultant steel plants. tech_choice_dict is organised as year: plant: best tech.
    """

    results_dict = choose_technology(
        year_end=year_end,
        solver_logic=SOLVER_LOGICS[scenario_dict["solver_logic"]],
        tech_moratorium=scenario_dict["tech_moratorium"],
        enforce_constraints=scenario_dict["enforce_constraints"],
        steel_demand_scenario=scenario_dict["steel_demand_scenario"],
        trans_switch_scenario=scenario_dict["transitional_switch"],
        tech_switch_scenario=TECH_SWITCH_SCENARIOS[
            scenario_dict["tech_switch_scenario"]
        ],
        trade_scenario=scenario_dict["trade_active"]
    )
    levelized_cost_updated = generate_levelized_cost_results(results_dict['plant_result_df'])

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(results_dict['tech_choice_dict'], PKL_DATA_INTERMEDIATE, "tech_choice_dict")
        serialize_file(results_dict['plant_result_df'], PKL_DATA_INTERMEDIATE, "plant_result_df")
        serialize_file(results_dict['investment_cycle_ref_result'], PKL_DATA_INTERMEDIATE, "investment_cycle_ref_result")
        serialize_file(results_dict['investment_dict_result'], PKL_DATA_INTERMEDIATE, "investment_dict_result")
        serialize_file(results_dict['capacity_results'], PKL_DATA_INTERMEDIATE, "capacity_results")
        serialize_file(results_dict['trade_results'], PKL_DATA_INTERMEDIATE, "trade_results")
        serialize_file(levelized_cost_updated, PKL_DATA_INTERMEDIATE, 'levelized_cost_updated')
    return results_dict
