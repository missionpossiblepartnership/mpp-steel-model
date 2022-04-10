"""Main solving script for deciding investment decisions."""

import pandas as pd
from tqdm import tqdm

from typing import Union

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.plant_container_class import PlantIdContainer
from mppsteel.utility.dataframe_utility import return_furnace_group
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS,
    MAIN_REGIONAL_SCHEMA,
    INVESTMENT_CYCLE_DURATION_YEARS,
    INVESTMENT_OFFCYCLE_BUFFER_TOP,
    INVESTMENT_OFFCYCLE_BUFFER_TAIL,
)

from mppsteel.config.model_scenarios import (
    TECH_SWITCH_SCENARIOS, SOLVER_LOGICS, STEEL_DEMAND_SCENARIO_MAPPER
)

from mppsteel.config.reference_lists import (
    SWITCH_DICT,
    TECHNOLOGY_STATES,
    FURNACE_GROUP_DICT,
    RESOURCE_CONTAINER_REF,
)

from mppsteel.data_loading.data_interface import load_business_cases
from mppsteel.data_loading.country_reference import country_df_formatter
from mppsteel.model.solver_constraints import (
    tech_availability_check,
    read_and_format_tech_availability,
    return_current_usage
)
from mppsteel.model.tco_and_abatement_optimizer import get_best_choice, subset_presolver_df
from mppsteel.model.solver_classes import (
    CapacityContainerClass, UtilizationContainerClass,
    PlantChoices, MarketContainerClass, MaterialUsage,
    create_wsa_2020_utilization_dict, apply_constraints
)
from mppsteel.model.plant_open_close import (
    open_close_flow, return_modified_plants
)
from mppsteel.model.levelized_cost import generate_levelized_cost_results
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def return_best_tech(
    tco_reference_data: pd.DataFrame,
    abatement_reference_data: pd.DataFrame,
    solver_logic: str,
    proportions_dict: dict,
    business_cases: pd.DataFrame,
    tech_availability: pd.DataFrame,
    tech_avail_from_dict: dict,
    plant_capacities: dict,
    year: int,
    plant_name: str,
    country_code: str,
    base_tech: str = None,
    tech_moratorium: bool = False,
    transitional_switch_only: bool = False,
    enforce_constraints: bool = False,
    material_usage_dict_container: MaterialUsage = None,
) -> Union[str, dict]:
    """Function generates the best technology choice from a number of key data and scenario inputs.

    Args:
        tco_reference_data (pd.DataFrame): DataFrame containing all TCO components by plant, technology and year.
        abatement_reference_data (pd.DataFrame): DataFrame containing all Emissions Abatement components by plant, technology and year.
        solver_logic (str): Scenario setting that decides the logic used to choose the best technology `scale`, `ranke` or `bins`.
        proportions_dict (dict): Scenario seeting that decides the weighting given to TCO or Emissions Abatement in the technology selector part of the solver logic.
        business_cases (pd.DataFrame): Standardised Business Cases.
        tech_availability (pd.DataFrame): Technology Availability DataFrame
        tech_avail_from_dict (dict): _description_
        plant_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        year (int): The current model year to get the best technology for.
        plant_name (str): The plant name.
        country_code (str): The country code related to the plant.
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
        combined_available_list = apply_constraints(
            business_cases,
            plant_capacities,
            material_usage_dict_container,
            combined_available_list,
            year,
            plant_name,
            base_tech,   
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

    return best_choice


def choose_technology(
    scenario_dict: dict
) -> dict:
    """Function containing the entire solver decision logic flow.
    1) In each year, the solver splits the plants into technology switchers, and non-switchers.
    2) The solver extracts the prior year technology of the non-switchers and assumes this is the current technology of the siwtchers.
    3) All switching plants are then sent through the `return_best_tech` function that decides the best technology depending on the switch type (main cycle or transitional switch).
    4) All results are saved to a dictionary which is outputted at the end of the year loop.

    Args:
        scenario_dict (int): Model Scenario settings.
    Returns:
        dict: A dictionary containing the best technology resuls. Organised as year: plant: best tech.
    """

    logger.info("Creating Steel plant df")

    solver_logic = SOLVER_LOGICS[scenario_dict["solver_logic"]]
    tech_moratorium = scenario_dict["tech_moratorium"]
    enforce_constraints = scenario_dict["enforce_constraints"]
    steel_demand_scenario = scenario_dict["steel_demand_scenario"]
    trans_switch_scenario = scenario_dict["transitional_switch"]
    tech_switch_scenario = TECH_SWITCH_SCENARIOS[
        scenario_dict["tech_switch_scenario"]
    ]
    trade_scenario=scenario_dict["trade_active"]
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')

    original_plant_df = read_pickle_folder(PKL_DATA_FORMATTED, "steel_plants_processed", "df")
    PlantInvestmentCycleContainer = read_pickle_folder(
        PKL_DATA_FORMATTED, "plant_investment_cycle_container", "df"
    )
    variable_costs_regional = read_pickle_folder(
        intermediate_path, "variable_costs_regional", "df"
    )
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref", "df")
    rmi_mapper = create_country_mapper()
    country_ref_f = country_df_formatter(country_ref)
    bio_constraint_model = read_pickle_folder(
        intermediate_path, "bio_constraint_model_formatted", "df"
    )
    ccs_co2 = read_pickle_folder(PKL_DATA_IMPORTS, "ccs_co2", "df")
    steel_demand_df = read_pickle_folder(
        PKL_DATA_FORMATTED, "regional_steel_demand_formatted", "df"
    )
    steel_demand_df = steel_demand_df.loc[:,STEEL_DEMAND_SCENARIO_MAPPER[steel_demand_scenario],:].copy()
    tech_availability = read_pickle_folder(PKL_DATA_IMPORTS, "tech_availability", "df")
    ta_dict = dict(
        zip(tech_availability["Technology"], tech_availability["Year available from"])
    )
    tech_availability = read_and_format_tech_availability(tech_availability)
    capex_dict = read_pickle_folder(
        PKL_DATA_FORMATTED, "capex_dict", "dict"
    )
    business_cases = load_business_cases()
    tco_summary_data = read_pickle_folder(
        intermediate_path, "tco_summary_data", "df"
    )
    tco_slim = subset_presolver_df(tco_summary_data, subset_type='tco_summary')
    levelized_cost = read_pickle_folder(intermediate_path, "levelized_cost", "df")
    levelized_cost["region"] = levelized_cost["country_code"].apply(lambda x: rmi_mapper[x])
    steel_plant_abatement_switches = read_pickle_folder(
        intermediate_path, "emissivity_abatement_switches", "df"
    )
    abatement_slim = subset_presolver_df(steel_plant_abatement_switches, subset_type='abatement')

    # Year range
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)

    # Initialize plant container
    PlantIDC = PlantIdContainer()
    PlantIDC.add_steel_plant_ids(original_plant_df)
    model_plant_df = original_plant_df.copy()

    # Instantiate Trade Container
    market_container = MarketContainerClass()
    region_list = model_plant_df[MAIN_REGIONAL_SCHEMA].unique()
    market_container.full_instantiation(year_range, region_list)

    # Utilization & Capacity Containers
    UtilizationContainer = UtilizationContainerClass()
    wsa_dict = create_wsa_2020_utilization_dict()
    region_list = list(wsa_dict.keys())
    UtilizationContainer.initiate_container(year_range=year_range, region_list=region_list)
    CapacityContainer = CapacityContainerClass()
    CapacityContainer.instantiate_container(year_range)

    # Initialize the Material Usage container
    MaterialUsageContainer = MaterialUsage()
    material_models = {
        'biomass': bio_constraint_model,
        'scrap': steel_demand_df,
        'co2': ccs_co2,
        'ccs': ccs_co2
    }
    for material in material_models:
        MaterialUsageContainer.load_constraint(material_models[material], material)

    # Plant Choices
    PlantChoiceContainer = PlantChoices()
    PlantChoiceContainer.initiate_container(year_range)

    

    # Investment Cycles
    for year in tqdm(year_range, total=len(year_range), desc="Years"):
        for material in material_models:
            MaterialUsageContainer.set_year_balance(material, year)

        logger.info(f"Running investment decisions for {year}")
        if year == 2020:
            logger.info(f'Loading initial technology choices for {year}')
            for row in model_plant_df.itertuples():
                PlantChoiceContainer.update_choices(year, row.plant_name, row.technology_in_2020)
            CapacityContainer.map_capacities(original_plant_df, model_plant_df, year)
            UtilizationContainer.assign_year_utilization(2020, wsa_dict)
        else:
            logger.info(f'Loading plant entries for {year}')
            for row in model_plant_df.itertuples():
                PlantChoiceContainer.update_choices(year, row.plant_name, '')

        CapacityContainer.map_capacities(original_plant_df, model_plant_df, year)
        logger.info(f'Starting the open close flow for {year}')
        investment_dict = PlantInvestmentCycleContainer.return_investment_dict()

        model_plant_df = open_close_flow(
            plant_container=PlantIDC,
            market_container=market_container,
            plant_df=model_plant_df,
            levelized_cost=levelized_cost,
            steel_demand_df=steel_demand_df,
            country_df=country_ref_f,
            business_cases=business_cases,
            variable_costs_df=variable_costs_regional,
            capex_dict=capex_dict,
            tech_choices_container=PlantChoiceContainer,
            investment_dict=investment_dict,
            capacity_container=CapacityContainer,
            utilization_container=UtilizationContainer,
            material_container=MaterialUsageContainer,
            year=year,
            trade_scenario=trade_scenario,
            steel_demand_scenario=steel_demand_scenario
        )

        all_plant_names = model_plant_df["plant_name"].copy()
        CapacityContainer.map_capacities(original_plant_df, model_plant_df, year)
        plant_capacities_dict = CapacityContainer.return_plant_capacity(year=year)
        logger.info(f'Creating investment cycle for new plants')
        new_open_plants = return_modified_plants(model_plant_df, year, 'open')
        PlantInvestmentCycleContainer.add_new_plants(
            new_open_plants['plant_name'], new_open_plants['start_of_operation']
        )
        switchers = PlantInvestmentCycleContainer.return_plant_switchers(year, 'combined')
        non_switchers = list(set(all_plant_names).difference(switchers))

        switchers_df = (
            model_plant_df.set_index(["plant_name"]).drop(non_switchers).reset_index()
        )
        switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        non_switchers_df = (
            model_plant_df.set_index(["plant_name"]).drop(switchers).reset_index()
        )
        non_switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        logger.info(f"-- Running investment decisions for Non Switching Plants")
        for plant_name in non_switchers:
            current_tech = ''
            year_founded = PlantInvestmentCycleContainer.plant_start_years[plant_name]
            if (year == 2020) or (year == year_founded):
                tech_in_2020 = non_switchers_df[
                    non_switchers_df["plant_name"] == plant_name
                ]["technology_in_2020"].values[0]
                current_tech = tech_in_2020
                PlantChoiceContainer.update_choices(year, plant_name, tech_in_2020)
            else:
                current_tech = PlantChoiceContainer.get_choice(year - 1, plant_name)
                PlantChoiceContainer.update_choices(year, plant_name, current_tech)
            
            entry = {
                'year': year,
                'plant_name': plant_name,
                'current_tech': current_tech,
                'switch_tech': current_tech,
                'switch_type': 'not a switch year'
            }
            PlantChoiceContainer.update_records(entry)

        for resource in RESOURCE_CONTAINER_REF:
            current_usage = return_current_usage(
                non_switchers_df["plant_name"].unique(),
                PlantChoiceContainer.return_choices(),
                plant_capacities_dict,
                business_cases,
                RESOURCE_CONTAINER_REF[resource],
                year
            )
            MaterialUsageContainer.constraint_transaction(
                resource, year, current_usage, override_constraint=True)
            
            # Get current balance after non_switchers are allocated resources
            # print(MaterialUsageContainer.get_current_balance(resource, year))

        logger.info(f"-- Running investment decisions for Switching Plants")
        for plant in switchers_df.itertuples():
            plant_name = plant.plant_name
            country_code = plant.country_code
            year_founded = plant.start_of_operation
            current_tech = ''
            if (year == 2020) or (year == year_founded):
                tech_in_2020 = switchers_df[switchers_df["plant_name"] == plant_name][
                    "technology_in_2020"
                ].values[0]
                current_tech = tech_in_2020
            else:
                current_tech = PlantChoiceContainer.get_choice(year - 1, plant_name)
            entry = {'year': year, 'plant_name': plant_name, 'current_tech': current_tech}

            if (current_tech == "Not operating") or (current_tech == "Close plant"):
                PlantChoiceContainer.update_choices(year, plant_name, "Close plant") 
                entry['switch_tech'] = "Close plant"
                entry['switch_type'] = 'Plant was already closed'

            elif (tech_in_2020 == 'EAF') & (plant.primary == 'N'):
                entry['switch_tech'] = "EAF"
                entry['switch_type'] = 'Secondary capacity is always EAF'
                PlantChoiceContainer.update_choices(year, plant_name, 'EAF')

            else:
                switch_type = PlantInvestmentCycleContainer.return_plant_switch_type(plant_name, year)

                if switch_type == "main cycle":
                    best_choice_tech = return_best_tech(
                        tco_slim,
                        abatement_slim,
                        solver_logic,
                        tech_switch_scenario,
                        business_cases,
                        tech_availability,
                        ta_dict,
                        plant_capacities_dict,
                        year,
                        plant_name,
                        country_code,
                        current_tech,
                        tech_moratorium=tech_moratorium,
                        enforce_constraints=enforce_constraints,
                        material_usage_dict_container=MaterialUsageContainer,
                    )
                    if best_choice_tech == current_tech:
                        entry['switch_type'] = 'No change in main investment cycle year'
                    else:
                        entry['switch_type'] = 'Regular change in investment cycle year'
                    PlantChoiceContainer.update_choices(year, plant_name, best_choice_tech)
                if switch_type == "trans switch":
                    best_choice_tech = return_best_tech(
                        tco_slim,
                        abatement_slim,
                        solver_logic,
                        tech_switch_scenario,
                        business_cases,
                        tech_availability,
                        ta_dict,
                        plant_capacities_dict,
                        year,
                        plant_name,
                        country_code,
                        current_tech,
                        tech_moratorium=tech_moratorium,
                        enforce_constraints=enforce_constraints,
                        material_usage_dict_container=MaterialUsageContainer,
                        transitional_switch_only=trans_switch_scenario,
                    )
                    if best_choice_tech != current_tech:
                        entry['switch_type'] = 'Transitional switch in off-cycle investment year'
                        PlantInvestmentCycleContainer.adjust_cycle_for_transitional_switch(plant_name, year)
                    else:
                        entry['switch_type'] = 'No change during off-cycle investment year'
                    PlantChoiceContainer.update_choices(year, plant_name, best_choice_tech)
                entry['switch_tech'] = best_choice_tech
            PlantChoiceContainer.update_records(entry)
    
    trade_summary_results = market_container.output_trade_summary_to_df()
    full_trade_calculations = market_container.output_trade_calculations_to_df()
    material_usage_results = MaterialUsageContainer.output_results_to_df()
    investment_dict = PlantInvestmentCycleContainer.return_investment_dict()
    plant_cycle_length_mapper = PlantInvestmentCycleContainer.return_cycle_lengths()
    investment_df = PlantInvestmentCycleContainer.create_investment_df()
    tech_choice_dict = PlantChoiceContainer.return_choices()
    tech_choice_records = PlantChoiceContainer.output_records_to_df()
    capacity_results = CapacityContainer.return_regional_capacity()
    utilization_results = UtilizationContainer.get_utilization_values()

    return {
        'tech_choice_dict': tech_choice_dict,
        'tech_choice_records': tech_choice_records,
        'plant_result_df': model_plant_df,
        'investment_cycle_ref_result': investment_df,
        'investment_dict_result': investment_dict,
        'plant_cycle_length_mapper_result': plant_cycle_length_mapper,
        'capacity_results': capacity_results,
        'utilization_results': utilization_results,
        'trade_summary_results': trade_summary_results,
        'full_trade_calculations': full_trade_calculations,
        'material_usage_results': material_usage_results
        }

@timer_func
def solver_flow(scenario_dict: dict, serialize: bool = False) -> dict:
    """Initiates the complete solver flow and serializes the outputs. Tracks all technology choices and plant changes.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        dict: A dictionary containing the best technology results and the resultant steel plants. tech_choice_dict is organised as year: plant: best tech.
    """
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    results_dict = choose_technology(scenario_dict=scenario_dict)
    levelized_cost_updated = generate_levelized_cost_results(
        scenario_dict=scenario_dict, steel_plant_df=results_dict['plant_result_df']
        )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(results_dict['tech_choice_dict'], intermediate_path, "tech_choice_dict")
        serialize_file(results_dict['tech_choice_records'], intermediate_path, "tech_choice_records")
        serialize_file(results_dict['plant_result_df'], intermediate_path, "plant_result_df")
        serialize_file(results_dict['investment_cycle_ref_result'], intermediate_path, "investment_cycle_ref_result")
        serialize_file(results_dict['investment_dict_result'], intermediate_path, "investment_dict_result")
        serialize_file(results_dict['plant_cycle_length_mapper_result'], intermediate_path, "plant_cycle_length_mapper_result")
        serialize_file(results_dict['capacity_results'], intermediate_path, "capacity_results")
        serialize_file(results_dict['utilization_results'], intermediate_path, "utilization_results")
        serialize_file(results_dict['trade_summary_results'], intermediate_path, "trade_summary_results")
        serialize_file(results_dict['full_trade_calculations'], intermediate_path, "full_trade_calculations")
        serialize_file(results_dict['material_usage_results'], intermediate_path, "material_usage_results")
        serialize_file(levelized_cost_updated, intermediate_path, 'levelized_cost_updated')
    return results_dict
