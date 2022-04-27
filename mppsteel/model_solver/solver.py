"""Main solving script for deciding investment decisions."""

import pandas as pd
from tqdm import tqdm

from typing import Union

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.plant_container_class import PlantIdContainer
from mppsteel.data_preprocessing.investment_cycles import PlantInvestmentCycle
from mppsteel.utility.dataframe_utility import return_furnace_group
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.config.model_config import (
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS,
    MAIN_REGIONAL_SCHEMA,
    INVESTMENT_OFFCYCLE_BUFFER_TOP,
    INVESTMENT_OFFCYCLE_BUFFER_TAIL,
)

from mppsteel.config.model_scenarios import (
    TECH_SWITCH_SCENARIOS, SOLVER_LOGICS
)

from mppsteel.config.reference_lists import (
    SWITCH_DICT,
    TECH_REFERENCE_LIST,
    TECHNOLOGY_STATES,
    FURNACE_GROUP_DICT,
    RESOURCE_CONTAINER_REF,
)
from mppsteel.data_preprocessing.tco_calculation_functions import calculate_green_premium
from mppsteel.data_load_and_format.country_reference import country_df_formatter
from mppsteel.model_solver.solver_constraints import (
    tech_availability_check,
    read_and_format_tech_availability,
    return_current_usage
)
from mppsteel.data_load_and_format.steel_plant_formatter import create_active_check_col
from mppsteel.model_solver.tco_and_abatement_optimizer import (
    get_best_choice, subset_presolver_df
)
from mppsteel.model_solver.solver_classes import (
    CapacityContainerClass, UtilizationContainerClass,
    PlantChoices, MarketContainerClass, MaterialUsage, create_material_usage_dict,
    create_wsa_2020_utilization_dict, apply_constraints
)
from mppsteel.model_solver.plant_open_close import (
    open_close_flow
)
from mppsteel.data_preprocessing.levelized_cost import generate_levelized_cost_results
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
    year: int,
    plant_name: str,
    country_code: str,
    base_tech: str = None,
    
    material_usage_dict_container: MaterialUsage = None,
) -> Union[str, dict]:
    """Function generates the best technology choice from a number of key data and scenario inputs.

    Args:
        tco_reference_data (pd.DataFrame): DataFrame containing all TCO components by plant, technology and year.
        abatement_reference_data (pd.DataFrame): DataFrame containing all Emissions Abatement components by plant, technology and year.
        solver_logic (str): Scenario setting that decides the logic used to choose the best technology `scale`, `ranke` or `bins`.
        proportions_dict (dict): Scenario seeting that decides the weighting given to TCO or Emissions Abatement in the technology selector part of the solver logic.
        business_case_ref (dict): Standardised Business Cases.
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
    proportions_dict = TECH_SWITCH_SCENARIOS[scenario_dict["tech_switch_scenario"]]
    solver_logic = SOLVER_LOGICS[scenario_dict['solver_logic']]
    transitional_switch_only = scenario_dict["transitional_switch"]
    tech_moratorium = scenario_dict["tech_moratorium"]
    enforce_constraints = scenario_dict["enforce_constraints"]
    green_premium_scenario = scenario_dict['green_premium_scenario']

    tco_ref_data = tco_reference_data.copy()

    if green_premium_scenario != 'off':
        usd_to_eur_rate = scenario_dict['usd_to_eur']
        discounted_green_premium_values = calculate_green_premium(
            variable_costs_df,
            plant_capacities,
            green_premium_timeseries,
            country_code,
            plant_name,
            year,
            usd_to_eur_rate
        )
        for technology in TECH_REFERENCE_LIST:
            current_tco_value = tco_ref_data.loc[year, country_code, technology]['tco']
            tco_ref_data.loc[(year, country_code, technology), 'tco'] = current_tco_value - discounted_green_premium_values[technology]

    if not base_tech:
        raise ValueError(f'Issue with base_tech not existing: {plant_name} | {year} | {base_tech}')

    if not isinstance(base_tech, str):
        raise ValueError(f'Issue with base_tech not being a string: {plant_name} | {year} | {base_tech}')

    # Valid Switches
    combined_available_list = [
        tech for tech in SWITCH_DICT if tech in SWITCH_DICT[base_tech]
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
        cycle_length = investment_container.return_cycle_lengths(plant_name)
        # Adjust tco values based on transistional switch years
        tco_ref_data['tco_gf_capex'] = tco_ref_data['tco_gf_capex'] * cycle_length / (
            cycle_length - (INVESTMENT_OFFCYCLE_BUFFER_TOP + INVESTMENT_OFFCYCLE_BUFFER_TAIL))

    if enforce_constraints:
        combined_available_list = apply_constraints(
            business_case_ref,
            plant_capacities,
            material_usage_dict_container,
            combined_available_list,
            year,
            plant_name,
            base_tech,
            override_constraint=False,
            apply_transaction=False
        )

    best_choice = get_best_choice(
        tco_ref_data,
        abatement_reference_data,
        country_code,
        year,
        base_tech,
        solver_logic,
        proportions_dict,
        combined_available_list,
        transitional_switch_only
    )

    if not isinstance(best_choice, str):
        raise ValueError(f'Issue with get_best_choice function returning a nan: {plant_name} | {year} | {base_tech} | {combined_available_list}')

    if enforce_constraints:
        material_usage_dict = create_material_usage_dict(
            material_usage_dict_container,
            plant_capacities,
            business_case_ref,
            plant_name,
            year,
            best_choice,
            override_constraint=False,
            apply_transaction=True
        )

    return best_choice


def active_check_results(steel_plant_df: pd.DataFrame, year_range: range, inverse: bool = False):

    def final_active_checker(row, year):
        if year < row.start_of_operation:
            return False
        if row.end_of_operation:
            if year >= row.end_of_operation:
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
    tech_moratorium = scenario_dict["tech_moratorium"]
    trade_scenario=scenario_dict["trade_active"]
    enforce_constraints = scenario_dict["enforce_constraints"]
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')

    original_plant_df = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
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
    co2_constraint = read_pickle_folder(PKL_DATA_IMPORTS, "ccs_co2", "df")
    ccs_constraint = read_pickle_folder(
        intermediate_path, "ccus_constraints_model_formatted", "df"
    )
    steel_demand_df = read_pickle_folder(
        intermediate_path, "regional_steel_demand_formatted", "df"
    )
    tech_availability = read_pickle_folder(PKL_DATA_IMPORTS, "tech_availability", "df")
    ta_dict = dict(
        zip(tech_availability["Technology"], tech_availability["Year available from"])
    )
    tech_availability = read_and_format_tech_availability(tech_availability)
    capex_dict = read_pickle_folder(
        PKL_DATA_FORMATTED, "capex_dict", "dict"
    )
    business_case_ref = read_pickle_folder(
        PKL_DATA_FORMATTED, "business_case_reference", "df"
    )
    green_premium_timeseries = read_pickle_folder(
        intermediate_path, "green_premium_timeseries", "df"
    ).set_index('year')
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

    # Initialize plant container
    PlantIDC = PlantIdContainer()
    PlantIDC.add_steel_plant_ids(original_plant_df)
    year_start_df = original_plant_df.copy()

    # Instantiate Trade Container
    market_container = MarketContainerClass()
    region_list = year_start_df[MAIN_REGIONAL_SCHEMA].unique()
    market_container.full_instantiation(MODEL_YEAR_RANGE, region_list)

    # Utilization & Capacity Containers
    UtilizationContainer = UtilizationContainerClass()
    wsa_dict = create_wsa_2020_utilization_dict()
    region_list = list(wsa_dict.keys())
    UtilizationContainer.initiate_container(year_range=MODEL_YEAR_RANGE, region_list=region_list)
    CapacityContainer = CapacityContainerClass()
    CapacityContainer.instantiate_container(MODEL_YEAR_RANGE)
    CapacityContainer.set_average_plant_capacity(original_plant_df)

    # Initialize the Material Usage container
    resource_models = {
        'biomass': bio_constraint_model,
        'scrap': steel_demand_df,
        'co2': co2_constraint,
        'ccs': ccs_constraint
    }
    MaterialUsageContainer = MaterialUsage()
    MaterialUsageContainer.initiate_years(MODEL_YEAR_RANGE, resource_list=resource_models.keys())

    for resource in resource_models:
        MaterialUsageContainer.load_constraint(resource_models[resource], resource)

    # Plant Choices
    PlantChoiceContainer = PlantChoices()
    PlantChoiceContainer.initiate_container(MODEL_YEAR_RANGE)
    # Investment Cycles
    for year in tqdm(MODEL_YEAR_RANGE, total=len(MODEL_YEAR_RANGE), desc="Years"):
        year_start_df['active_check'] = year_start_df.apply(create_active_check_col, year=year, axis=1)
        active_plant_df = year_start_df[year_start_df['active_check'] == True].copy()
        inactive_year_start_df = year_start_df[year_start_df['active_check'] == False].copy()
        CapacityContainer.map_capacities(active_plant_df, year)
        logger.info(f'Number of active (inactive) plants in {year}: {len(active_plant_df)} ({len(inactive_year_start_df)})')

        for resource in resource_models:
            MaterialUsageContainer.set_year_balance(year, resource)

        # Assign initial technologies for plants in the first year
        logger.info(f"Running investment decisions for {year}")
        if year == 2020:
            logger.info(f'Loading initial technology choices for {year}')
            for row in active_plant_df.itertuples():
                PlantChoiceContainer.update_choice(year, row.plant_name, row.initial_technology)
            UtilizationContainer.assign_year_utilization(2020, wsa_dict)

        # Exceptions for plants in plants database that are scheduled to open later, to have their prior technology as their previous choice
        for row in inactive_year_start_df.itertuples():
            if row.start_of_operation == year + 1:
                PlantChoiceContainer.update_choice(year, row.plant_name, row.initial_technology)

        all_active_plant_names = active_plant_df["plant_name"].copy()
        plant_to_region_mapper = dict(zip(all_active_plant_names, active_plant_df[MAIN_REGIONAL_SCHEMA]))
        plant_capacities_dict = CapacityContainer.return_plant_capacity(year=year)
        switchers = PlantInvestmentCycleContainer.return_plant_switchers(all_active_plant_names, year, 'combined')
        non_switchers = list(set(all_active_plant_names).difference(switchers))
        switchers_df = (
            active_plant_df.set_index(["plant_name"]).drop(non_switchers).reset_index()
        ).copy()
        switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        non_switchers_df = (
            active_plant_df.set_index(["plant_name"]).drop(switchers).reset_index()
        ).copy()
        non_switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        logger.info(f"-- Assigning usage for exisiting plants")

        # check resource allocation for non-switchers
        for row in non_switchers_df.itertuples():
            plant_name = row.plant_name
            current_tech = ''
            year_founded = PlantInvestmentCycleContainer.plant_start_years[plant_name]
        
            if (year == 2020) or (year == year_founded):
                current_tech = row.initial_technology
            else:
                current_tech = PlantChoiceContainer.get_choice(year - 1, plant_name)
            PlantChoiceContainer.update_choice(year, plant_name, current_tech)
            
            entry = {
                'year': year,
                'plant_name': plant_name,
                'current_tech': current_tech,
                'switch_tech': current_tech,
                'switch_type': 'not a switch year'
            }
            PlantChoiceContainer.update_records(entry)
        prior_year_utilization = UtilizationContainer.get_utilization_values(year) if year == 2020 else UtilizationContainer.get_utilization_values(year-1)

        for resource in RESOURCE_CONTAINER_REF:
            current_usage = return_current_usage(
                non_switchers,
                PlantChoiceContainer.return_choices(year),
                plant_capacities_dict,
                prior_year_utilization,
                plant_to_region_mapper,
                business_case_ref,
                RESOURCE_CONTAINER_REF[resource],
            )
            if resource == 'scrap':
                logger.info(f'Scrap usage | Non-Switchers: {current_usage: 0.2f} | Count: {len(non_switchers)}')
            MaterialUsageContainer.constraint_transaction(
                year, resource, current_usage, override_constraint=True, apply_transaction=True)

        # check resource allocation for EAF secondary capacity
        secondary_eaf_switchers = switchers_df[switchers_df['primary_capacity'] == 'N'].copy()
        secondary_eaf_switchers_plants = secondary_eaf_switchers['plant_name'].unique()

        for plant_name in secondary_eaf_switchers_plants:
            entry = {
                'year': year, 
                'plant_name': plant_name, 
                'current_tech': 'EAF', 
                'switch_tech': 'EAF', 
                'switch_type': 'Secondary capacity is always EAF'
            }
            PlantChoiceContainer.update_records(entry)
            PlantChoiceContainer.update_choice(year, plant_name, 'EAF')

        for resource in RESOURCE_CONTAINER_REF:
            current_usage = return_current_usage(
                secondary_eaf_switchers_plants,
                PlantChoiceContainer.return_choices(year),
                plant_capacities_dict,
                prior_year_utilization,
                plant_to_region_mapper,
                business_case_ref,
                RESOURCE_CONTAINER_REF[resource],
            )
            if resource == 'scrap':
                logger.info(f'Scrap usage | Switchers - Secondary EAF: {current_usage: 0.2f} | Count: {len(secondary_eaf_switchers_plants)}')
            MaterialUsageContainer.constraint_transaction(
                year, resource, current_usage, override_constraint=True, apply_transaction=True)
        
        logger.info(f"Scrap usage | Amount remaining for switchers/new plants: {MaterialUsageContainer.get_current_balance(year, 'scrap'): 0.2f}")

        # Run open/close capacity
        capacity_adjusted_df = open_close_flow(
            plant_container=PlantIDC,
            market_container=market_container,
            plant_df=active_plant_df,
            levelized_cost=levelized_cost,
            steel_demand_df=steel_demand_df,
            country_df=country_ref_f,
            business_case_ref=business_case_ref,
            tech_availability=tech_availability,
            variable_costs_df=variable_costs_regional,
            capex_dict=capex_dict,
            tech_choices_container=PlantChoiceContainer,
            investment_container=PlantInvestmentCycleContainer,
            capacity_container=CapacityContainer,
            utilization_container=UtilizationContainer,
            material_container=MaterialUsageContainer,
            year=year,
            trade_scenario=trade_scenario,
            tech_moratorium=tech_moratorium,
            enforce_constraints=enforce_constraints
        )
        capacity_adjusted_active_plants = capacity_adjusted_df[capacity_adjusted_df['active_check'] == True].copy()
        all_active_plant_names = capacity_adjusted_active_plants["plant_name"].copy()
        plant_capacities_dict = CapacityContainer.return_plant_capacity(year=year)
        switchers = PlantInvestmentCycleContainer.return_plant_switchers(all_active_plant_names, year, 'combined')
        non_switchers = list(set(all_active_plant_names).difference(switchers))
        switchers_df = (
            capacity_adjusted_active_plants.set_index(["plant_name"]).drop(non_switchers).reset_index()
        ).copy()
        switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        switchers_df = switchers_df.sample(frac=1)
        logger.info(f"-- Running investment decisions for Non Switching Plants")

        primary_switchers_df = switchers_df[switchers_df['primary_capacity'] == 'Y'].copy()

        for row in primary_switchers_df.itertuples():
            # set initial metadata
            plant_name = row.plant_name
            country_code = row.country_code
            year_founded = PlantInvestmentCycleContainer.plant_start_years[plant_name]
            current_tech = ''
            if (year == 2020) or (year == year_founded):
                current_tech = row.initial_technology
            else:
                current_tech = PlantChoiceContainer.get_choice(year - 1, plant_name)
            entry = {'year': year, 'plant_name': plant_name, 'current_tech': current_tech}
            
            # CASE 1: CLOSE PLANT
            if current_tech == "Close plant":
                
                PlantChoiceContainer.update_choice(year, plant_name, "Close plant") 
                entry['switch_tech'] = "Close plant"
                entry['switch_type'] = 'Plant was already closed'

            # CASE 2: SWITCH TECH
            else:
                switch_type = PlantInvestmentCycleContainer.return_plant_switch_type(plant_name, year)

                # CASE 2-A: MAIN CYCLE
                if switch_type == "main cycle":
                    best_choice_tech = return_best_tech(
                        tco_reference_data=tco_slim,
                        abatement_reference_data=abatement_slim,
                        business_case_ref=business_case_ref,
                        variable_costs_df=variable_costs_regional,
                        green_premium_timeseries=green_premium_timeseries,
                        tech_availability=tech_availability,
                        tech_avail_from_dict=ta_dict,
                        plant_capacities=plant_capacities_dict,
                        scenario_dict=scenario_dict,
                        investment_container=PlantInvestmentCycleContainer,
                        year=year,
                        plant_name=plant_name,
                        country_code=country_code,
                        base_tech=current_tech,
                        material_usage_dict_container=MaterialUsageContainer,
                    )
                    if best_choice_tech == current_tech:
                        entry['switch_type'] = 'No change in main investment cycle year'
                    else:
                        entry['switch_type'] = 'Regular change in investment cycle year'
                    PlantChoiceContainer.update_choice(year, plant_name, best_choice_tech)

                # CASE 2-B: TRANSITIONARY SWITCH
                if switch_type == "trans switch":
                    best_choice_tech = return_best_tech(
                        tco_reference_data=tco_slim,
                        abatement_reference_data=abatement_slim,
                        business_case_ref=business_case_ref,
                        variable_costs_df=variable_costs_regional,
                        green_premium_timeseries=green_premium_timeseries,
                        tech_availability=tech_availability,
                        tech_avail_from_dict=ta_dict,
                        plant_capacities=plant_capacities_dict,
                        scenario_dict=scenario_dict,
                        investment_container=PlantInvestmentCycleContainer,
                        year=year,
                        plant_name=plant_name,
                        country_code=country_code,
                        base_tech=current_tech,
                        material_usage_dict_container=MaterialUsageContainer,
                    )
                    if best_choice_tech != current_tech:
                        entry['switch_type'] = 'Transitional switch in off-cycle investment year'
                        PlantInvestmentCycleContainer.adjust_cycle_for_transitional_switch(plant_name, year)
                    else:
                        entry['switch_type'] = 'No change during off-cycle investment year'
                    PlantChoiceContainer.update_choice(year, plant_name, best_choice_tech)

                entry['switch_tech'] = best_choice_tech

            PlantChoiceContainer.update_records(entry)
        year_start_df = pd.concat([capacity_adjusted_df, inactive_year_start_df]).reset_index(drop=True)
        MaterialUsageContainer.print_year_summary(year)

    final_steel_plant_df = year_start_df.copy()
    active_check_results_dict = active_check_results(final_steel_plant_df, MODEL_YEAR_RANGE)
    trade_summary_results = market_container.output_trade_summary_to_df()
    full_trade_calculations = market_container.output_trade_calculations_to_df()
    material_usage_results = MaterialUsageContainer.output_results_to_df()
    investment_dict = PlantInvestmentCycleContainer.return_investment_dict()
    plant_cycle_length_mapper = PlantInvestmentCycleContainer.return_cycle_lengths()
    investment_df = PlantInvestmentCycleContainer.create_investment_df()
    tech_choice_dict = PlantChoiceContainer.return_choices()
    tech_choice_records = PlantChoiceContainer.output_records_to_df()
    regional_capacity_results = CapacityContainer.return_regional_capacity()
    plant_capacity_results = CapacityContainer.return_plant_capacity()
    utilization_results = UtilizationContainer.get_utilization_values()
    constraints_summary = MaterialUsageContainer.output_constraints_summary(MODEL_YEAR_RANGE)

    return {
        'tech_choice_dict': tech_choice_dict,
        'tech_choice_records': tech_choice_records,
        'plant_result_df': final_steel_plant_df,
        'active_check_results_dict': active_check_results_dict,
        'investment_cycle_ref_result': investment_df,
        'investment_dict_result': investment_dict,
        'plant_cycle_length_mapper_result': plant_cycle_length_mapper,
        'regional_capacity_results': regional_capacity_results,
        'plant_capacity_results': plant_capacity_results,
        'utilization_results': utilization_results,
        'trade_summary_results': trade_summary_results,
        'full_trade_calculations': full_trade_calculations,
        'material_usage_results': material_usage_results,
        'constraints_summary': constraints_summary
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
        serialize_file(results_dict['active_check_results_dict'], intermediate_path, "active_check_results_dict")
        serialize_file(results_dict['investment_cycle_ref_result'], intermediate_path, "investment_cycle_ref_result")
        serialize_file(results_dict['investment_dict_result'], intermediate_path, "investment_dict_result")
        serialize_file(results_dict['plant_cycle_length_mapper_result'], intermediate_path, "plant_cycle_length_mapper_result")
        serialize_file(results_dict['regional_capacity_results'], intermediate_path, "regional_capacity_results")
        serialize_file(results_dict['plant_capacity_results'], intermediate_path, "plant_capacity_results")
        serialize_file(results_dict['utilization_results'], intermediate_path, "utilization_results")
        serialize_file(results_dict['trade_summary_results'], intermediate_path, "trade_summary_results")
        serialize_file(results_dict['full_trade_calculations'], intermediate_path, "full_trade_calculations")
        serialize_file(results_dict['material_usage_results'], intermediate_path, "material_usage_results")
        serialize_file(results_dict['constraints_summary'], intermediate_path, 'constraints_summary')
        serialize_file(levelized_cost_updated, intermediate_path, 'levelized_cost_updated')
    return results_dict
