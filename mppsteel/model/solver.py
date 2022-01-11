"""Main solving script for deciding investment decisions."""

import pandas as pd
from tqdm import tqdm

from mppsteel.utility.utils import (
    read_pickle_folder, serialize_file,
    get_logger, return_furnace_group, 
    timer_func, add_scenarios
)

from mppsteel.model_config import (
    MODEL_YEAR_START, PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE,
    GREEN_PREMIUM_SCENARIOS, MODEL_YEAR_END, INVESTMENT_CYCLE_LENGTH
)

from mppsteel.utility.reference_lists import (
    SWITCH_DICT, TECHNOLOGY_STATES, FURNACE_GROUP_DICT,
    TECH_MATERIAL_CHECK_DICT, RESOURCE_CONTAINER_REF,
)

from mppsteel.minimodels.timeseries_generator import (
    timeseries_generator,
)

from mppsteel.data_loading.data_interface import (
    ccs_co2_getter, biomass_getter,
    load_materials, load_business_cases
)

from mppsteel.data_loading.reg_steel_demand_formatter import (
    steel_demand_getter
)

from mppsteel.model.tco import (
    tco_calc, tco_min_ranker,
    abatement_min_ranker
)

# Create logger
logger = get_logger("Solver")


def read_and_format_tech_availability():
    """[summary]

    Returns:
        [type]: [description]
    """    
    tech_availability = read_pickle_folder(PKL_DATA_IMPORTS, 'tech_availability', 'df')
    tech_availability.columns = [col.lower().replace(' ', '_') for col in tech_availability.columns]
    return tech_availability[['technology', 'main_technology_type', 'technology_phase', 'year_available_from', 'year_available_until']].set_index('technology')

def tech_availability_check(tech_df: pd.DataFrame, technology: str, year: int, tech_moratorium: bool = False) -> bool:
    """[summary]

    Args:
        tech_df (pd.DataFrame): [description]
        technology (str): [description]
        year (int): [description]
        tech_moratorium (bool, optional): [description]. Defaults to False.

    Returns:
        bool: [description]
    """    
    row = tech_df.loc[technology].copy()
    year_available = row.loc['year_available_from']
    year_unavailable = row.loc['year_available_until']
    if tech_moratorium:
        if row.loc['technology_phase'] == 'Initial':
            year_unavailable = 2030
    if year_available < year < year_unavailable:
        # print(f'{technology} will be available in {year}')
        return True
    if year < year_available:
        # print(f'{technology} will not be ready yet in {year}')
        return False
    if year > year_unavailable:
        # print(f'{technology} will become unavailable in {year}')
        return False


def plant_closure_check(utilization_rate: float, cutoff: float, current_tech: str):
    """[summary]

    Args:
        utilization_rate (float): [description]
        cutoff (float): [description]
        current_tech (str): [description]

    Returns:
        [type]: [description]
    """    
    if utilization_rate < cutoff:
        return 'Close Plant'
    return current_tech


def create_plant_capacities_dict():
    """[summary]

    Returns:
        [type]: [description]
    """    
    # Edit this one!
    steel_plant_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'steel_plants_processed', 'df')
    technologies = steel_plant_df['technology_in_2020']
    plant_names = steel_plant_df['plant_name']
    primary_capacities = steel_plant_df['primary_capacity_2020']
    secondary_capacities = steel_plant_df['secondary_capacity_2020']
    plant_capacities = {}
    ticker = 0
    while ticker < len(plant_names):
        plant_name = plant_names.iloc[ticker]
        row = {
            '2020_tech': technologies.iloc[ticker],
            'primary_capacity': primary_capacities.iloc[ticker],
            'secondary_capacity': secondary_capacities.iloc[ticker]}
        plant_capacities[plant_name] = row
        ticker += 1
    return plant_capacities


def calculate_primary_and_secondary(tech_capacities: dict, plant: str, tech: str):
    """[summary]

    Args:
        tech_capacities (dict): [description]
        plant (str): [description]
        tech (str): [description]

    Returns:
        [type]: [description]
    """    
    if tech == 'EAF':
        return tech_capacities[plant]['secondary_capacity'] + tech_capacities[plant]['primary_capacity']
    return tech_capacities[plant]['primary_capacity']

def material_usage_summary(business_case_df: pd.DataFrame, material: str, technology: str = ''):
    """[summary]

    Args:
        business_case_df (pd.DataFrame): [description]
        material (str): [description]
        technology (str, optional): [description]. Defaults to ''.

    Returns:
        [type]: [description]
    """    
    if technology:
        try:
            return business_case_df.groupby(['material_category', 'technology']).sum().loc[material, technology].values[0]
        except:
            return 0
    return business_case_df.groupby(['material_category', 'technology']).sum().loc[material]

def total_plant_capacity(plant_cap_dict: dict):
    """[summary]

    Args:
        plant_cap_dict (dict): [description]

    Returns:
        [type]: [description]
    """    
    all_capacities = [calculate_primary_and_secondary(plant_cap_dict, plant, plant_cap_dict[plant]['2020_tech']) for plant in plant_cap_dict.keys()]
    all_capacities = [x for x in all_capacities if str(x) != 'nan']
    return sum(all_capacities)


def material_usage(
    plant_capacities: dict, steel_plant_df: pd.DataFrame, business_cases: pd.DataFrame,
    materials_list: list, plant_name: str, year: float, tech: str, material: str,
    steel_demand_scenario: str
    ):
    """[summary]

    Args:
        plant_capacities (dict): [description]
        steel_plant_df (pd.DataFrame): [description]
        business_cases (pd.DataFrame): [description]
        materials_list (list): [description]
        plant_name (str): [description]
        year (float): [description]
        tech (str): [description]
        material (str): [description]

    Returns:
        [type]: [description]
    """

    plant_capacity = calculate_primary_and_secondary(plant_capacities, plant_name, tech) / 1000
    plant_country = steel_plant_df[steel_plant_df['plant_name'] == plant_name]['country_code'].values[0]
    steel_demand = steel_demand_getter(steel_plant_df, year, steel_demand_scenario, 'crude', plant_country)
    capacity_sum = total_plant_capacity(plant_capacities)
    projected_production = (plant_capacity / capacity_sum) * steel_demand
    material_list = []
    for material in materials_list:
        usage_value = material_usage_summary(business_cases, material, tech)
        material_list.append(usage_value)
    material_usage = projected_production * sum(material_list)
    return material_usage

def plant_tech_resource_checker(
    plant_name: str,
    base_tech: str,
    year: int,
    steel_demand_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    steel_demand_scenario: str,
    business_cases: pd.DataFrame,
    biomass_df: pd.DataFrame,
    ccs_co2_df: pd.DataFrame,
    materials_list: list,
    tech_material_dict: dict,
    resource_container_ref: dict,
    plant_capacities: dict,
    material_usage_dict: dict = {},
    output_type: str = 'excluded'
):
    """[summary]

    Args:
        plant_name (str): [description]
        base_tech (str): [description]
        year (int): [description]
        steel_demand_df (pd.DataFrame): [description]
        business_cases (pd.DataFrame): [description]
        biomass_df (pd.DataFrame): [description]
        ccs_co2_df (pd.DataFrame): [description]
        materials_list (list): [description]
        tech_material_dict (dict): [description]
        resource_container_ref (dict): [description]
        plant_capacities (dict): [description]
        material_usage_dict (dict, optional): [description]. Defaults to {}.
        output_type (str, optional): [description]. Defaults to 'excluded'.

    Returns:
        [type]: [description]
    """

    tech_list = SWITCH_DICT[base_tech].copy()
    if 'Close plant' in tech_list:
        tech_list.remove('Close plant')

    tech_approved_list = []

    for tech in tech_list:

        material_check_container = []

        for material_check in tech_material_dict[tech]:
            # Setting parameters
            if material_check == 'Bioenergy':
                material_ref = 'Bioenergy'
                material_capacity = biomass_getter(biomass_df, year)
                materials_to_check = ['Biomass', 'Biomethane']

            if material_check == 'Used CO2':
                material_ref = 'Used CO2'
                material_capacity = ccs_co2_getter(ccs_co2_df, 'Steel CO2 use market', year)
                materials_to_check = ['Used CO2']

            if material_check == 'Captured CO2':
                material_ref = 'Captured CO2'
                material_capacity = ccs_co2_getter(ccs_co2_df, 'Total Steel CCS capacity', year)
                materials_to_check = ['Captured CO2']

            if material_check in ['Scrap', 'Scrap EAF']:
                material_ref = 'Scrap'
                # material_capacity = steel_demand_value_selector(steel_demand_df, 'Scrap', year, 'bau')
                plant_country = steel_plant_df[steel_plant_df['plant_name'] == plant_name]['country_code'].values[0]
                material_capacity = steel_demand_getter(steel_plant_df, year, steel_demand_scenario, 'Crude', plant_country)
                materials_to_check = ['Scrap']

            # Checking for zero
            if material_capacity <= 0:
                logger.info(f'{year} -> Material {material_check} is not available, capacity = 0')
                material_check_container.append(False)
            else:
                # Core logic
                material_container = material_usage_dict[resource_container_ref[material_ref]]
                if material_check in ['Bioenergy', 'Used CO2', 'Captured CO2', 'Scrap']:
                    current_usage = sum(material_container)
                    if current_usage == 0:
                        logger.info('First usage for {material_check}')
                    resource_remaining = material_capacity - current_usage
                    plant_usage = material_usage(plant_capacities, steel_demand_df, business_cases, materials_list, plant_name, year, tech, materials_to_check, steel_demand_scenario)
                    if plant_usage > resource_remaining:
                        print(f'{year} -> {plant_name} cannot adopt {tech} because usage of {material_check} exceeds capacity | uses {plant_usage} of remaining {resource_remaining}')
                        material_check_container.append(False)
                    else:
                        print(f'{year} -> {plant_name} can adopt {tech} because usage of {material_check} does not exceed capacity | uses {plant_usage} of remaining {resource_remaining}')
                        material_check_container.append(True)
                        material_container.append(plant_usage)

                if material_check in ['Scrap EAF']:
                    if material_capacity > 1.5:
                        print(f'Sufficient enough scrap for {tech} -> {material_capacity}')
                        material_check_container.append(True)
                    else:
                        print(f'Not enough scrap for {tech}')
                        material_check_container.append(False)

        if all(material_check_container):
            logger.info(f'PASSED: {tech} has passed availability checks for {plant_name}')
            tech_approved_list.append(tech)
        else:
            if tech == base_tech:
                logger.info(f'PASSED: {tech} is the same as based tech, but would have failed otherwise')
                tech_approved_list.append(tech)
                # material_container.append(plant_usage)
            else:
                logger.info(f'FAILED: {tech} has NOT passed availability checks for {plant_name}')

    unavailable_techs = list(set(tech_list).difference(set(tech_approved_list)))
    # Final check and return

    if output_type == 'excluded':
        return unavailable_techs
    if output_type == 'included':
        return tech_approved_list

def create_new_material_usage_dict(resource_container_ref: dict):
    """[summary]

    Args:
        resource_container_ref (dict): [description]

    Returns:
        [type]: [description]
    """    
    return {material_key: [] for material_key in resource_container_ref.values()}

def overall_scores(
    tco_df: pd.DataFrame,
    emissions_df: pd.DataFrame,
    proportions_dict: dict,
    steel_demand_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    biomass_df: pd.DataFrame,
    ccs_co2_df: pd.DataFrame,
    plant_capacities: dict,
    materials_list: list,
    year: int,
    plant_name: str,
    steel_demand_scenario: str,
    base_tech: str = '',
    tech_moratorium: bool = False,
    transitional_switch_only: bool = False,
    material_usage_dict_container: dict = {},
    return_container: bool = True,
):
    """[summary]

    Args:
        tco_df (pd.DataFrame): [description]
        emissions_df (pd.DataFrame): [description]
        proportions_dict (dict): [description]
        steel_demand_df (pd.DataFrame): [description]
        steel_plant_df (pd.DataFrame): [description]
        business_cases (pd.DataFrame): [description]
        biomass_df (pd.DataFrame): [description]
        ccs_co2_df (pd.DataFrame): [description]
        plant_capacities (dict): [description]
        materials_list (list): [description]
        year (int): [description]
        plant_name (str): [description]
        steel_demand_scenario (str): [description]
        base_tech (str, optional): [description]. Defaults to ''.
        tech_moratorium (bool, optional): [description]. Defaults to False.
        transitional_switch_only (bool, optional): [description]. Defaults to False.
        material_usage_dict_container (dict, optional): [description]. Defaults to {}.
        return_container (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """
    tco_df_c = tco_df.copy().sort_index()
    emissions_df_c = emissions_df.copy().sort_index()
    new_df = tco_df_c.copy()
    new_df['combined_score'] = ( proportions_dict['tco'] * tco_df_c['rank_score'] ) + ( proportions_dict['emissions'] * emissions_df_c['rank_score'] )

    tech_availability = read_and_format_tech_availability()
    # Availability checks
    unavailable_techs = [tech for tech in tco_df.index if not tech_availability_check(tech_availability, tech, year, tech_moratorium=tech_moratorium)]

    if base_tech in unavailable_techs:
        print(f'Current tech {base_tech} is unavailable in {year}, but including anyway')
        unavailable_techs.remove(base_tech)

    # Constraints checks
    constraints_check = plant_tech_resource_checker(
        plant_name, base_tech, year, steel_demand_df,
        steel_plant_df, steel_demand_scenario,
        business_cases, materials_list, biomass_df,
        ccs_co2_df, TECH_MATERIAL_CHECK_DICT,
        RESOURCE_CONTAINER_REF, material_usage_dict_container,
        plant_capacities, 'excluded'
        )

    # Non_switches
    excluded_switches = [key for key in SWITCH_DICT.keys() if key not in SWITCH_DICT[base_tech]]

    # Drop excluded techs
    combined_unavailable_list = list(set(unavailable_techs + constraints_check + excluded_switches))
    new_df.drop(combined_unavailable_list, inplace=True)

    # Transitional switches
    if transitional_switch_only:
        # Cannot downgrade tech
        # Must be current or transitional tech
        transitional_switch_possibilities = TECHNOLOGY_STATES['current'] + TECHNOLOGY_STATES['transitional']
        matches = set(transitional_switch_possibilities).intersection(set(new_df.index))
        # Must be within the furnace group
        furnace_matches = matches.intersection(set(return_furnace_group(FURNACE_GROUP_DICT, base_tech)))
        new_df = new_df.loc[furnace_matches]

    new_df.drop(['rank_score'], axis=1, inplace=True)
    if return_container:
        return new_df.sort_values('combined_score', ascending=False), material_usage_dict_container
    return new_df.sort_values('combined_score', ascending=False)


def choose_technology(
    year_end: int, rank_only: bool = False, 
    tech_moratorium: bool = False,
    error_plant: str = '',
    carbon_tax_scenario: bool = False, 
    green_premium_scenario: float = 0,
    steel_demand_scenario: str = 'bau',
    tech_switch_scenario: dict = {'tco': 0.6, 'emissions': 0.4},
    ):
    """[summary]

    Args:
        year_end (int): [description]
        rank_only (bool, optional): [description]. Defaults to False.
        tech_moratorium (bool, optional): [description]. Defaults to False.
        error_plant (str, optional): [description]. Defaults to ''.

    Returns:
        [type]: [description]
    """    

    logger.info('Creating Steel plant df')

    def plant_name_check(df: pd.DataFrame, plant_name: str, name_to_check: str, extra: str = ''):
        if plant_name == name_to_check:
            print(f'{plant_name} : {extra}')
            print(df[df['plant_name'] == plant_name]['technology_in_2020'].values[0])

    plant_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'steel_plants_processed', 'df')
    investment_year_ref = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'plant_investment_cycles', 'df')
    # steel_demand_df = extend_steel_demand(MODEL_YEAR_END)
    steel_demand_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'regional_steel_demand_formatted', 'df')
    carbon_tax_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'carbon_tax', 'df')
    all_plant_variable_costs_summary = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'all_plant_variable_costs_summary', 'df')
    biomass_availability = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'biomass_availability', 'df')
    ccs_co2 = read_pickle_folder(PKL_DATA_IMPORTS, 'ccs_co2', 'df')
    green_premium_scenario_values = GREEN_PREMIUM_SCENARIOS['green_premium_scenario']
    green_premium_timeseries = timeseries_generator('carbon_tax', MODEL_YEAR_START,year_end, green_premium_scenario_values[1],green_premium_scenario_values[0],'pct')
    emissions_switching_df_summary = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'emissions_switching_df_summary', 'df')
    materials = load_materials()
    opex_values_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'capex_dict', 'df')
    business_cases = load_business_cases()
    plant_capacities_dict = create_plant_capacities_dict()

    all_plant_names = plant_df['plant_name'].copy()

    year_range = range(MODEL_YEAR_START, year_end+1)
    current_plant_choices = {}
    for year in tqdm(year_range, total=len(year_range), desc='Years'):
        logger.info(f'Running investment decisions for {year}')
        current_plant_choices[str(year)] = {}

        switchers = extract_tech_plant_switchers(investment_year_ref, year)
        non_switchers = list(set(all_plant_names).difference(switchers))

        switchers_df = plant_df.set_index(['plant_name']).drop(non_switchers).reset_index()
        switchers_df.rename({'index': 'plant_name'},axis=1,inplace=True)
        non_switchers_df = plant_df.set_index(['plant_name']).drop(switchers).reset_index()
        non_switchers_df.rename({'index': 'plant_name'},axis=1,inplace=True)

        if year == 2020:
            technologies = non_switchers_df['technology_in_2020'].values

        else:
            technologies = current_plant_choices[str(year-1)].values()

        yearly_usage = material_usage_per_plant(non_switchers, technologies, business_cases, plant_df, plant_capacities_dict, steel_demand_df, materials, year, steel_demand_scenario)
        material_usage_dict = load_resource_usage_dict(yearly_usage)
        logger.info(f'-- Running investment decisions for Non Switching Plants')
        for plant_name in tqdm(non_switchers, total=len(non_switchers), desc=f'Non Switchers {year}'):
            # plant_name_check(plant_name, error_plant, 'Non Switch Year')
            if year == 2020:
                # plant_name_check(plant_name, error_plant, 'Non Switch Year: 2020')
                tech_in_2020 = non_switchers_df[non_switchers_df['plant_name'] == plant_name]['technology_in_2020'].values[0]
                current_plant_choices[str(year)][plant_name] = tech_in_2020
            else:
                # plant_name_check(plant_name, error_plant, 'Non Switch Year: Non-2020')
                current_plant_choices[str(year)][plant_name] = current_plant_choices[str(year-1)][plant_name]

        logger.info(f'-- Running investment decisions for Switching Plants')
        for plant in tqdm(switchers_df.itertuples(), total=switchers_df.shape[0], desc=f'Switchers {year}'):
            plant_name = plant.plant_name

            # plant_name_check(plant_name, error_plant, 'Switch Year')

            if year == 2020:
                # plant_name_check(plant_name, error_plant, 'Switch Year 2020')
                tech_in_2020 = switchers_df[switchers_df['plant_name'] == plant_name]['technology_in_2020'].values[0]
                current_tech = tech_in_2020

            else:
                # plant_name_check(plant_name, error_plant, 'Switch Year Non-2020')
                current_tech = current_plant_choices[str(year-1)][plant_name]

            if current_tech == 'Not operating' or 'Close plant':
                # plant_name_check(plant_name, error_plant, 'Closed')
                current_plant_choices[str(year)][plant_name] = 'Close plant'

            else:

                switch_type = investment_year_ref.reset_index().set_index(['year', 'plant_name']).loc[year, plant_name].values[0]

                tco = tco_calc(
                    plant, year, current_tech, carbon_tax_df, plant_df,
                    all_plant_variable_costs_summary, green_premium_timeseries,
                    opex_values_dict['other_opex'],carbon_tax_scenario, green_premium_scenario,
                    INVESTMENT_CYCLE_LENGTH)

                tco_switching_df_summary_final_rank = tco_min_ranker(tco, ['value'], rank_only)
                emissions_switching_df_summary_final_rank = abatement_min_ranker(emissions_switching_df_summary, current_tech, year, ['abated_s1_emissions'], rank_only)

                if switch_type == 'main cycle':
                    # plant_name_check(plant_name, error_plant, 'Main cycle')
                    best_score_tech = ''
                    scores, material_usage_dict = overall_scores(
                        tco_switching_df_summary_final_rank,
                        emissions_switching_df_summary_final_rank,
                        tech_switch_scenario,
                        steel_demand_df,
                        plant_df,
                        business_cases,
                        biomass_availability,
                        ccs_co2,
                        plant_capacities_dict,
                        materials,
                        year,
                        plant_name,
                        steel_demand_scenario,
                        current_tech,
                        tech_moratorium=tech_moratorium,
                        material_usage_dict_container=material_usage_dict,
                        return_container=True
                    )
                    try:
                        # Improve this!!!
                        best_score_tech = scores.index[0]
                        if best_score_tech == current_tech:
                            print(f'No change in main investment cycle in {year} for {plant_name} | {year} -> {current_tech} to {best_score_tech}')

                        else:
                            print(f'Regular change in main investment cycle in {year} for {plant_name} | {year} -> {current_tech} to {best_score_tech}')
                        current_plant_choices[str(year)][plant_name] = best_score_tech
                    except:
                        print(f'Error in ranking in {year} for {plant_name} with {current_tech}')
                        current_plant_choices[str(year)][plant_name] = current_tech

                if switch_type == 'trans switch':
                    # plant_name_check(plant_name, error_plant, 'Trans switch')
                    best_score_tech = ''
                    scores, material_usage_dict = overall_scores(
                        tco_switching_df_summary_final_rank,
                        emissions_switching_df_summary_final_rank,
                        tech_switch_scenario,
                        steel_demand_df,
                        business_cases,
                        biomass_availability,
                        ccs_co2,
                        plant_capacities_dict,
                        materials,
                        year,
                        plant_name,
                        steel_demand_scenario,
                        current_tech,
                        tech_moratorium=tech_moratorium,
                        transitional_switch_only=True,
                        material_usage_dict_container=material_usage_dict,
                        return_container=True
                    )
                    try:
                        # Change this!!
                        best_score_tech = scores.index[0]
                        if best_score_tech != current_tech:
                            print(f'Transistional switch flipped for {plant_name} in {year} -> {current_tech} to {best_score_tech}')
                        else:
                            print(f'{plant_name} kept its current tech {current_tech} in transitional year {year}')

                        current_plant_choices[str(year)][plant_name] = best_score_tech

                    except:
                        print(f'Error in ranking in {year} for {plant_name} with {current_tech}')
                        current_plant_choices[str(year)][plant_name] = current_tech

    return current_plant_choices

def material_usage_per_plant(
    plant_list: list,
    technology_list: list,
    business_cases: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    plant_capacities: dict,
    steel_demand_df: pd.DataFrame,
    materials_list: list, year: float,
    steel_demand_scenario: str):
    """[summary]

    Args:
        plant_list (list): [description]
        technology_list (list): [description]
        business_cases (pd.DataFrame): [description]
        plant_capacities (dict): [description]
        steel_demand_df (pd.DataFrame): [description]
        materials_list (list): [description]
        year (float): [description]

    Returns:
        [type]: [description]
    """    
    df_list = []
    zipped_data = zip(plant_list, technology_list)
    # steel_demand = steel_demand_value_selector(steel_demand_df, 'Crude', year, 'bau')
    capacity_sum = total_plant_capacity(plant_capacities)
    for plant_name, tech in zipped_data:
        plant_capacity = calculate_primary_and_secondary(plant_capacities, plant_name, tech) / 1000
        plant_country = steel_plant_df[steel_plant_df['plant_name'] == plant_name]['country_code'].values[0]
        steel_demand = steel_demand_getter(steel_demand_df, year, steel_demand_scenario, 'crude', plant_country)
        projected_production = (plant_capacity / capacity_sum) * steel_demand
        df = pd.DataFrame(index=materials_list, columns=['value'])
        for material in materials_list:
            usage_value = material_usage_summary(business_cases, material, tech)
            df.loc[material, 'value'] = projected_production * usage_value
        df_list.append(df)
    return pd.concat(df_list).reset_index().groupby(['index']).sum()


def extract_tech_plant_switchers(inv_cycle_ref: pd.DataFrame, year: int, combined_output: bool = True):
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
        main_switchers = inv_cycle_ref.sort_index().loc[year, 'main cycle']['plant_name'].to_list()
    except KeyError:
        pass
    try:
        trans_switchers = inv_cycle_ref.sort_index().loc[year, 'trans switch']['plant_name'].to_list()
    except KeyError:
        pass
    if combined_output:
        return main_switchers + trans_switchers
    return main_switchers, trans_switchers

def load_resource_usage_dict(yearly_usage_df: pd.DataFrame):
    """[summary]

    Args:
        yearly_usage_df (pd.DataFrame): [description]

    Returns:
        [type]: [description]
    """    
    resource_usage_dict = create_new_material_usage_dict(RESOURCE_CONTAINER_REF)
    resource_usage_dict['biomass'] = list({yearly_usage_df.loc['Biomass']['value'] or 0})
    resource_usage_dict['scrap'] = list({yearly_usage_df.loc['Scrap']['value'] or 0})
    resource_usage_dict['used_co2'] = list({yearly_usage_df.loc['Used CO2']['value'] or 0})
    resource_usage_dict['captured_co2'] = list({yearly_usage_df.loc['Captured CO2']['value'] or 0})
    return resource_usage_dict


@timer_func
def solver_flow(scenario_dict: dict, year_end: int, serialize_only: bool = False):
    """[summary]

    Args:
        year_end (int): [description]
        serialize_only (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """

    tech_choice_dict = choose_technology(
        year_end=year_end,
        rank_only=True,
        tech_moratorium=scenario_dict['tech_moratorium'],
        error_plant='SSAB Americas Alabama steel plant',
        carbon_tax_scenario=scenario_dict['carbon_tax'],
        green_premium_scenario=scenario_dict['green_premium_scenario'],
        steel_demand_scenario=scenario_dict['steel_demand_scenario'],
        tech_switch_scenario=scenario_dict['tech_switch_scenario']
        )

    if serialize_only:
        logger.info(f'-- Serializing dataframes')
        serialize_file(tech_choice_dict, PKL_DATA_INTERMEDIATE, "tech_choice_dict")
    return tech_choice_dict
