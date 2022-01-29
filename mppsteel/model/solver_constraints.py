"""Script with functions for implementing solver constraints."""

import pandas as pd

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger
)

from mppsteel.model_config import (
    PKL_DATA_INTERMEDIATE, TECH_MORATORIUM_DATE
)

from mppsteel.utility.reference_lists import (
    SWITCH_DICT, TECHNOLOGY_STATES, RESOURCE_CONTAINER_REF
)

from mppsteel.data_loading.data_interface import (
    ccs_co2_getter,
)

from mppsteel.data_loading.pe_model_formatter import (
    bio_constraint_getter
)

from mppsteel.data_loading.reg_steel_demand_formatter import (
    steel_demand_getter
)

# Create logger
logger = get_logger("Solver Constraints")

def map_technology_state(tech: str):
    for tech_state in TECHNOLOGY_STATES.keys():
        if tech in TECHNOLOGY_STATES[tech_state]:
            return tech_state


def read_and_format_tech_availability(df: pd.DataFrame):
    """[summary]

    Returns:
        [type]: [description]
    """
    df_c = df.copy()
    df_c.columns = [col.lower().replace(' ', '_') for col in df_c.columns]
    df_c = df_c[~df_c['technology'].isin(['Close plant', 'Charcoal mini furnace', 'New capacity'])]
    df_c['technology_phase'] = df_c['technology'].apply(lambda x: map_technology_state(x))
    return df_c[['technology', 'main_technology_type', 'technology_phase', 'year_available_from', 'year_available_until']].set_index('technology')

def tech_availability_check(
    tech_df: pd.DataFrame, technology: str, year: int,
    tech_moratorium: bool = False, default_year_unavailable: int = 2200) -> bool:
    """[summary]

    Args:
        tech_df (pd.DataFrame): [description]
        technology (str): [description]
        year (int): [description]
        tech_moratorium (bool, optional): [description]. Defaults to False.

    Returns:
        bool: [description]
    """
    row = tech_df.loc[technology]
    year_available_from = row.loc['year_available_from']
    technology_phase = row.loc['technology_phase']
    year_available_until = default_year_unavailable

    if tech_moratorium and (technology_phase == 'current'):
        year_available_until = TECH_MORATORIUM_DATE
    if int(year_available_from) <= int(year) < int(year_available_until):
        # print(f'{technology} will be available in {year}')
        return True
    if int(year) <= int(year_available_from):
        # print(f'{technology} will not be ready yet in {year}')
        return False
    if int(year) > int(year_available_until):
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
        return 'Close plant'
    return current_tech


def create_plant_capacities_dict():
    """[summary]

    Returns:
        [type]: [description]
    """
    steel_plant_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'steel_plants_processed', 'df')
    plant_capacities = {}
    for row in steel_plant_df.itertuples():
        plant_name = row.plant_name
        row = {
            '2020_tech': row.technology_in_2020,
            'primary_capacity': row.primary_capacity_2020,
            'secondary_capacity': row.secondary_capacity_2020
        }
        plant_capacities[plant_name] = row
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


def material_usage_calc(
    plant_capacities: dict, steel_demand_df: pd.DataFrame, business_cases: pd.DataFrame,
    materials_list: list, plant_name: str, country_code: str, year: float, tech: str, material: str, steel_demand_scenario: str
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
    steel_demand = steel_demand_getter(steel_demand_df, year, steel_demand_scenario, 'crude', country_code)
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
    bio_constraint_model: pd.DataFrame,
    ccs_co2_df: pd.DataFrame,
    materials_list: list,
    tech_material_dict: dict,
    resource_container_ref: dict,
    plant_capacities: dict,
    material_usage_dict: dict = None,
    output_type: str = 'excluded'
):
    """[summary]

    Args:
        plant_name (str): [description]
        base_tech (str): [description]
        year (int): [description]
        steel_demand_df (pd.DataFrame): [description]
        business_cases (pd.DataFrame): [description]
        bio_constraint_model (pd.DataFrame): [description]
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

    tech_approved_list = []

    country_code = steel_plant_df[steel_plant_df['plant_name'] == plant_name]['country_code'].values[0]

    for tech in tech_list:

        material_check_container = []

        for material_check in tech_material_dict[tech]:
            # Setting parameters
            if material_check == 'Bioenergy':
                material_ref = 'Bioenergy'
                material_capacity = bio_constraint_getter(bio_constraint_model, year)
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
                material_capacity = steel_demand_getter(steel_demand_df, year, steel_demand_scenario, 'crude', country_code)
                materials_to_check = ['Scrap']

            # Checking for zero
            if material_capacity <= 0:
                #logger.info(f'{year} -> Material {material_check} is not available, capacity = 0')
                material_check_container.append(False)
            else:
                # Core logic
                material_container = material_usage_dict[resource_container_ref[material_ref]]
                if material_check in ['Bioenergy', 'Used CO2', 'Captured CO2', 'Scrap']:
                    current_usage = sum(material_container)
                    if current_usage == 0:
                        #logger.info('First usage for {material_check}')
                        pass
                    resource_remaining = material_capacity - current_usage
                    plant_usage = material_usage_calc(plant_capacities, steel_demand_df, business_cases, materials_list, plant_name, country_code, year, tech, materials_to_check, steel_demand_scenario)
                    if plant_usage > resource_remaining:
                        # print(f'{year} -> {plant_name} cannot adopt {tech} because usage of {material_check} exceeds capacity | uses {plant_usage} of remaining {resource_remaining}')
                        material_check_container.append(False)
                    else:
                        # print(f'{year} -> {plant_name} can adopt {tech} because usage of {material_check} does not exceed capacity | uses {plant_usage} of remaining {resource_remaining}')
                        material_check_container.append(True)
                        material_container.append(plant_usage)

                if material_check in ['Scrap EAF']:
                    if material_capacity > 1.5:
                        # print(f'Sufficient enough scrap for {tech} -> {material_capacity}')
                        material_check_container.append(True)
                    else:
                        # print(f'Not enough scrap for {tech}')
                        material_check_container.append(False)

        if all(material_check_container):
            #logger.info(f'PASSED: {tech} has passed availability checks for {plant_name}')
            tech_approved_list.append(tech)
        else:
            if tech == base_tech:
                #logger.info(f'PASSED: {tech} is the same as based tech, but would have failed otherwise')
                tech_approved_list.append(tech)
                # material_container.append(plant_usage)
            else:
                pass
                #logger.info(f'FAILED: {tech} has NOT passed availability checks for {plant_name}')

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
