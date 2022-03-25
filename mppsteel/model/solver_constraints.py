"""Script with functions for implementing solver constraints."""

from typing import Union
import pandas as pd

from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.config.model_config import PKL_DATA_INTERMEDIATE, TECH_MORATORIUM_DATE
from mppsteel.config.reference_lists import (
    SWITCH_DICT,
    TECHNOLOGY_PHASES,
    TECHNOLOGY_STATES,
    RESOURCE_CONTAINER_REF,
)
from mppsteel.data_loading.data_interface import (
    ccs_co2_getter,
)
from mppsteel.data_loading.pe_model_formatter import bio_constraint_getter
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.data_loading.steel_plant_formatter import (
    calculate_primary_capacity, total_plant_capacity
)
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Solver Constraints")


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
    """[summary]

    Args:
        tech_df (pd.DataFrame): A formatted tech availability DataFrame.
        technology (str): The technology to check availability for.
        year (int): The year to check whether a specified `technology` is available or not.
        tech_moratorium (bool, optional): Boolean flag that determines whether a specified technology is available or not. Defaults to False.
        default_year_unavailable (int): Determines the default year a given technology will not be available from - will be altered according to function logic.

    Returns:
        bool: A boolean that determines whether a specified `technology` is available in the specified `year`.
    """
    row = tech_df.loc[technology]
    year_available_from = row.loc["year_available_from"]
    technology_phase = row.loc["technology_phase"]
    year_available_until = default_year_unavailable

    if tech_moratorium and (technology_phase in ["Initial", "Transition"]):
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


def material_usage_summary(
    business_case_df: pd.DataFrame, material: str, technology: str = ""
) -> Union[float, pd.DataFrame]:
    """Summaries the amount of a given material used by a certain technology.

    Args:
        business_case_df (pd.DataFrame): Standardised business cases DataFrame.
        material (str): The material that you want to summarise the material usage for.
        technology (str, optional): Optional parameter to return the material value for a given technology. Defaults to "".

    Returns:
        Union[float, pd.DataFrame]: Returns a float if the `technology` value is specified, otherwise returns a DataFrame.
    """
    if technology:
        try:
            return (
                business_case_df.groupby(["material_category", "technology"])
                .sum()
                .loc[material, technology]
                .values[0]
            )
        except:
            return 0
    return (
        business_case_df.groupby(["material_category", "technology"])
        .sum()
        .loc[material]
    )


def material_usage_calc(
    plant_capacities: dict,
    steel_demand_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    materials_list: list,
    plant_name: str,
    country_code: str,
    year: float,
    tech: str,
    materials_to_check: list,
    steel_demand_scenario: str,
) -> float:
    """Calculates the amount of materials used as a factor of total production. 

    Args:
        plant_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        steel_demand_df (pd.DataFrame): Steel Demand timeseries.
        business_cases (pd.DataFrame): Standardised business cases.
        materials_list (list): List of materials you want to track material usage for
        plant_name (str): The name of the plant you want material usage for.
        country_code (str): The country code of the plant.
        year (float): The year to get material usage values for.
        tech (str): The technology you want to get material usage values for.
        materials_to_check (list): material to check -> FIX THIS not used!
        steel_demand_scenario (str): The scenario for steel demand `bau` or `high circ`.

    Returns:
        float: The material usage value to be used.
    """

    plant_capacity = (
        calculate_primary_capacity(plant_capacities, plant_name, tech) / 1000
    )
    steel_demand = steel_demand_getter(
        steel_demand_df, year, steel_demand_scenario, "crude", country_code=country_code
    )
    capacity_sum = total_plant_capacity(plant_capacities)
    projected_production = (plant_capacity / capacity_sum) * steel_demand
    material_list = []
    for material in materials_list:
        usage_value = material_usage_summary(business_cases, material, tech)
        material_list.append(usage_value)
    return projected_production * sum(material_list)


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
    available_tech_list: list = None,
    material_usage_dict: dict = None,
    output_type: str = "excluded",
) -> list:
    """Checks the amout of forecasted material usage against the amount of the resource available and assigns the technology test results to included or excluded lists as outputs.

    Args:
        plant_name (str): The plant name
        base_tech (str): The base technology
        year (int): The year to check material usage for
        steel_demand_df (pd.DataFrame): Steel Demand timeseries
        steel_plant_df (pd.DataFrame): Steel Plant DataFrame
        steel_demand_scenario (str): The scenario for steel demand `bau` or `high circ`.
        business_cases (pd.DataFrame): Standardised business cases DataFrame.
        bio_constraint_model (pd.DataFrame): The Bio Constraint DataFrame Model.
        ccs_co2_df (pd.DataFrame): The CCS CO2 Timeseries constraint DataFrame
        materials_list (list): A list of materials to run usage checks against.
        tech_material_dict (dict): The technology material dictionary
        resource_container_ref (dict): A dictionary that captures the current iteration of the resource usage.
        plant_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        available_tech_list (list, optional): The list of valid technologies to asses material usage stats for. Defaults to None.
        material_usage_dict (dict, optional): A state dictionary container used to store the latest material usage stats. Defaults to None.
        output_type (str, optional): The output type of the list generated 'excluded' only returns excluded technologies. 'included' only returns valid technologies. Defaults to "excluded".

    Returns:
        list: A list containing either the technologies that passed or failed the material usage calculations.
    """

    tech_list = SWITCH_DICT[base_tech]
    if available_tech_list:
        tech_list = available_tech_list

    tech_approved_list = []

    country_code = steel_plant_df[steel_plant_df["plant_name"] == plant_name][
        "country_code"
    ].values[0]

    for tech in tech_list:

        material_check_container = []

        for material_check in tech_material_dict[tech]:
            # Setting parameters
            if material_check == "Bioenergy":
                material_ref = "Bioenergy"
                material_capacity = bio_constraint_getter(bio_constraint_model, year)
                materials_to_check = ["Biomass", "Biomethane"]

            if material_check == "Used CO2":
                material_ref = "Used CO2"
                material_capacity = ccs_co2_getter(
                    ccs_co2_df, "Steel CO2 use market", year
                )
                materials_to_check = ["Used CO2"]

            if material_check == "Captured CO2":
                material_ref = "Captured CO2"
                material_capacity = ccs_co2_getter(
                    ccs_co2_df, "Total Steel CCS capacity", year
                )
                materials_to_check = ["Captured CO2"]

            if material_check in ["Scrap", "Scrap EAF"]:
                material_ref = "Scrap"
                material_capacity = steel_demand_getter(
                    steel_demand_df, year, steel_demand_scenario, "scrap", country_code=country_code
                )
                materials_to_check = ["Scrap"]

            # Checking for zero
            if material_capacity <= 0:
                # logger.info(f'{year} -> Material {material_check} is not available, capacity = 0')
                material_check_container.append(False)
            else:
                # Core logic
                material_container = material_usage_dict[
                    resource_container_ref[material_ref]
                ]
                if material_check in ["Bioenergy", "Used CO2", "Captured CO2", "Scrap"]:
                    current_usage = sum(material_container)
                    if current_usage == 0:
                        # logger.info('First usage for {material_check}')
                        pass
                    resource_remaining = material_capacity - current_usage
                    plant_usage = material_usage_calc(
                        plant_capacities,
                        steel_demand_df,
                        business_cases,
                        materials_list,
                        plant_name,
                        country_code,
                        year,
                        tech,
                        materials_to_check,
                        steel_demand_scenario,
                    )
                    if plant_usage > resource_remaining:
                        # print(f'{year} -> {plant_name} cannot adopt {tech} because usage of {material_check} exceeds capacity | uses {plant_usage} of remaining {resource_remaining}')
                        material_check_container.append(False)
                    else:
                        # print(f'{year} -> {plant_name} can adopt {tech} because usage of {material_check} does not exceed capacity | uses {plant_usage} of remaining {resource_remaining}')
                        material_check_container.append(True)
                        material_container.append(plant_usage)

                if material_check in ["Scrap EAF"]:
                    if material_capacity > 1.5:
                        # print(f'Sufficient enough scrap for {tech} -> {material_capacity}')
                        material_check_container.append(True)
                    else:
                        # print(f'Not enough scrap for {tech}')
                        material_check_container.append(False)

        if all(material_check_container):
            # logger.info(f'PASSED: {tech} has passed availability checks for {plant_name}')
            tech_approved_list.append(tech)
        else:
            if tech == base_tech:
                # logger.info(f'PASSED: {tech} is the same as based tech, but would have failed otherwise')
                tech_approved_list.append(tech)
                # material_container.append(plant_usage)
            else:
                pass
                # logger.info(f'FAILED: {tech} has NOT passed availability checks for {plant_name}')

    unavailable_techs = list(set(tech_list).difference(set(tech_approved_list)))
    # Final check and return

    if output_type == "excluded":
        return unavailable_techs
    if output_type == "included":
        return tech_approved_list


def create_new_material_usage_dict(resource_container_ref: dict) -> dict:
    """Creates a new empty material usage dictionary for use in a new year.

    Args:
        resource_container_ref (dict): A dictionary with values as resource names that are the materials you want to track usage for.

    Returns:
        dict: A dictionary with material name: empty list key: value pairs.
    """
    return {material_key: [] for material_key in resource_container_ref.values()}


def material_usage_per_plant(
    plant_list: list,
    technology_list: list,
    business_cases: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    plant_capacities: dict,
    steel_demand_df: pd.DataFrame,
    materials_list: list,
    year: float,
    steel_demand_scenario: str,
) -> pd.DataFrame:
    """Creates a reference to the total material usage per plant. 

    Args:
        plant_list (list): The list of plants that you want to check material usage for. 
        technology_list (list): The list of technologies that you want to check material usage for.
        business_cases (pd.DataFrame): Standardised business cases DataFrame.
        steel_plant_df (pd.DataFrame): Steel Plant DataFrame.
        plant_capacities (dict): A dictionary containing plant: capacity/inital tech key:value pairs.
        steel_demand_df (pd.DataFrame): Steel Demand DataFrame.
        materials_list (list): Materials that you want to check usage for.
        year (float): The year you want to check material usage for.
        steel_demand_scenario (str): The scenario for steel demand `bau` or `high circ`.

    Returns:
        pd.DataFrame: Returns a material usage DataFrame split on a plant level.
    """
    df_list = []
    zipped_data = zip(plant_list, technology_list)
    capacity_sum = total_plant_capacity(plant_capacities)
    for plant_name, tech in zipped_data:
        plant_capacity = (
            calculate_primary_capacity(plant_capacities, plant_name, tech) / 1000
        )
        plant_country = steel_plant_df[steel_plant_df["plant_name"] == plant_name][
            "country_code"
        ].values[0]
        steel_demand = steel_demand_getter(
            steel_demand_df, year, steel_demand_scenario, "crude", country_code=plant_country
        )
        projected_production = (plant_capacity / capacity_sum) * steel_demand
        df = pd.DataFrame(index=materials_list, columns=["value"])
        for material in materials_list:
            usage_value = material_usage_summary(business_cases, material, tech)
            try:
                df.loc[material, "value"] = projected_production * usage_value
            except ValueError:
                raise ValueError(f'material: {material} | plant: {plant_name} | tech: {tech} | production: {projected_production}')
        df_list.append(df)
    return pd.concat(df_list).reset_index().groupby(["index"]).sum()


def load_resource_usage_dict(yearly_usage_df: pd.DataFrame) -> dict:
    """Returns a resource usage dictionary.

    Args:
        yearly_usage_df (pd.DataFrame): A DataFrame based on the years usage of materials.

    Returns:
        dict: Returns a preloaded resource usage dictionary.
    """
    resource_usage_dict = create_new_material_usage_dict(RESOURCE_CONTAINER_REF)
    resource_usage_dict["biomass"] = list(
        {yearly_usage_df.loc["Biomass"]["value"] or 0}
    )
    resource_usage_dict["scrap"] = list({yearly_usage_df.loc["Scrap"]["value"] or 0})
    resource_usage_dict["used_co2"] = list(
        {yearly_usage_df.loc["Used CO2"]["value"] or 0}
    )
    resource_usage_dict["captured_co2"] = list(
        {yearly_usage_df.loc["Captured CO2"]["value"] or 0}
    )
    return resource_usage_dict
