"""Production Results generator for technology investments"""

import itertools
import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    BIOMASS_ENERGY_DENSITY_GJ_PER_TON,
    MODEL_YEAR_START,
    MODEL_YEAR_END,
    GIGATON_TO_MEGATON_FACTOR,
    PKL_DATA_FORMATTED,
    TON_TO_KILOGRAM_FACTOR,
    MET_COAL_ENERGY_DENSITY_MJ_PER_KG,
)

from mppsteel.config.reference_lists import LOW_CARBON_TECHS, TECH_REFERENCE_LIST

from mppsteel.data_loading.steel_plant_formatter import map_plant_id_to_df
from mppsteel.model.emissions_reference_tables import emissivity_getter
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def generate_production_stats(
    tech_capacity_df: pd.DataFrame,
    utilization_results: dict,
    country_mapper: dict
) -> pd.DataFrame:
    """Creates new columns for production, capacity_utilisation and a check for whether the technology is a low carbon tech.

    Args:
        tech_capacity_df (pd.DataFrame): A DataFrame containing the capacities of each steel plant

    Returns:
        pd.DataFrame: A DataFrame containing the new columns: produciton, capacity_utilization, and low_carbon_tech
    """
    logger.info("- Generating Production Results from capacity")

    def utilization_mapper(row):
        return utilization_results[row.year][row.region]

    def production_mapper(row):
        return row.capacity * row.capacity_utilization

    tech_capacity_df["low_carbon_tech"] = tech_capacity_df["technology"].apply(
        lambda tech: "Y" if tech in LOW_CARBON_TECHS else "N"
    )
    tech_capacity_df['region'] = tech_capacity_df["country_code"].apply(
        lambda x: country_mapper[x])
    tech_capacity_df["capacity_utilization"] = tech_capacity_df.apply(utilization_mapper, axis=1)
    tech_capacity_df["production"] = tech_capacity_df.apply(production_mapper, axis=1)
    return tech_capacity_df


def tech_capacity_splits(steel_plants: pd.DataFrame, tech_choices: dict) -> pd.DataFrame:
    """Create a DataFrame containing the technologies and capacities for every plant in every year.

    Returns:
        pd.DataFrame: The combined DataFrame
    """
    logger.info("- Generating Capacity split DataFrame")
    capacities_dict = dict(zip(steel_plants['plant_name'], steel_plants['plant_capacity']))
    steel_plant_dict = dict(
        zip(steel_plants["plant_name"].values, steel_plants["country_code"].values)
    )
    steel_plant_names = capacities_dict.keys()
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    df_list = []

    for year in tqdm(year_range, total=len(year_range), desc="Tech Capacity Splits"):
        df = pd.DataFrame({"year": year, "plant_name": steel_plant_names})
        df["technology"] = df["plant_name"].apply(lambda plant: get_tech_choice(tech_choices, year, plant))
        df = df[df['technology'] != '']
        df['capacity'] = df['plant_name'].apply(lambda plant_name: capacities_dict[plant_name] / GIGATON_TO_MEGATON_FACTOR)
        df_list.append(df)

    df_combined = pd.concat(df_list)
    df_combined = map_plant_id_to_df(df_combined, steel_plants, "plant_name")
    df_combined["country_code"] = df["plant_name"].apply(
        lambda plant: steel_plant_dict[plant]
    )
    return df_combined


def production_stats_rules(row, business_case_ref: dict, material_category: str):
    value = row.production * business_case_ref[(row.technology, material_category)]
    if material_category in {"BF slag", "Other slag"}:
        return value / TON_TO_KILOGRAM_FACTOR
    
    elif material_category == "Met coal":
        return value * MET_COAL_ENERGY_DENSITY_MJ_PER_KG

    #elif material_category in {"Biomass", "Biomethane"}:
    #    return value * BIOMASS_ENERGY_DENSITY_GJ_PER_TON
    
    return value

def production_stats_generator(
    production_df: pd.DataFrame, materials_list: list, as_summary: bool = False
) -> pd.DataFrame:
    """Generate the consumption of resources for each plant in each year depending on the technologies used.

    Args:
        production_df (pd.DataFrame): A DataFrame containing the production stats for each plant in each year.
        as_summary (bool, optional): A boolean flag that will aggregate the results if set to True. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with each resource usage stat included as a column.
    """
    logger.info(f"- Generating Production Stats")
    df_c = production_df.copy()
    material_dict_mapper = load_materials_mapper(materials_list)
    inverse_material_dict_mapper = load_materials_mapper(materials_list, reverse=True)
    new_materials = material_dict_mapper.values()
    business_case_ref = read_pickle_folder(PKL_DATA_FORMATTED, "business_case_reference", "df")

    # Create columns
    for new_material_name in tqdm(new_materials, total=len(new_materials), desc='Material Loop'):
        df_c[new_material_name] = df_c.apply(
            production_stats_rules,
            business_case_ref=business_case_ref,
            material_category=inverse_material_dict_mapper[new_material_name],
            axis=1
        )

    df_c["bioenergy"] = df_c["biomass"] + df_c["biomethane"]
    df_c["coal"] = df_c["met_coal"] + df_c["thermal_coal"]
    if as_summary:
        return df_c.groupby(["year", "technology"]).sum()

    return df_c


def generate_production_emission_stats(
    production_df: pd.DataFrame, emissions_df: pd.DataFrame, steel_plants: pd.DataFrame, as_summary: bool = False
) -> pd.DataFrame:
    """Generates a DataFrame with the emissions generated for S1, S2 & S3.

    Args:
        production_df (pd.DataFrame): A DataFrame containing the production stats for each plant in each year.
        as_summary (bool, optional): A boolean flag that will aggregate the results if set to True. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with each emission scope included as a column.
    """
    logger.info("- Generating Production Emission Stats")
    calculated_emissivity_combined = (
        emissions_df.reset_index().set_index(
            ["year", "country_code", "technology"]
        )
    )
    emissions_name_ref = ["s1", "s2", "s3"]

    df_c = production_df.copy()
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    country_codes = steel_plants['country_code'].unique()
    full_product_ref = list(itertools.product(year_range, country_codes, TECH_REFERENCE_LIST))
    
    s1_value_ref = {}
    s2_value_ref = {}
    s3_value_ref = {}
    for year, country_code, technology in tqdm(full_product_ref, total=len(full_product_ref), desc='Business Case Reference Loop'):
        s1_value = emissivity_getter(calculated_emissivity_combined, year, country_code, technology, 's1')
        s2_value = emissivity_getter(calculated_emissivity_combined, year, country_code, technology, 's2')
        s3_value = emissivity_getter(calculated_emissivity_combined, year, country_code, technology, 's3')
        s1_value_ref[(year, country_code, technology)] = s1_value
        s2_value_ref[(year, country_code, technology)] = s2_value
        s3_value_ref[(year, country_code, technology)] = s3_value

    emission_dict = {
        's1': s1_value_ref,
        's2': s2_value_ref,
        's3': s3_value_ref
    }

    def emissions_mapper(row, emission_ref: dict):
        if (row.technology == "Close plant") or not row.technology:
            return 0
        return row.production * emission_ref[(row.year, row.country_code, row.technology)]

    for emission_type in emissions_name_ref:
        df_c[f"{emission_type}_emissions"] = df_c.apply(
            emissions_mapper,
            emission_ref=emission_dict[emission_type],
            axis=1
        )

    if as_summary:
        return df_c.groupby(["year", "technology"]).sum()
    return df_c


def get_tech_choice(tc_dict: dict, year: int, plant_name: str) -> str:
    """Return a technology choice for a given plant in a given year.

    Args:
        tc_dict (dict): Dictionary containing all technology choices for every plant across every year.
        year (int): The year you want the technology choice for.
        plant_name (str): The name of the plant

    Returns:
        str: The technology choice requested via the function arguments.
    """
    if plant_name in tc_dict[str(year)]:
        return tc_dict[str(year)][plant_name]
    return ''


def load_materials_mapper(materials_list: list, reverse: bool = False) -> dict:
    """A mapper for material names to material names to be used as dataframe column references.

    Returns:
        dict: A dictionary containing a mapping of original material names to column reference material names.
    """
    material_col_names = [material.lower().replace(" ", "_") for material in materials_list]
    dict_obj = dict(zip(materials_list, material_col_names))
    if reverse:
        return {v: k for k, v in dict_obj.items()}
    return dict_obj


def production_stats_getter(
    df: pd.DataFrame, year: int, plant_name, value_col: str
) -> float:
    """Returns a specified stat from the Production DataFrame.

    Args:
        df (pd.DataFrame): A DataFrame of the Production Statistics containing resource usage.
        year (int): The year that you want to reference.
        plant_name (_type_): The name of the reference plant.
        value_col (str): The column containing the value you want to reference.

    Returns:
        float: The value of the value_col passed as a function argument.
    """
    return df.xs((year, plant_name))[value_col]


@timer_func
def production_results_flow(scenario_dict: dict, serialize: bool = False) -> dict:
    """Production results flow to create the Production resource usage DataFrame and the Production Emissions DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary containing the two DataFrames.
    """
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    final_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'final')
    logger.info("- Starting Production Results Model Flow")
    plant_result_df = read_pickle_folder(
        intermediate_path, "plant_result_df", "df"
    )
    tech_choices_dict = read_pickle_folder(
        intermediate_path, "tech_choice_dict", "dict"
    )
    utilization_results = read_pickle_folder(
        intermediate_path, "utilization_results", "dict"
    )
    rmi_mapper = create_country_mapper()
    calculated_emissivity_combined = read_pickle_folder(
        intermediate_path, "calculated_emissivity_combined", "df"
    )
    tech_capacity_df = tech_capacity_splits(plant_result_df, tech_choices_dict)
    production_results = generate_production_stats(
        tech_capacity_df, utilization_results, rmi_mapper,
    )
    business_cases = read_pickle_folder(
        PKL_DATA_FORMATTED, "standardised_business_cases", "df"
    )
    materials_list = business_cases.index.get_level_values(1).unique()
    production_resource_usage = production_stats_generator(production_results, materials_list)
    production_emissions = generate_production_emission_stats(production_results, calculated_emissivity_combined, plant_result_df)
    results_dict = {
        "production_resource_usage": production_resource_usage,
        "production_emissions": production_emissions,
    }

    for key in results_dict:
        if key in ["production_resource_usage", "production_emissions"]:
            results_dict[key] = add_results_metadata(
                results_dict[key], scenario_dict, single_line=True, scenario_name=True
            )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(
            results_dict["production_resource_usage"],
            final_path,
            "production_resource_usage",
        )
        serialize_file(
            results_dict["production_emissions"],
            final_path,
            "production_emissions"
        )
    return results_dict
