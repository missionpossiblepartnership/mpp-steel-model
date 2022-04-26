"""Production Results generator for technology investments"""

import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    GIGAJOULE_TO_MEGAJOULE_FACTOR,
    GIGATON_TO_MEGATON_FACTOR,
    MEGATON_TO_TON,
    MODEL_YEAR_RANGE,
    PETAJOULE_TO_TERAJOULE,
    PKL_DATA_FORMATTED,
    MET_COAL_ENERGY_DENSITY_MJ_PER_KG,
    TON_TO_KILOGRAM_FACTOR
)

from mppsteel.config.reference_lists import (
    GJ_RESOURCES, LOW_CARBON_TECHS
)

from mppsteel.data_load_and_format.steel_plant_formatter import map_plant_id_to_df
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def tech_capacity_splits(
    steel_plants: pd.DataFrame,
    tech_choices: dict,
    capacity_dict: dict,
    active_plant_checker_dict: dict,
    plant_country_code_mapper: dict,
) -> pd.DataFrame:
    """Create a DataFrame containing the technologies and capacities for every plant in every year.

    Returns:
        pd.DataFrame: The combined DataFrame
    """
    logger.info("- Generating Capacity split DataFrame")

    df_list = []

    for year in tqdm(MODEL_YEAR_RANGE, total=len(MODEL_YEAR_RANGE), desc="Tech Capacity Splits"):
        steel_plant_names = capacity_dict[year].keys()
        df = pd.DataFrame({"year": year, "plant_name": steel_plant_names})
        df["technology"] = df["plant_name"].apply(lambda plant: get_tech_choice(tech_choices, active_plant_checker_dict, year, plant))
        df['capacity'] = df['plant_name'].apply(lambda plant_name: get_capacity(capacity_dict, active_plant_checker_dict, year, plant_name))
        df = df[df['technology'] != '']
        df_list.append(df)

    df_combined = pd.concat(df_list)
    df_combined = map_plant_id_to_df(df_combined, steel_plants, "plant_name")
    df_combined["country_code"] = df["plant_name"].apply(
        lambda plant: plant_country_code_mapper[plant]
    )
    return df_combined


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
        return 0 if row.technology == 'Close plant' else utilization_results[row.year][row.region]

    def production_mapper(row):
        return 0 if row.technology == 'Close plant' else row.capacity * row.capacity_utilization

    tech_capacity_df["low_carbon_tech"] = tech_capacity_df["technology"].apply(
        lambda tech: "Y" if tech in LOW_CARBON_TECHS else "N"
    )
    tech_capacity_df['region'] = tech_capacity_df["country_code"].apply(
        lambda x: country_mapper[x])
    tech_capacity_df["capacity_utilization"] = tech_capacity_df.apply(utilization_mapper, axis=1)
    tech_capacity_df["production"] = tech_capacity_df.apply(production_mapper, axis=1)
    return tech_capacity_df


def production_material_usage(row, business_case_ref: dict, material_category: str):
    # Production is in Mt, material usage is t/t, energy usage is GJ/t
    # Transform to t usage for materials and GJ usage for energy
    return (row.production * MEGATON_TO_TON) * business_case_ref[(row.technology, material_category)]

def generate_unit_cols(df: pd.DataFrame, replace_dict: dict, conversion: float):
    df_c = df.copy()
    for item in replace_dict.items():
        old_cols = [col for col in df_c if item[0] in col[-4:]]
        new_cols = [col.replace(item[0], item[1]) for col in old_cols]
        for new_col, old_col in zip(new_cols, old_cols):
            df_c[new_col] = df_c[old_col].astype(float) * conversion
    return df_c

def production_stats_generator(
    production_df: pd.DataFrame, materials_list: list) -> pd.DataFrame:
    """Generate the consumption of resources for each plant in each year depending on the technologies used.

    Args:
        production_df (pd.DataFrame): A DataFrame containing the production stats for each plant in each year.

    Returns:
        pd.DataFrame: A DataFrame with each resource usage stat included as a column.
    """
    logger.info(f"- Generating Production Stats")
    df_c = production_df.copy()
    inverse_material_dict_mapper = load_materials_mapper(materials_list, reverse=True)
    new_materials = inverse_material_dict_mapper.keys()
    business_case_ref = read_pickle_folder(PKL_DATA_FORMATTED, "business_case_reference", "df")

    # Create columns
    for new_material_name in tqdm(new_materials, total=len(new_materials), desc='Material Loop'):
        df_c[new_material_name] = df_c.apply(
            production_material_usage,
            business_case_ref=business_case_ref,
            material_category=inverse_material_dict_mapper[new_material_name],
            axis=1
        )
    df_c["bioenergy_gj"] = df_c["biomass_gj"] + df_c["biomethane_gj"]
    df_c["met_coal_gj"] = df_c["met_coal_t"] * (MET_COAL_ENERGY_DENSITY_MJ_PER_KG * TON_TO_KILOGRAM_FACTOR) / GIGAJOULE_TO_MEGAJOULE_FACTOR
    df_c["coal_gj"] = df_c["met_coal_gj"] + df_c["thermal_coal_gj"]

    df_c = generate_unit_cols(df_c, {'_gj': '_pj'}, 1/PETAJOULE_TO_TERAJOULE)
    df_c = generate_unit_cols(df_c, {'_t': '_mt'}, 1/MEGATON_TO_TON)
    return df_c


def generate_production_emission_stats(
    production_df: pd.DataFrame, emissions_df: pd.DataFrame, carbon_tax_timeseries: pd.DataFrame) -> pd.DataFrame:
    """Generates a DataFrame with the emissions generated for S1, S2 & S3.

    Args:
        production_df (pd.DataFrame): A DataFrame containing the production stats for each plant in each year.

    Returns:
        pd.DataFrame: A DataFrame with each emission scope included as a column.
    """
    logger.info("- Generating Production Emission Stats")


    df_c = production_df.copy()

    calculated_emissivity_combined_dict = (
        emissions_df.reset_index(drop=True).set_index(
            ["year", "country_code", "technology"]
        ).to_dict()
    )

    emissivity_dict = {
        's1': calculated_emissivity_combined_dict['s1_emissivity'],
        's2': calculated_emissivity_combined_dict['s2_emissivity'],
        's3': calculated_emissivity_combined_dict['s3_emissivity']
    }

    def emissions_mapper(row, emissivity_ref: dict):
        return 0 if row.technology == "Close plant" else (row.production * MEGATON_TO_TON) * emissivity_ref[(row.year, row.country_code, row.technology)]

    for emission_type in emissivity_dict:
        df_c[f"{emission_type}_emissions_t"] = df_c.apply(
            emissions_mapper,
            emissivity_ref=emissivity_dict[emission_type],
            axis=1
        )
        # emissivity is tCO2 per t Steel
        # therefore get production in t units
        df_c[f"{emission_type}_emissions_mt"] = df_c[f"{emission_type}_emissions_t"] / MEGATON_TO_TON
        df_c[f"{emission_type}_emissions_gt"] = df_c[f"{emission_type}_emissions_mt"] / GIGATON_TO_MEGATON_FACTOR

        def carbon_cost_calculator(row, carbon_tax_timeseries: pd.DataFrame):
            return (row.s1_emissions_t + row.s2_emissions_t) * carbon_tax_timeseries.loc[row.year]['value']

    df_c['carbon_cost'] = df_c.apply(
        carbon_cost_calculator, carbon_tax_timeseries=carbon_tax_timeseries, axis=1)

    return df_c


def get_tech_choice(tc_dict: dict, active_plant_checker_dict: dict, year: int, plant_name: str) -> str:
    """Return a technology choice for a given plant in a given year.

    Args:
        tc_dict (dict): Dictionary containing all technology choices for every plant across every year.
        year (int): The year you want the technology choice for.
        plant_name (str): The name of the plant

    Returns:
        str: The technology choice requested via the function arguments.
    """
    return tc_dict[year][plant_name] if active_plant_checker_dict[plant_name] else ''

def get_capacity(capacity_dict: dict, active_plant_checker_dict: dict, year: int, plant_name: str) -> str:
    """Return a technology choice for a given plant in a given year.

    Args:
        tc_dict (dict): Dictionary containing all technology choices for every plant across every year.
        year (int): The year you want the technology choice for.
        plant_name (str): The name of the plant

    Returns:
        str: The technology choice requested via the function arguments.
    """
    return capacity_dict[year][plant_name] if active_plant_checker_dict[plant_name] else 0


def load_materials_mapper(materials_list: list, reverse: bool = False) -> dict:
    """A mapper for material names to material names to be used as dataframe column references.

    Returns:
        dict: A dictionary containing a mapping of original material names to column reference material names.
    """
    material_col_names = [material.lower().replace(" ", "_") for material in materials_list]
    dict_obj = dict(zip(materials_list, material_col_names))
    dict_obj = {(original_material): (f'{new_material}_gj' if original_material in GJ_RESOURCES else f'{new_material}_t') for original_material, new_material in dict_obj.items()}
    if reverse:
        return {v: k for k, v in dict_obj.items()}
    return dict_obj


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
    plant_capacity_results = read_pickle_folder(
        intermediate_path, "plant_capacity_results", "df"
    )
    business_cases = read_pickle_folder(
        PKL_DATA_FORMATTED, "standardised_business_cases", "df"
    )
    carbon_tax_timeseries = read_pickle_folder(
        intermediate_path, "carbon_tax_timeseries", "df"
    )
    active_plant_checker_dict = read_pickle_folder(
        intermediate_path, "active_plant_checker_dict", "df"
    )
    carbon_tax_timeseries.set_index("year", inplace=True)
    materials_list = business_cases.index.get_level_values(1).unique()
    plant_to_country_code_ref = dict(
        zip(plant_result_df["plant_name"].values, plant_result_df["country_code"].values)
    )
    tech_capacity_df = tech_capacity_splits(
        plant_result_df, tech_choices_dict, plant_capacity_results, active_plant_checker_dict, plant_to_country_code_ref)
    production_results = generate_production_stats(
        tech_capacity_df, utilization_results, rmi_mapper,
    )
    production_resource_usage = production_stats_generator(production_results, materials_list)
    production_emissions = generate_production_emission_stats(production_results, calculated_emissivity_combined, carbon_tax_timeseries)

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
