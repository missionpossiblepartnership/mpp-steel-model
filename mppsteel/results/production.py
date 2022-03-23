"""Production Results generator for technology investments"""

import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    MODEL_YEAR_END,
    PKL_DATA_INTERMEDIATE,
    PKL_DATA_FINAL,
)

from mppsteel.config.reference_lists import LOW_CARBON_TECHS

from mppsteel.model.solver import (
    load_materials,
    load_business_cases,
)
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.data_loading.steel_plant_formatter import map_plant_id_to_df
from mppsteel.data_loading.data_interface import load_business_cases
from mppsteel.model.emissions_reference_tables import emissivity_getter
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.location_utility import get_region_from_country_code
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Production Results")


def generate_production_stats(
    tech_capacity_df: pd.DataFrame,
    capacity_results: pd.DataFrame,
    steel_df: pd.DataFrame,
    country_reference_dict: dict,
    steel_demand_scenario: str,
    year_end: int,
) -> pd.DataFrame:
    """Creates new columns for production, capacity_utilisation and a check for whether the technology is a low carbon tech.

    Args:
        tech_capacity_df (pd.DataFrame): A DataFrame containing the capacities of each steel plant
        steel_df (pd.DataFrame): A DataFrame containing the steel demand data.
        steel_demand_scenario (str): The secnario for the steel demand.
        year_end (int): The year that the model ends.

    Returns:
        pd.DataFrame: A DataFrame containing the new columns: produciton, capacity_utilization, and low_carbon_tech
    """
    logger.info("- Generating Production Results from capacity")
    df_list = []
    year_range = range(MODEL_YEAR_START, year_end + 1)
    tech_capacity_df["low_carbon_tech"] = tech_capacity_df["technology"].apply(
        lambda tech: "Y" if tech in LOW_CARBON_TECHS else "N"
    )
    tech_capacity_df["region"] = tech_capacity_df["country_code"].apply(
        lambda x: get_region_from_country_code(x, "rmi_region", country_reference_dict)
    )
    regions = tech_capacity_df['region'].unique()
    for year in tqdm(year_range, total=len(year_range), desc="Production Stats"):
        df = tech_capacity_df[tech_capacity_df["year"] == year].copy()
        # Regional production split
        for region in regions:
            regional_steel_demand = steel_demand_getter(steel_df, year, steel_demand_scenario, "crude", region=region)
            regional_capacity = capacity_results[str(year)][region]
            capacity_utilization_factor = regional_steel_demand / regional_capacity
            df_r = df[df["region"] == region].copy()
            df_r['production'] = df_r["capacity"].apply(lambda capacity: capacity * capacity_utilization_factor) # This calculates production!!!
            df_r["capacity_utilization"] = capacity_utilization_factor
            df_list.append(df_r)
    return pd.concat(df_list).reset_index(drop=True)


def tech_capacity_splits(steel_plants: pd.DataFrame, tech_choices: dict) -> pd.DataFrame:
    """Create a DataFrame containing the technologies and capacities for every plant in every year.

    Returns:
        pd.DataFrame: The combined DataFrame
    """
    logger.info("- Generating Capacity split DataFrame")
    capacities_dict = dict(zip(steel_plants['plant_name'], steel_plants['combined_capacity']))
    steel_plant_dict = dict(
        zip(steel_plants["plant_name"].values, steel_plants["country_code"].values)
    )
    max_year = max([int(year) for year in tech_choices])
    steel_plants = capacities_dict.keys()
    year_range = range(MODEL_YEAR_START, max_year + 1)
    df_list = []

    for year in tqdm(year_range, total=len(year_range), desc="Tech Capacity Splits"):
        df = pd.DataFrame(
            {"year": year, "plant_name": steel_plants, "technology": "", "capacity": 0}
        )
        df["technology"] = df["plant_name"].apply(
            lambda plant: get_tech_choice(tech_choices, year, plant)
        )
        df['capacity'] = df['plant_name'].apply(lambda plant_name: capacities_dict[plant_name] / 1000)
        df_list.append(df)

    df_combined = pd.concat(df_list)
    df_combined = map_plant_id_to_df(df_combined, "plant_name")
    df_combined["country_code"] = df["plant_name"].apply(
        lambda plant: steel_plant_dict[plant]
    )
    return df_combined


def production_stats_generator(
    production_df: pd.DataFrame, as_summary: bool = False
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

    material_dict_mapper = load_materials_mapper()
    standardised_business_cases = load_business_cases()

    # Create columns
    for colname in material_dict_mapper.values():
        df_c[colname] = 0

    # Create values
    for row in tqdm(
        df_c.itertuples(), total=df_c.shape[0], desc="Production Stats Generator"
    ):
        for item in material_dict_mapper.items():
            material_category = item[0]
            new_colname = item[1]
            if material_category == "BF slag":
                df_c.loc[row.Index, new_colname] = (
                    row.production
                    * business_case_getter(
                        standardised_business_cases, row.technology, material_category
                    )
                    / 1000
                )
            elif material_category == "Met coal":
                df_c.loc[row.Index, new_colname] = (
                    row.production
                    * business_case_getter(
                        standardised_business_cases, row.technology, material_category
                    )
                    * 28
                )
            elif material_category == "Hydrogen":
                df_c.loc[row.Index, new_colname] = (
                    row.production
                    * business_case_getter(
                        standardised_business_cases, row.technology, material_category
                    )
                    
                )
            else:
                df_c.loc[
                    row.Index, new_colname
                ] = row.production * business_case_getter(
                    standardised_business_cases, row.technology, material_category
                )

    df_c["bioenergy"] = df_c["biomass"] + df_c["biomethane"]
    df_c["coal"] = df_c["met_coal"] + df_c["thermal_coal"]
    if as_summary:
        return df_c.groupby(["year", "technology"]).sum()

    # Convert Hydrogen from Twh to Pj
    for material in ["hydrogen"]:
        df_c[material] = df_c[material] * 3.6

    return df_c


def generate_production_emission_stats(
    production_df: pd.DataFrame, emissions_df: pd.DataFrame, as_summary: bool = False
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

    # Create columns
    for colname in emissions_name_ref:
        df_c[f"{colname}_emissions"] = 0

        # Create values
    for row in tqdm(
        df_c.itertuples(), total=df_c.shape[0], desc="Production Emissions"
    ):
        if row.technology == "Close plant":
            for colname in emissions_name_ref:
                df_c.loc[row.Index, f"{colname}_emissions"] = 0
        else:
            for colname in emissions_name_ref:
                df_c.loc[
                    row.Index, f"{colname}_emissions"
                ] = row.production * emissivity_getter(
                    calculated_emissivity_combined,
                    row.year,
                    row.country_code,
                    row.technology,
                    colname,
                )

    if as_summary:
        return df_c.groupby(["year", "technology"]).sum()
    return df_c


def business_case_getter(df: pd.DataFrame, tech: str, material: str) -> float:
    """Get business case usage values from a DataFrame.

    Args:
        df (pd.DataFrame): The standardised and summarised business cases.
        tech (str): The technology that you want to get values for.
        material (str): The material that you want to get values for.

    Returns:
        float: The business case value that requested via the function arguments.
    """
    if material in df[(df["technology"] == tech)]["material_category"].unique():
        return df[(df["technology"] == tech) & (df["material_category"] == material)][
            "value"
        ].values
    return 0


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


def load_materials_mapper() -> dict:
    """A mapper for material names to material names to be used as dataframe column references.

    Returns:
        dict: A dictionary containing a mapping of original material names to column reference material names.
    """
    materials = load_materials()
    material_col_names = [material.lower().replace(" ", "_") for material in materials]
    return dict(zip(materials, material_col_names))


@timer_func
def production_results_flow(scenario_dict: dict, serialize: bool = False) -> dict:
    """Production results flow to create the Production resource usage DataFrame and the Production Emissions DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary containing the two DataFrames.
    """
    logger.info("- Starting Production Results Model Flow")
    steel_demand_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted", "df"
    )
    plant_result_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "plant_result_df", "df"
    )
    tech_choices_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "tech_choice_dict", "dict"
    )
    capacity_results = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "capacity_results", "dict"
    )
    country_reference_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    calculated_emissivity_combined = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "calculated_emissivity_combined", "df"
    )
    tech_capacity_df = tech_capacity_splits(plant_result_df, tech_choices_dict)
    steel_demand_scenario = scenario_dict["steel_demand_scenario"]
    production_results = generate_production_stats(
        tech_capacity_df, capacity_results, steel_demand_df, 
        country_reference_dict, steel_demand_scenario, MODEL_YEAR_END
    )
    production_resource_usage = production_stats_generator(production_results)
    production_emissions = generate_production_emission_stats(production_results, calculated_emissivity_combined)
    results_dict = {
        "production_resource_usage": production_resource_usage,
        "production_emissions": production_emissions,
    }

    for key in results_dict:
        if key in ["production_resource_usage", "production_emissions"]:
            results_dict[key] = add_results_metadata(
                results_dict[key], scenario_dict, single_line=True
            )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(
            results_dict["production_resource_usage"],
            PKL_DATA_FINAL,
            "production_resource_usage",
        )
        serialize_file(
            results_dict["production_emissions"], PKL_DATA_FINAL, "production_emissions"
        )
    return results_dict
