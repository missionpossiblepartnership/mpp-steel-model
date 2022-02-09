"""Production Results generator for technology investments"""

from typing import Union, Tuple

import pandas as pd
from tqdm import tqdm

from mppsteel.model_config import (
    AVERAGE_LEVEL_OF_CAPACITY,
    MODEL_YEAR_START,
    MODEL_YEAR_END,
    PKL_DATA_INTERMEDIATE,
    PKL_DATA_FINAL,
)

from mppsteel.utility.reference_lists import LOW_CARBON_TECHS

from mppsteel.model.solver import (
    load_materials,
    load_business_cases,
    create_plant_capacities_dict,
)

from mppsteel.model.solver_constraints import calculate_primary_and_secondary

from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.data_loading.steel_plant_formatter import map_plant_id_to_df

from mppsteel.data_loading.data_interface import load_business_cases

from mppsteel.model.emissions_reference_tables import emissivity_getter

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file

from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Production Results")


def generate_production_stats(
    tech_capacity_df: pd.DataFrame,
    steel_df: pd.DataFrame,
    steel_demand_scenario: str,
    year_end: int,
) -> pd.DataFrame:
    """[summary]

    Args:
        tech_capacity_df (pd.DataFrame): [description]
        steel_df (pd.DataFrame): [description]
        year_end (int): [description]

    Returns:
        [type]: [description]
    """
    logger.info(f"- Generating Production Results from capacity")
    df_list = []
    year_range = range(MODEL_YEAR_START, year_end + 1)
    for year in tqdm(year_range, total=len(year_range), desc="Production Stats"):
        df = tech_capacity_df[tech_capacity_df["year"] == year].copy()
        capacity_sum = df["capacity"].sum()
        steel_demand = steel_demand_getter(
            steel_df, year, steel_demand_scenario, "crude", "World"
        )
        # This calculates production!!!
        df["production"] = (df["capacity"] / capacity_sum) * steel_demand
        df["capacity_utilization"] = (steel_demand / 1000) / df["capacity"]
        df["low_carbon_tech"] = df["technology"].apply(
            lambda tech: "Y" if tech in LOW_CARBON_TECHS else "N"
        )
        df_list.append(df)
    return pd.concat(df_list).reset_index(drop=True)


def tech_capacity_splits() -> Tuple[pd.DataFrame, int]:
    """[summary]

    Returns:
        [type]: [description]
    """
    logger.info(f"- Generating Capacity split DataFrame")
    tech_capacities_dict = create_plant_capacities_dict()
    tech_choices_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "tech_choice_dict", "df"
    )
    steel_plant_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
    )
    steel_plant_dict = dict(
        zip(steel_plant_df["plant_name"].values, steel_plant_df["country_code"].values)
    )
    max_year = max([int(year) for year in tech_choices_dict])
    steel_plants = tech_capacities_dict.keys()
    year_range = range(MODEL_YEAR_START, max_year + 1)
    df_list = []

    def value_mapper(row, enum_dict):
        row[enum_dict["capacity"]] = (
            calculate_primary_and_secondary(
                tech_capacities_dict,
                row[enum_dict["plant_name"]],
                row[enum_dict["technology"]],
            )
            / 1000
        )
        return row

    for year in tqdm(year_range, total=len(year_range), desc="Tech Capacity Splits"):
        df = pd.DataFrame(
            {"year": year, "plant_name": steel_plants, "technology": "", "capacity": 0}
        )
        df["technology"] = df["plant_name"].apply(
            lambda plant: get_tech_choice(tech_choices_dict, year, plant)
        )
        enumerated_cols = enumerate_iterable(df.columns)
        df = df.apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
        df_list.append(df)

    df_combined = pd.concat(df_list)
    df_combined = map_plant_id_to_df(df_combined, "plant_name")
    df_combined["country_code"] = df["plant_name"].apply(
        lambda plant: steel_plant_dict[plant]
    )

    return df_combined, max_year


def production_stats_generator(
    production_df: pd.DataFrame, as_summary: bool = False
) -> pd.DataFrame:
    """[summary]
    Args:
        production_df (pd.DataFrame): [description]
        as_summary (bool, optional): [description]. Defaults to False.
    Returns:
        [type]: [description]
    """
    logger.info(f"- Generating Production Stats")
    df_c = production_df.copy()

    material_dict_mapper = load_materials_mapper()
    standardised_business_cases = load_business_cases()

    # Create columns
    for colname in material_dict_mapper.values():
        df_c[colname] = 0
    # df_c['power'] = 0

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
                    / 3.6
                )
            else:
                df_c.loc[
                    row.Index, new_colname
                ] = row.production * business_case_getter(
                    standardised_business_cases, row.technology, material_category
                )

        # Create power column
        # electricity_value = business_case_getter(standardised_business_cases, row.technology, 'Electricity')
        # df_c.loc[row.Index, 'power'] = row.production * electricity_value / 3.6

    df_c["bioenergy"] = df_c["biomass"] + df_c["biomethane"]
    if as_summary:
        return df_c.groupby(["year", "technology"]).sum()

    # Convert Electricity & Hydrogen from Twh to Pj
    for material in ["electricity", "hydrogen"]:
        df_c[material] = df_c[material] / 3.6

    return df_c


def generate_production_emission_stats(
    production_df: pd.DataFrame, as_summary: bool = False
) -> pd.DataFrame:
    """[summary]
    Args:
        production_df (pd.DataFrame): [description]
        as_summary (bool, optional): [description]. Defaults to False.
    Returns:
        [type]: [description]
    """
    logger.info(f"- Generating Production Emission Stats")
    calculated_emissivity_combined = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "calculated_emissivity_combined", "df"
    )
    calculated_emissivity_combined = (
        calculated_emissivity_combined.reset_index().set_index(
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
    if material in df[(df["technology"] == tech)]["material_category"].unique():
        return df[(df["technology"] == tech) & (df["material_category"] == material)][
            "value"
        ].values
    return 0


def get_tech_choice(tc_dict: dict, year: int, plant_name: str) -> str:
    return tc_dict[str(year)][plant_name]


def load_materials_mapper() -> dict:
    """[summary]

    Returns:
        [type]: [description]
    """
    materials = load_materials()
    material_col_names = [material.lower().replace(" ", "_") for material in materials]
    return dict(zip(materials, material_col_names))


@timer_func
def production_results_flow(scenario_dict: dict, serialize: bool = False) -> dict:
    """[summary]

    Args:
        serialize (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """
    logger.info(f"- Starting Production Results Model Flow")
    steel_demand_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted", "df"
    )
    tech_capacity_df, max_solver_year = tech_capacity_splits()
    steel_demand_scenario = scenario_dict["steel_demand_scenario"]
    production_results = generate_production_stats(
        tech_capacity_df, steel_demand_df, steel_demand_scenario, max_solver_year
    )
    production_resource_usage = production_stats_generator(production_results)
    production_emissions = generate_production_emission_stats(production_results)
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
        logger.info(f"-- Serializing dataframes")
        serialize_file(
            results_dict["production_resource_usage"],
            PKL_DATA_FINAL,
            "production_resource_usage",
        )
        serialize_file(
            results_dict["production_emissions"], PKL_DATA_FINAL, "production_emissions"
        )
    return results_dict
