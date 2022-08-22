"""Production Results generator for technology investments"""

from typing import Union
import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    GIGAJOULE_TO_MEGAJOULE_FACTOR,
    GIGATON_TO_MEGATON_FACTOR,
    MEGATON_TO_TON,
    PETAJOULE_TO_GIGAJOULE,
    PKL_DATA_FORMATTED,
    MET_COAL_ENERGY_DENSITY_MJ_PER_KG,
    TON_TO_KILOGRAM_FACTOR,
)

from mppsteel.config.reference_lists import GJ_RESOURCES, TECHNOLOGY_PHASES
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    return_pkl_paths,
    serialize_file,
)
from mppsteel.model_solver.solver_summary import (
    tech_capacity_splits,
    utilization_mapper,
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.utility.log_utility import get_logger


logger = get_logger(__name__)


def production_mapper(row):
    return (
        0
        if row.technology == "Close plant"
        else row.capacity * row.capacity_utilization
    )


def generate_production_stats(
    tech_capacity_df: pd.DataFrame, utilization_results: dict, country_mapper: dict
) -> pd.DataFrame:
    """Creates new columns for production, capacity_utilisation and a check for whether the technology is a low carbon tech.

    Args:
        tech_capacity_df (pd.DataFrame): A DataFrame containing the capacities of each steel plant
        utilization_results (dict): A dictionary of region to utilization values.
        country_mapper (dict): A dictionary of country_codes to regions.

    Returns:
        pd.DataFrame: A DataFrame containing the new columns: produciton, capacity_utilization, and low_carbon_tech
    """
    logger.info("- Generating Production Results from capacity")

    tech_capacity_df["low_carbon_tech"] = tech_capacity_df["technology"].apply(
        lambda tech: "Y" if tech in TECHNOLOGY_PHASES["end_state"] else "N"
    )
    tech_capacity_df["region"] = tech_capacity_df["country_code"].apply(
        lambda x: country_mapper[x]
    )
    tech_capacity_df["capacity_utilization"] = tech_capacity_df.apply(
        utilization_mapper, utilization_results=utilization_results, axis=1
    )
    tech_capacity_df["production"] = tech_capacity_df.apply(production_mapper, axis=1)
    return tech_capacity_df


def production_material_usage(
    row: pd.Series, business_case_ref: dict, material_category: str
) -> float:
    """Returns the value of the material usage based on the type of resource in material_category the consumption rate, and the amount produced.

    Args:
        row (pd.Series): Tge row containing the production value and the technology.
        business_case_ref (dict): Reference dictionary containing the usage rate of the material_category and technology.
        material_category (str): The resource to get the material_usage for.

    Returns:
        float: The material usage value
    """
    # Production is in Mt, material usage is t/t, energy usage is GJ/t
    # Transform to t usage for materials and GJ usage for energy
    return (row.production * MEGATON_TO_TON) * business_case_ref[
        (row.technology, material_category)
    ]


def generate_unit_cols(
    df: pd.DataFrame, replace_dict: dict, conversion: float
) -> pd.DataFrame:
    """Creates a new column with the units specified in the `replace_dict` values using the `conversion` rate specifued.

    Args:
        df (pd.DataFrame): The DataFrame to modify by creating the new column.
        replace_dict (dict): A dictionary containing the old unit as a key and the new unit as a value.
        conversion (float): The conversion rate to apply to the new unit column.

    Returns:
        pd.DataFrame: A DataFrame with
    """
    df_c = df.copy()
    for item in replace_dict.items():
        old_cols = [col for col in df_c if item[0] in col[-4:]]
        new_cols = [col.replace(item[0], item[1]) for col in old_cols]
        for new_col, old_col in zip(new_cols, old_cols):
            df_c[new_col] = df_c[old_col].astype(float) * conversion
    return df_c


def production_stats_generator(
    production_df: pd.DataFrame, materials_list: list
) -> pd.DataFrame:
    """Generate the consumption of resources for each plant in each year depending on the technologies used.

    Args:
        production_df (pd.DataFrame): A DataFrame containing the production stats for each plant in each year.
        materials_list (list): A list of materials that will have columns in the production stats DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with each resource usage stat included as a column.
    """
    logger.info("- Generating Production Stats")
    df_c = production_df.copy()
    inverse_material_dict_mapper = load_materials_mapper(materials_list, reverse=True)
    new_materials = inverse_material_dict_mapper.keys()
    business_case_ref = read_pickle_folder(
        PKL_DATA_FORMATTED, "business_case_reference", "df"
    )

    # Create columns
    for new_material_name in tqdm(
        new_materials, total=len(new_materials), desc="Material Loop"
    ):
        df_c[new_material_name] = df_c.apply(
            production_material_usage,
            business_case_ref=business_case_ref,
            material_category=inverse_material_dict_mapper[new_material_name],
            axis=1,
        )
    df_c["bioenergy_gj"] = df_c["biomass_gj"] + df_c["biomethane_gj"]
    df_c["met_coal_gj"] = (
        df_c["met_coal_t"]
        * (MET_COAL_ENERGY_DENSITY_MJ_PER_KG * TON_TO_KILOGRAM_FACTOR)
        / GIGAJOULE_TO_MEGAJOULE_FACTOR
    )
    df_c["coal_gj"] = df_c["met_coal_gj"] + df_c["thermal_coal_gj"]

    df_c = generate_unit_cols(df_c, {"_gj": "_pj"}, 1 / PETAJOULE_TO_GIGAJOULE)
    df_c = generate_unit_cols(df_c, {"_t": "_mt"}, 1 / MEGATON_TO_TON)
    return df_c


def generate_production_emission_stats(
    production_df: pd.DataFrame,
    emissions_df: pd.DataFrame,
    carbon_tax_timeseries: pd.DataFrame,
) -> pd.DataFrame:
    """Generates a DataFrame with the emissions generated for S1, S2 & S3, and a carbon cost column.

    Args:
        production_df (pd.DataFrame): A DataFrame containing the production stats for each plant in each year.
        emissions_df (pd.DataFrame): A DataFrame containing the emissions for each plant in each year.
        carbon_tax_timeseries (pd.DataFrame): A DataFrame containing the carbon tax timeseries for each plant in each year.
    Returns:
        pd.DataFrame: A DataFrame with each emission scope included as a column, including the carbon cost.
    """
    logger.info("- Generating Production Emission Stats")

    df_c = production_df.copy()

    calculated_emissivity_combined_dict = (
        emissions_df.reset_index(drop=True)
        .set_index(["year", "country_code", "technology"])
        .to_dict()
    )

    emissivity_dict = {
        "s1": calculated_emissivity_combined_dict["s1_emissivity"],
        "s2": calculated_emissivity_combined_dict["s2_emissivity"],
        "s3": calculated_emissivity_combined_dict["s3_emissivity"],
    }

    def emissions_mapper(row: pd.Series, emissivity_ref: dict) -> pd.DataFrame:
        return (
            0
            if row.technology == "Close plant"
            else (row.production * MEGATON_TO_TON)
            * emissivity_ref[(row.year, row.country_code, row.technology)]
        )

    for emission_type in emissivity_dict:
        df_c[f"{emission_type}_emissions_t"] = df_c.apply(
            emissions_mapper, emissivity_ref=emissivity_dict[emission_type], axis=1
        )
        # emissivity is tCO2 per t Steel
        # therefore get production in t units
        df_c[f"{emission_type}_emissions_mt"] = (
            df_c[f"{emission_type}_emissions_t"] / MEGATON_TO_TON
        )
        df_c[f"{emission_type}_emissions_gt"] = (
            df_c[f"{emission_type}_emissions_mt"] / GIGATON_TO_MEGATON_FACTOR
        )

        def carbon_cost_calculator(
            row: pd.Series, carbon_tax_timeseries: pd.DataFrame
        ) -> float:
            return (
                row.s1_emissions_t + row.s2_emissions_t
            ) * carbon_tax_timeseries.loc[row.year]["value"]

    df_c["carbon_cost"] = df_c.apply(
        carbon_cost_calculator, carbon_tax_timeseries=carbon_tax_timeseries, axis=1
    )

    return df_c


def load_materials_mapper(materials_list: list, reverse: bool = False) -> dict:
    """A mapper for material names to material names to be used as dataframe column references.

    Returns:
        dict: A dictionary containing a mapping of original material names to column reference material names.
    """
    material_col_names = [
        material.lower().replace(" ", "_") for material in materials_list
    ]
    dict_obj = dict(zip(materials_list, material_col_names))
    dict_obj = {
        (original_material): (
            f"{new_material}_gj"
            if original_material in GJ_RESOURCES
            else f"{new_material}_t"
        )
        for original_material, new_material in dict_obj.items()
    }
    if reverse:
        return {v: k for k, v in dict_obj.items()}
    return dict_obj


@timer_func
def production_results_flow(
    scenario_dict: dict,
    pkl_paths: Union[dict, None] = None,
    serialize: bool = False,
    model_run: str = "",
) -> dict:
    """Production results flow to create the Production resource usage DataFrame and the Production Emissions DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        pkl_paths (Union[dict, None], optional): A dictionary containing custom pickle paths. Defaults to {}.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.
        model_run (str, optional): The run of the model to customize pkl folder paths. Defaults to "".

    Returns:
        dict: A dictionary containing the two DataFrames.
    """
    scenario_name = scenario_dict["scenario_name"]

    intermediate_path_preprocessing, intermediate_path, final_path = return_pkl_paths(
        scenario_name, pkl_paths, model_run
    )

    logger.info("- Starting Production Results Model Flow")
    plant_result_df = read_pickle_folder(intermediate_path, "plant_result_df", "df")
    tech_choices_dict = read_pickle_folder(
        intermediate_path, "tech_choice_dict", "dict"
    )
    utilization_results = read_pickle_folder(
        intermediate_path, "utilization_results", "dict"
    )
    rmi_mapper = create_country_mapper()
    calculated_emissivity_combined = read_pickle_folder(
        intermediate_path_preprocessing, "calculated_emissivity_combined", "df"
    )
    plant_capacity_results = read_pickle_folder(
        intermediate_path, "plant_capacity_results", "df"
    )
    business_cases: pd.DataFrame = read_pickle_folder(
        PKL_DATA_FORMATTED, "standardised_business_cases", "df"
    )
    carbon_tax_timeseries: pd.DataFrame = read_pickle_folder(
        intermediate_path_preprocessing, "carbon_tax_timeseries", "df"
    )
    active_check_results_dict = read_pickle_folder(
        intermediate_path, "active_check_results_dict", "df"
    )
    carbon_tax_timeseries.set_index("year", inplace=True)
    materials_list = business_cases.index.get_level_values(1).unique()
    tech_capacity_df = tech_capacity_splits(
        plant_result_df,
        tech_choices_dict,
        plant_capacity_results,
        active_check_results_dict,
    )
    production_results = generate_production_stats(
        tech_capacity_df,
        utilization_results,
        rmi_mapper,
    )
    production_resource_usage = production_stats_generator(
        production_results, materials_list
    )
    production_emissions = generate_production_emission_stats(
        production_results, calculated_emissivity_combined, carbon_tax_timeseries
    )
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
            results_dict["production_emissions"], final_path, "production_emissions"
        )
    return results_dict
