"""Creates summary of combined dataset for resource usage"""

from typing import Dict, Union
import pandas as pd
from copy import deepcopy
from tqdm import tqdm
import functools as ft

from mppsteel.config.model_config import (
    GIGAJOULE_TO_MEGAJOULE_FACTOR,
    TERAWATT_TO_PETAJOULE_FACTOR,
    MEGATON_TO_TON,
    HYDROGEN_ENERGY_DENSITY_MJ_PER_KG,
    BIOMASS_ENERGY_DENSITY_GJ_PER_TON,
    TON_TO_KILOGRAM_FACTOR,
)
from mppsteel.utility.function_timer_utility import timer_func

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file

logger = get_logger(__name__)

RESULTS_METADATA: Dict[str, Union[str, Dict[str, Union[str, float, int]]]] = {
    "grid_power": {
        "old_value_column": "electricity_pj",
        "new_value_column": "grid_power_demand_twh",
        "conversion_factor": 1 / TERAWATT_TO_PETAJOULE_FACTOR,
    },
    "green_hydrogen": {
        "old_value_column": "hydrogen_gj",
        "new_value_column": "green_hydrogen_mt",
        "conversion_factor": GIGAJOULE_TO_MEGAJOULE_FACTOR
        / HYDROGEN_ENERGY_DENSITY_MJ_PER_KG
        / TON_TO_KILOGRAM_FACTOR
        / MEGATON_TO_TON,
    },
    "ccs_demand": {
        "old_value_column": "captured_co2_mt",
        "new_value_column": "ccs_demand_mt",
        "conversion_factor": 1,
    },
    "biofuel_biomass": {
        "old_value_column": "biomass_gj",
        "new_value_column": "biomass_mt",
        "conversion_factor": 1 / (BIOMASS_ENERGY_DENSITY_GJ_PER_TON * MEGATON_TO_TON),
    },
    "biofuel_biomethane": {
        "old_value_column": "biomethane_gj",
        "new_value_column": "biomethane_mt",
        "conversion_factor": 1 / (BIOMASS_ENERGY_DENSITY_GJ_PER_TON * MEGATON_TO_TON),
    },
    "natural_gas": {
        "old_value_column": "natural_gas_gj",
        "new_value_column": "natural_gas_demand_gj",
        "conversion_factor": 1,
    },
    "met_coal": {
        "old_value_column": "met_coal_gj",
        "new_value_column": "met_coal_demand_gj",
        "conversion_factor": 1,
    },
    "thermal_coal": {
        "old_value_column": "thermal_coal_gj",
        "new_value_column": "thermal_coal_demand_gj",
        "conversion_factor": 1,
    },
    "cdr_usage": {
        "old_value_column": "captured_co2_mt",
        "new_value_column": "cdr_usage_mt",
        "conversion_factor": 1,
    },
}

REGION_COLUMN: str = "region_rmi"


def calculate_metric(
    production_df: pd.DataFrame,
    base_columns: list,
    merge_columns: list,
    old_value_column: str,
    new_value_column: str,
    conversion_factor: float = 1,
) -> pd.DataFrame:
    """Performas a number of operations on a DataFrame.
    1) Subsets a DataFrame,
    2) Convert a column old_value_column to a new column new_value_column based on the conversion_factor.
    3) Rename the region column name.
    4) Groups the final DataFrame.

    Args:
        production_df (pd.DataFrame): The production resource usage DataFrame.
        base_columns (list): The base columns that will be used not be altered during the operations.
        merge_columns (list): The columns that will be used to group the final DataFrame.
        old_value_column (str): The name of the column to be multipled by the conversion_factor.
        new_value_column (str): The name of the column to replace old_value_column.
        conversion_factor (float, optional): A float that will be multiplied by the old_value_column to create a new_value_column. Defaults to 1.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    # Column manipulation
    all_columns = base_columns + [old_value_column]

    # DataFrame manipulation
    df_c = production_df[all_columns].copy()
    df_c[new_value_column] = df_c[old_value_column] * conversion_factor
    df_c.drop(old_value_column, axis=1, inplace=True)
    df_c.rename({REGION_COLUMN: "region"}, axis=1, inplace=True)

    # Final grouping
    return df_c.groupby(merge_columns).agg("sum")


def model_residual_emissions(
    production_emissions: pd.DataFrame, base_columns: list, merge_columns: list
) -> pd.DataFrame:
    """Function that generates a residual emissions column by combining s1_emissions_mt and s2_emissions_mt.

    Args:
        production_emissions (pd.DataFrame): The production emissions DataFrame.
        base_columns (list): The base columns that will be used not be altered during the operations.
        merge_columns (list): The columns that will be used to group the final DataFrame.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    # Column manipulation
    residual_emissions_columns = ["s1_emissions_mt", "s2_emissions_mt"]
    all_columns = base_columns + residual_emissions_columns

    # DataFrame manipulation
    df_c = production_emissions[all_columns].copy()
    df_c["residual_emissions_mt"] = df_c["s1_emissions_mt"] + df_c["s2_emissions_mt"]
    df_c.drop(residual_emissions_columns, axis=1, inplace=True)
    df_c.rename(mapper={"region_rmi": "region"}, axis=1, inplace=True)

    # Final grouping
    return df_c.groupby(merge_columns).agg("sum")


def model_primary_secondary_materials(
    production_df: pd.DataFrame, base_columns: list, merge_columns: list
) -> pd.DataFrame:
    """Function that generates primary and secondary material related statistics.
    - primary_plus_secondary_material = crude_steel_mt + scrap_steel_mt (outputted as sum)
    - scrap_steel_pct = scrap_steel_mt + primary_plus_secondary_material (otuputted as mean)
    - crude_steel_pct = crude_steel_mt / primary_plus_secondary_material (otuputted as mean)

    Args:
        production_df (pd.DataFrame): The production resource usage DataFrame.
        base_columns (list): The base columns that will be used not be altered during the operations.
        merge_columns (list): The columns that will be used to group the final DataFrame.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    # Column manipulation
    primary_secondary_materials = ["production", "scrap_mt"]
    all_columns = base_columns + primary_secondary_materials
    column_mapper = {"production": "crude_steel_mt", "scrap_mt": "scrap_steel_mt"}

    # DataFrame manipulation
    df_c = production_df[all_columns].copy()
    df_c.rename(mapper=column_mapper, axis=1, inplace=True)
    df_c["primary_plus_secondary_material"] = (
        df_c["crude_steel_mt"] + df_c["scrap_steel_mt"]
    )
    df_c["scrap_steel_pct"] = (
        df_c["scrap_steel_mt"] / df_c["primary_plus_secondary_material"]
    )
    df_c["crude_steel_pct"] = (
        df_c["crude_steel_mt"] / df_c["primary_plus_secondary_material"]
    )
    df_c.drop("primary_plus_secondary_material", axis=1, inplace=True)
    df_c.rename(mapper={"region_rmi": "region"}, axis=1, inplace=True)

    # Final grouping
    return (
        df_c.groupby(merge_columns)
        .agg(
            {
                "crude_steel_mt": "sum",
                "scrap_steel_mt": "sum",
                "scrap_steel_pct": "mean",
                "crude_steel_pct": "mean",
            }
        )
        .round(2)
    )


def create_demand_summary(
    production_df: pd.DataFrame,
    emissions_df: pd.DataFrame,
    results_metadata_dict: dict,
    metadata_columns: list,
    final_metadata_columns: list,
) -> pd.DataFrame:
    """Flow for generating the demand summary.
    1) Generates results for each resource in results_metadata_dict using production_df
    2) Generates results for residual_emissions
    3) Generates results for primary_secondary_materials
    4) Combines all the results into one DataFrame
    5) Returns the combined DataFrame.

    Args:
        production_df (pd.DataFrame): The production resource usage DataFrame.
        production_emissions (pd.DataFrame): The production emissions DataFrame.
        results_metadata_dict (dict): A dictionary of resources that will be used to store the demand summary results.
        metadata_columns (list): The base columns that will be used not be altered during the operations.
        final_metadata_columns (list): The columns that will be used to group the final DataFrame.

    Returns:
        pd.DataFrame: A combined demand summary of production summary and emissions summary DataFrames.
    """
    dfs = []
    for resource in tqdm(
        results_metadata_dict,
        total=len(results_metadata_dict),
        desc="Creating Results Output",
    ):
        result_df = calculate_metric(
            production_df=production_df,
            base_columns=metadata_columns,
            merge_columns=final_metadata_columns,
            old_value_column=results_metadata_dict[resource]["old_value_column"],
            new_value_column=results_metadata_dict[resource]["new_value_column"],
            conversion_factor=results_metadata_dict[resource]["conversion_factor"],
        )
        dfs.append(result_df)

    residual_emissions = model_residual_emissions(
        production_emissions=emissions_df,
        base_columns=metadata_columns,
        merge_columns=final_metadata_columns,
    )
    primary_secondary_materials = model_primary_secondary_materials(
        production_df=production_df,
        base_columns=metadata_columns,
        merge_columns=final_metadata_columns,
    )
    for df in [primary_secondary_materials, residual_emissions]:
        dfs.append(df)
    df_final = ft.reduce(
        lambda left, right: pd.merge(left, right, on=final_metadata_columns), dfs
    )
    return df_final.reset_index().round(2)


@timer_func
def create_resource_demand_summary(
    output_folder_path: str, serialize: bool = False
) -> dict:
    """Production results flow to create the Production resource usage DataFrame and the Production Emissions DataFrame.

    Args:
        output_folder_path (dict): The ouput path to store the resource_demand_summary.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary containing the two DataFrames.
    """
    production_resource_usage = read_pickle_folder(
        output_folder_path, "production_resource_usage", "df"
    )
    production_emissions = read_pickle_folder(
        output_folder_path, "production_emissions", "df"
    )
    metadata_columns = ["scenario", "year", REGION_COLUMN]
    final_metadata_columns = deepcopy(metadata_columns)
    final_metadata_columns.remove(REGION_COLUMN)
    final_metadata_columns.append("region")

    resource_demand_summary = create_demand_summary(
        production_df=production_resource_usage,
        emissions_df=production_emissions,
        results_metadata_dict=RESULTS_METADATA,
        metadata_columns=metadata_columns,
        final_metadata_columns=final_metadata_columns,
    )
    if serialize:
        logger.info("-- Serializing dataframe")
        serialize_file(
            resource_demand_summary, output_folder_path, "resource_demand_summary"
        )
    return resource_demand_summary
