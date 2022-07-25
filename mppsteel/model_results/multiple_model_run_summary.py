"Script to create multipl model run summary DataFrame"

import itertools
import pandas as pd
import numpy as np

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)



NON_RESOURCE_COLS_DICT = {
    "production_resource_usage": [
        'year', 'plant_name', 'technology', 'capacity', 'country_code',
        'plant_id', 'low_carbon_tech', 'region', 'capacity_utilization',
        'production', 'scenario', 'scenarios', 'region_continent', 
        'region_wsa', 'region_rmi', 'order_of_run', 'total_runs'
    ],
    "production_emissions": [
        'year', 'plant_name', 'technology', 'capacity', 'country_code',
        'plant_id', 'low_carbon_tech', 'region', 'capacity_utilization',
        'production','scenario', 'scenarios', 'region_continent', 'region_wsa', 
        'region_rmi', 'order_of_run', 'total_runs'
    ]
}
GROUPING_COLS = ["order_of_run", "region_rmi"]

def subset_df(df: pd.DataFrame, df_type: str, value_col: str, unit_str = "", year: int = None):
    if year in df["year"].unique():
        df = df[df["year"] == year].copy()
    resource_cols = set(df.columns).difference(set(NON_RESOURCE_COLS_DICT[df_type]))
    value_col_list = return_resource_col(resource_cols, value_col, unit_str)
    subset_cols = GROUPING_COLS + value_col_list
    return df[subset_cols]

def return_scenario(df):
    return df["scenario"].unique()[0]

def return_model_runs(df):
    return df["order_of_run"].unique()

def created_summary_stats(df, value_col: str, stat_type: str):
    if stat_type == "mean":
        df_c = df.groupby("order_of_run").mean()
    elif stat_type == "min":
        df_c = df.groupby("order_of_run").min()
    elif stat_type == "max":
        df_c = df.groupby("order_of_run").max()
    elif stat_type == "var":
        df_c = df.groupby("order_of_run").var()
    elif stat_type == "std":
        df_c = df.groupby("order_of_run").std()
    elif stat_type == "sum":
        df_c = df.groupby("order_of_run").sum()
    else:
        raise ValueError(f"Incorrect stat type entered: {stat_type}")
    return np.concatenate(df_c[value_col].values)

def return_resource_col(resource_list: list, resource_partial_string: str, unit: str = ""):
    matches = [match for match in resource_list if resource_partial_string in match]
    if matches and unit:
        unit_match = [match for match in matches if f"_{unit}" in match]
        matches = unit_match if unit_match else matches
    if not matches:
        raise ValueError(f"No matches for your entries: {locals()}")
    return matches

def generate_multiple_model_run_summary_df(
    df: pd.DataFrame, df_type: str, value_col: str, unit_str = "", year: int = None):
    df_s = subset_df(df, df_type, value_col, unit_str, year)
    value_col = list(set(df_s.columns).difference(set(GROUPING_COLS)))
    df_container = []
    for resource in value_col:
        df = pd.DataFrame(
            data = {
                "model_run": return_model_runs(df),
                "scenario": return_scenario(df),
                "resource": resource,
                "year": year,
                "sum": created_summary_stats(df_s, value_col, "sum"),
            }
        )
    df_container.append(df)
    return pd.concat(df_container).reset_index(drop=True)

def create_emissions_summary_stack(emissions_df: pd.DataFrame, unit: str = "mt", years: list = None):
    df_container = []
    iterator = create_iterator(["s1", "s2", "s3"], years)
    for emissions_col, year in iterator:
        df = generate_multiple_model_run_summary_df(emissions_df, "production_emissions", emissions_col, unit, year)
        df_container.append(df)
    new_col = f"combined_emissions_{unit}"
    emissions_df[new_col] = emissions_df[f"s1_emissions_{unit}"] + emissions_df[f"s2_emissions_{unit}"] + emissions_df[f"s3_emissions_{unit}"]
    for year in years:
        combined = generate_multiple_model_run_summary_df(emissions_df, "production_emissions", new_col, unit, year)
        df_container.append(combined)
    return pd.concat(df_container).reset_index(drop=True)

def get_specific_cols(columns: list, substring_to_find: str, substring_to_remove: str):
    return [col.replace(substring_to_remove, "") for col in columns if substring_to_find in col]

def remove_items_from_list(ref_list: list, cols_to_remove: list):
    for column in cols_to_remove:
        ref_list.remove(column)

def create_production_summary_stack(production_df: pd.DataFrame, material_unit: str, energy_unit: str, years: list = None):
    material_unit_for_cols = f"_{material_unit}"
    energy_unit_for_cols = f"_{energy_unit}"
    material_cols = get_specific_cols(production_df.columns, material_unit_for_cols, material_unit_for_cols)
    remove_items_from_list(material_cols, ["emissivity"])
    energy_cols = get_specific_cols(production_df.columns, energy_unit_for_cols, energy_unit_for_cols)
    remove_items_from_list(energy_cols, ["coal"])
    df_container = []
    material_iterator = create_iterator(material_cols, years)
    energy_iterator = create_iterator(energy_cols, years)
    for material_col, year in material_iterator:
        df = generate_multiple_model_run_summary_df(production_df, "production_resource_usage", material_col, material_unit, year)
        df_container.append(df)
    for energy_col, year in energy_iterator:
        df = generate_multiple_model_run_summary_df(production_df, "production_resource_usage", energy_col, energy_unit, year)
        df_container.append(df)
    return pd.concat(df_container).reset_index(drop=True)

def create_iterator(initial_iterator: list, years: list = None):
    return list(itertools.product(initial_iterator, years)) if years else initial_iterator
