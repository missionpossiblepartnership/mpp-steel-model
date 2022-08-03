"Script to create multiple model run summary DataFrames"

import pandas as pd

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def consumption_summary(df: pd.DataFrame, grouping_cols: list, unit_list: list):
    total_runs = df["total_runs"].unique()[0]
    resource_cols = [col for col in df.columns if any(ext in col for ext in unit_list)]
    combined_cols = grouping_cols + resource_cols
    df_c = df[combined_cols]
    grouped_df = df_c.groupby(
        by=grouping_cols,
    ).sum()
    grouped_df = grouped_df / total_runs
    return grouped_df.reset_index()._to_pandas()

def generic_summary(
    df: pd.DataFrame, grouping_cols: list, 
    value_cols: list, agg_dict: dict, 
    include_plant_count: bool = False
):
    total_runs = df["total_runs"].unique()[0]
    combined_cols = grouping_cols + value_cols
    df_c = df[combined_cols]
    if include_plant_count:
        df_c["number_of_plants"] = 0
        agg_dict["number_of_plants"] = "size"
    grouped_df = df_c.groupby(by=grouping_cols).agg(agg_dict)
    grouped_df = grouped_df / total_runs
    return grouped_df.reset_index()._to_pandas()

def summarise_combined_data(agg_dict: dict) -> dict:
    logger.info("Creating production_emissions summary")
    production_emissions_summary = consumption_summary(
        df=agg_dict["production_emissions"], 
        grouping_cols=["scenario", "year", "region_rmi", "technology"], 
        unit_list=["_gt", "_mt"]
    )

    logger.info("Creating production resource usage summary")
    production_resource_usage_summary = consumption_summary(
        df=agg_dict["production_resource_usage"], 
        grouping_cols=["scenario", "year", "region_rmi", "technology"], 
        unit_list=["_gt", "_mt", "_gj", "_pj"]
    )

    logger.info("Creating plant capacity and production summary")
    plant_capacity_summary = generic_summary(
        df=agg_dict["production_resource_usage"],
        grouping_cols=["scenario", "year", "region_rmi", "technology"],
        value_cols=["capacity", "production"],
        agg_dict={"capacity": "sum", "production": "sum"},
        include_plant_count=True
    )

    plant_capacity_summary_country_breakdown = generic_summary(
        df=agg_dict["production_resource_usage"],
        grouping_cols=["scenario", "year", "country_code", "technology"],
        value_cols=["capacity", "production", "iron_ore_mt", "scrap_mt"],
        agg_dict={"capacity": "sum", "production": "sum", "iron_ore_mt": "sum", "scrap_mt": "sum"},
        include_plant_count=True
    )

    logger.info("Creating cost of steelmaking summary")
    cost_of_steelmaking_summary = generic_summary(
        df=agg_dict["cost_of_steelmaking"],
        grouping_cols=["scenario", "year", "region_rmi"],
        value_cols=["cost_of_steelmaking"],
        agg_dict={"cost_of_steelmaking": "sum"}
    )

    logger.info("Creating investment results summary")
    investment_results_summary = generic_summary(
        df=agg_dict["investment_results"],
        grouping_cols=["scenario", "year", "region_rmi", "switch_type", "start_tech", "end_tech"],
        value_cols=["capital_cost"],
        agg_dict={"capital_cost": "sum"},
        include_plant_count=True
    )

    logger.info("Creating levelized cost summary")
    levelized_cost_standardized_summary = generic_summary(
        df=agg_dict["levelized_cost_standardized"],
        grouping_cols=["scenario", "year", "region", "country_code", "technology"],
        value_cols=["levelized_cost"],
        agg_dict={"levelized_cost": "sum"}
    )

    calc_em_value_cols = ["s1_emissivity", "s2_emissivity", "s3_emissivity", "combined_emissivity"]
    calc_em_agg_dict = {val_col: "sum" for val_col in calc_em_value_cols}
    calculated_emissivity_combined_summary = generic_summary(
        df=agg_dict["calculated_emissivity_combined"],
        grouping_cols=["scenario", "year", "region", "country_code", "technology"],
        value_cols=["s1_emissivity", "s2_emissivity", "s3_emissivity", "combined_emissivity"],
        agg_dict=calc_em_agg_dict,
    )

    return {
        "production_emissions_summary": production_emissions_summary,
        "production_resource_usage_summary": production_resource_usage_summary,
        "plant_capacity_summary": plant_capacity_summary,
        "plant_capacity_summary_country_breakdown": plant_capacity_summary_country_breakdown,
        "cost_of_steelmaking_summary": cost_of_steelmaking_summary,
        "investment_results_summary": investment_results_summary,
        "levelized_cost_standardized_summary": levelized_cost_standardized_summary,
        "calculated_emissivity_combined_summary": calculated_emissivity_combined_summary,
        "full_trade_summary": agg_dict["full_trade_summary"]._to_pandas(),
        "plant_result_df": agg_dict["plant_result_df"]._to_pandas()
    }
