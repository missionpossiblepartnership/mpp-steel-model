"Script to create multiple model run summary DataFrames"

from typing import Callable
import pandas as pd

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)

NAMED_AGG_DICT = {
    "capital_cost": pd.NamedAgg(column="capital_cost", aggfunc="sum"),
    "number_of_plants": pd.NamedAgg(column="year", aggfunc="size"),
    "capacity": pd.NamedAgg(column="capacity", aggfunc="sum"),
    "production": pd.NamedAgg(column="production", aggfunc="sum"),
    "iron_ore_mt": pd.NamedAgg(column="iron_ore_mt", aggfunc="sum"),
    "scrap_mt": pd.NamedAgg(column="scrap_mt", aggfunc="sum"),
    "cost_of_steelmaking": pd.NamedAgg(column="cost_of_steelmaking", aggfunc="sum"),
    "levelized_cost": pd.NamedAgg(column="levelized_cost", aggfunc="sum"),
    "s1_emissivity": pd.NamedAgg(column="s1_emissivity", aggfunc="sum"),
    "s2_emissivity": pd.NamedAgg(column="s2_emissivity", aggfunc="sum"),
    "s3_emissivity": pd.NamedAgg(column="s3_emissivity", aggfunc="sum"),
    "combined_emissivity": pd.NamedAgg(column="combined_emissivity", aggfunc="sum"),
}


def consumption_summary(df: pd.DataFrame, grouping_cols: list, unit_list: list) -> pd.DataFrame:
    """Creates an average consumption summary of a multiple run by subsetting a DataFrame based on 
    grouped columns and unit/value columns and divides the sum of the grouped dataframe by the number of model runs.

    Args:
        df (pd.DataFrame): The DataFrame containing the multiple model runs.
        grouping_cols (list): The columns representing the data dimensions.
        unit_list (list): A list of the extensions from the columns.

    Returns:
        pd.DataFrame: The summarised and averaged DataFrame.
    """
    total_runs = df["total_runs"].unique()[0]
    resource_cols = [col for col in df.columns if any(ext in col for ext in unit_list)]
    df_c = df[grouping_cols + resource_cols]
    grouped_df = df_c.groupby(
        by=grouping_cols,
    ).sum()
    grouped_df = grouped_df / total_runs
    return grouped_df.reset_index()


def generic_summary(
    df: pd.DataFrame,
    grouping_cols: list,
    value_cols: list,
    custom_agg_function: Callable,
) -> pd.DataFrame:
    """Creates an average consumption summary of a multiple run by subsetting a DataFrame based on 
    grouped columns and unit/value columns and divides the DataFrame that has been grouped by a custom operation
    by the number of model runs.

    Args:
        df (pd.DataFrame): The DataFrame containing the multiple model runs.
        grouping_cols (list): The columns representing the data dimensions.
        value_cols (list): The columns representing the data values.
        custom_agg_function (Callable): A custom function that performs an operation on the subsetted DataFrame.

    Returns:
        pd.DataFrame: The summarised and averaged DataFrame.
    """
    total_runs = df["total_runs"].unique()[0]
    grouped_df = custom_agg_function(df[grouping_cols + value_cols], grouping_cols)
    grouped_df = grouped_df / total_runs
    return grouped_df.reset_index()


def investment_groupby(df: pd.DataFrame, grouping_cols: list) -> pd.DataFrame:
    """Groups an Investment Summary DataFrame in preparation to create a summary output.

    Args:
        df (pd.DataFrame): The DataFrame containing the multiple model runs.
        grouping_cols (list): The columns representing the data dimensions.

    Returns:
        pd.DataFrame: A grouped DataFrame.
    """
    return df.groupby(by=grouping_cols).agg(
        capital_cost=NAMED_AGG_DICT["capital_cost"],
        number_of_plants=NAMED_AGG_DICT["number_of_plants"],
    )


def plant_capacity_groupby(df: pd.DataFrame, grouping_cols: list) -> pd.DataFrame:
    """Groups a Plant Capacity Summary DataFrame in preparation to create a summary output.

    Args:
        df (pd.DataFrame): The DataFrame containing the multiple model runs.
        grouping_cols (list): The columns representing the data dimensions.

    Returns:
        pd.DataFrame: A grouped DataFrame.
    """
    return df.groupby(by=grouping_cols).agg(
        capacity=NAMED_AGG_DICT["capacity"],
        production=NAMED_AGG_DICT["production"],
        number_of_plants=NAMED_AGG_DICT["number_of_plants"],
    )


def plant_capacity_country_breakdown_groupby(df: pd.DataFrame, grouping_cols: list) -> pd.DataFrame:
    """Groups a Plant Capacity by Country Breakdown Summary DataFrame in preparation to create a summary output.

    Args:
        df (pd.DataFrame): The DataFrame containing the multiple model runs.
        grouping_cols (list): The columns representing the data dimensions.

    Returns:
        pd.DataFrame: A grouped DataFrame.
    """
    return df.groupby(by=grouping_cols).agg(
        capacity=NAMED_AGG_DICT["capacity"],
        production=NAMED_AGG_DICT["production"],
        iron_ore_mt=NAMED_AGG_DICT["iron_ore_mt"],
        scrap_mt=NAMED_AGG_DICT["scrap_mt"],
        number_of_plants=NAMED_AGG_DICT["number_of_plants"],
    )


def cost_of_steelmaking_groupby(df: pd.DataFrame, grouping_cols: list) -> pd.DataFrame:
    """Groups a Cost of Steelmaking Summary DataFrame in preparation to create a summary output.

    Args:
        df (pd.DataFrame): The DataFrame containing the multiple model runs.
        grouping_cols (list): The columns representing the data dimensions.

    Returns:
        pd.DataFrame: A grouped DataFrame.
    """
    return df.groupby(by=grouping_cols).agg(
        cost_of_steelmaking=NAMED_AGG_DICT["cost_of_steelmaking"],
    )


def levelized_cost_standardized_groupby(df: pd.DataFrame, grouping_cols: list) -> pd.DataFrame:
    """Groups a Levelized Cost Summary DataFrame in preparation to create a summary output.

    Args:
        df (pd.DataFrame): The DataFrame containing the multiple model runs.
        grouping_cols (list): The columns representing the data dimensions.

    Returns:
        pd.DataFrame: A grouped DataFrame.
    """
    return df.groupby(by=grouping_cols).agg(
        levelized_cost=NAMED_AGG_DICT["levelized_cost"],
    )


def calculated_emissivity_combined_groupby(df: pd.DataFrame, grouping_cols: list) -> pd.DataFrame:
    """Groups an Calculated Emissivity Summary DataFrame in preparation to create a summary output.

    Args:
        df (pd.DataFrame): The DataFrame containing the multiple model runs.
        grouping_cols (list): The columns representing the data dimensions.

    Returns:
        pd.DataFrame: A grouped DataFrame.
    """
    return df.groupby(by=grouping_cols).agg(
        s1_emissivity=NAMED_AGG_DICT["s1_emissivity"],
        s2_emissivity=NAMED_AGG_DICT["s2_emissivity"],
        s3_emissivity=NAMED_AGG_DICT["s3_emissivity"],
        combined_emissivity=NAMED_AGG_DICT["combined_emissivity"],
    )


def summarise_combined_data(
    df: pd.DataFrame, results_dict: dict, filename: str = "",
) -> dict:
    """Runs through the flow to create summarised DataFrames based on the filename.

    Args:
        df (pd.DataFrame): The base filename Dataframe used to created the summary DataFrame(s).
        results_dict (dict): The container used to store the results.
        filename (str, optional): The base filename used to control the logi flow of summary DataFrame generation. Defaults to "".

    Returns:
        dict: A dictionary containing the new generated DataFrames relevant to filename.
    """
    logger.info(f"Creating {filename} summary")

    if filename == "production_emissions":
        production_emissions_summary = consumption_summary(
            df,
            grouping_cols=["scenario", "year", "region_rmi", "technology"],
            unit_list=["production", "_gt", "_mt"],
        )
        results_dict["production_emissions_summary"].append(production_emissions_summary)

    elif filename == "production_resource_usage":
        production_resource_usage_summary = consumption_summary(
            df,
            grouping_cols=["scenario", "year", "region_rmi", "technology"],
            unit_list=["capacity", "production", "_gt", "_mt", "_gj", "_pj"],
        )

        plant_capacity_summary = generic_summary(
            df,
            grouping_cols=["scenario", "year", "region_rmi", "technology"],
            value_cols=["capacity", "production"],
            custom_agg_function=plant_capacity_groupby,
        )

        plant_capacity_summary_country_breakdown = generic_summary(
            df,
            grouping_cols=["scenario", "year", "country_code", "technology"],
            value_cols=["capacity", "production", "iron_ore_mt", "scrap_mt"],
            custom_agg_function=plant_capacity_country_breakdown_groupby,
        )
        results_dict[
            "production_resource_usage_summary"
        ].append(production_resource_usage_summary)
        results_dict["plant_capacity_summary"].append(plant_capacity_summary)
        results_dict[
            "plant_capacity_summary_country_breakdown"
        ].append(plant_capacity_summary_country_breakdown)

    elif filename == "cost_of_steelmaking":
        cost_of_steelmaking_summary = generic_summary(
            df,
            grouping_cols=["scenario", "year", "region_rmi"],
            value_cols=["cost_of_steelmaking"],
            custom_agg_function=cost_of_steelmaking_groupby,
        )
        results_dict["cost_of_steelmaking_summary"].append(cost_of_steelmaking_summary)

    elif filename == "investment_results":
        investment_results_summary = generic_summary(
            df,
            grouping_cols=[
                "scenario",
                "year",
                "region_rmi",
                "switch_type",
                "start_tech",
                "end_tech",
            ],
            value_cols=["capital_cost"],
            custom_agg_function=investment_groupby,
        )
        results_dict["investment_results_summary"].append(investment_results_summary)

    elif filename == "levelized_cost_standardized":
        levelized_cost_standardized_summary = generic_summary(
            df,
            grouping_cols=["scenario", "year", "region", "country_code", "technology"],
            value_cols=["levelized_cost"],
            custom_agg_function=levelized_cost_standardized_groupby,
        )
        results_dict[
            "levelized_cost_standardized_summary"
        ].append(levelized_cost_standardized_summary)

    elif filename == "calculated_emissivity_combined":
        calculated_emissivity_combined_summary = generic_summary(
            df,
            grouping_cols=["scenario", "year", "region", "country_code", "technology"],
            value_cols=[
                "s1_emissivity",
                "s2_emissivity",
                "s3_emissivity",
                "combined_emissivity",
            ],
            custom_agg_function=calculated_emissivity_combined_groupby,
        )
        results_dict[
            "calculated_emissivity_combined_summary"
        ].append(calculated_emissivity_combined_summary)

    elif filename in {"full_trade_summary", "plant_result_df"}:
        results_dict[filename].append(df)

    return results_dict
