"""New Plant Capacity Graph"""

from typing import Union
from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.model_config import MEGATON_TO_KILOTON_FACTOR, MODEL_YEAR_RANGE
from mppsteel.config.reference_lists import MPP_COLOR_LIST

from mppsteel.utility.log_utility import get_logger
from mppsteel.model_graphs.plotly_graphs import area_chart, bar_chart, line_chart

logger = get_logger(__name__)


def create_new_capacity_subset(
    plant_df: pd.DataFrame, data_agg_type: str = "sum"
) -> pd.DataFrame:
    """Creates a DataFrame that captures all of the new capacity of the DataFrame.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        data_agg_type (str, optional): The method of aggregating the data. Either `sum` or `cumsum`. Defaults to 'sum'.

    Returns:
        pd.DataFrame: A DataFrame with the new capacity added in each region.
    """

    def assign_cumsum_value(sum_df: pd.DataFrame, year: int) -> float:
        return sum_df.loc[year, "plant_capacity"] if year in sum_df.index else 0

    df_c = plant_df.copy()
    new_plants = [plant_id for plant_id in df_c["plant_id"] if plant_id[:3] == "MPP"]
    new_plants_df = df_c[df_c["plant_id"].isin(new_plants)].copy()
    if data_agg_type == "sum":
        new_plants_df = new_plants_df.rename(
            mapper={"start_of_operation": "year"}, axis=1
        )
        new_plants_df = (
            new_plants_df[
                [
                    "plant_name",
                    "year",
                    "end_of_operation",
                    "plant_capacity",
                    "rmi_region",
                ]
            ]
            .groupby(["year", "rmi_region"])
            .agg("sum")
        )

    if data_agg_type == "cumsum":
        regions = new_plants_df["rmi_region"].unique()
        region_dict = {}
        df_list = []
        for region in regions:
            region_df = new_plants_df[new_plants_df["rmi_region"] == region][
                ["start_of_operation", "plant_capacity"]
            ].copy()
            region_df = region_df.groupby(["start_of_operation"]).sum()
            new_df = pd.DataFrame({"year": MODEL_YEAR_RANGE})
            new_df["plant_capacity"] = new_df["year"].apply(
                lambda year: assign_cumsum_value(region_df, year)
            )
            new_df = new_df.set_index("year").cumsum()
            region_dict[region] = new_df
        for region_key in region_dict:
            df_r = region_dict[region_key]
            df_r["rmi_region"] = region_key
            df_list.append(df_r)
        new_plants_df = pd.concat(df_list)

    new_plants_df["plant_capacity_mt"] = (
        new_plants_df["plant_capacity"] / MEGATON_TO_KILOTON_FACTOR
    )
    return new_plants_df.reset_index()


def new_plant_capacity_graph(
    plant_df: pd.DataFrame,
    graph_type: str,
    save_filepath: str = None,
    ext: str = "png",
) -> None:
    """Creates a graph showing the new capacity across each region.

    Args:
        plant_df (pd.DataFrame): The full steel plant DataFrame.
        graph_type (str): Specify the type of graph to return, either 'area' or 'bar'.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".
    """
    regions = plant_df["rmi_region"].unique()
    color_mapper = dict(zip_longest(regions, MPP_COLOR_LIST))
    fig_ = None

    if graph_type == "area":
        fig_ = area_chart(
            data=create_new_capacity_subset(plant_df, "cumsum"),
            x="year",
            y="plant_capacity_mt",
            color="rmi_region",
            color_discrete_map=color_mapper,
            name=f"New Plant Capacity Per Region (Cumulative)",
            x_axis="Year",
            y_axis="Plant Capacity [Mt]",
            hoverdata=None,
        )

    if graph_type == "bar":
        fig_ = bar_chart(
            data=create_new_capacity_subset(plant_df, "sum"),
            x="year",
            y="plant_capacity_mt",
            color="rmi_region",
            color_discrete_map=color_mapper,
            xaxis_title="Year",
            yaxis_title="Plant Capacity [Mt]",
            title_text=f"New Plant Capacity Per Region",
        )

    if save_filepath:
        assert fig_
        fig_.write_image(f"{save_filepath}.{ext}")


def trade_balance_graph(
    trade_df: pd.DataFrame,
    save_filepath: str = None,
    ext: str = "png",
) -> px.line:
    """Creates a graph showing the trade balance for each region around a zero-balance axis.

    Args:
        trade_df (pd.DataFrame): The Trade results DataFrame.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.line: A plotly express line graph.
    """
    regions = trade_df["region"].unique()
    color_mapper = dict(zip_longest(regions, MPP_COLOR_LIST))

    fig_ = line_chart(
        data=trade_df,
        x="year",
        y="trade_balance",
        color="region",
        color_discrete_map=color_mapper,
        name=f"Global Crude Steel Trade Balance",
        x_axis="Year",
        y_axis="Crude Steel Trade Balance [Mt]",
    )
    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")
