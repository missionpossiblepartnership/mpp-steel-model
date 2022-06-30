"""Graph for Levelized Cost"""
from typing import Union

import pandas as pd
import plotly.express as px

from mppsteel.config.reference_lists import TECH_REFERENCE_LIST
from mppsteel.utility.log_utility import get_logger
from mppsteel.model_graphs.plotly_graphs import bar_chart

logger = get_logger(__name__)


def get_lcost_lowest_vals(
    df: pd.DataFrame, chosen_year: int, value_col: str
) -> Union[pd.DataFrame, dict]:
    """Gets the lowest regional Levelized Cost values for a specified year.

    Args:
        df (pd.DataFrame): The DataFrame containing the LCOS.
        chosen_year (int): The year you want to calculate the LCOS for.
        value_col (str): The column containing the values with the LCOS.

    Returns:
        Union[pd.DataFrame, dict]: Returns the subsetted DataFrame with the lowest costs and
        also a dictionary with the delta values between the lowest and highest cost regions.
    """
    tech_delta_dict = {}
    tech_list = []
    df_c = df.copy()
    df_c.rename(mapper={"levelized_cost": value_col}, axis=1, inplace=True)
    df_s = df_c.set_index(["year", "technology", "country_code"]).loc[chosen_year]
    for technology in TECH_REFERENCE_LIST:
        df_t = df_s.loc[technology]
        min_region = df_t[value_col].idxmin()
        min_val = df_t[value_col].min()
        max_val = df_t[value_col].max()
        tech_delta_dict[technology] = max_val - min_val
        df_subset = df_c[
            (df_c["year"] == chosen_year)
            & (df_c["technology"] == technology)
            & (df_c["country_code"] == min_region)
        ]
        tech_list.append(df_subset)
    df_combined = (
        pd.concat(tech_list)
        .set_index(["technology"])
        .drop(["year", "country_code"], axis=1)
    )
    return df_combined, tech_delta_dict


def assign_country_deltas(df: pd.DataFrame, delta_dict: dict) -> pd.DataFrame:
    """Assigns the delta values to each respective dictionary

    Args:
        df (pd.DataFrame): A DataFrame containing the lowest region cost values.
        delta_dict (dict): A dictionary containing the delta values between low and high.

    Returns:
        pd.DataFrame: A DataFrame with a new column `LCOS delta` containing the delat for each technology between
        the lowest and highest cost values.
    """
    df_c = df.copy()
    tech_values = df_c.index.get_level_values(0).unique()
    for technology in tech_values:
        df_c.loc[technology, "LCOS delta"] = delta_dict[technology]
    return df_c

def melt_and_subset(df: pd.DataFrame, cost_types: list):
    df_c = df.copy()
    df_c = df_c.reset_index().melt(
        id_vars=["technology"], var_name="cost_type", value_name="cost",
    )
    return df_c[df_c["cost_type"].isin(cost_types)].copy()


def lcost_graph(
    lcost_data: pd.DataFrame,
    chosen_year: int = 2030,
    save_filepath: str = None,
    ext: str = "png",
) -> px.bar:
    """Creates a bar graph for the Levelized Cost.

    Args:
        lcost_data (pd.DataFrame): The levelized cost DataFrame.
        chosen_year (int, optional): The year you want to calculate the Lcost for. Defaults to 2030.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.bar: A Plotly express bar chart.
    """

    lcost_c, country_deltas = get_lcost_lowest_vals(lcost_data, chosen_year, "LCOS")
    lcost_c = assign_country_deltas(lcost_c, country_deltas)

    bar_chart_order = {"LCOS": "#E76B67", "LCOS delta": "#1E3B63"}

    lcost_c = melt_and_subset(lcost_c, bar_chart_order.keys())

    fig_ = bar_chart(
        data=lcost_c,
        x="technology",
        y="cost",
        color="cost_type",
        color_discrete_map=bar_chart_order,
        xaxis_title="Technology",
        yaxis_title="Levelized Cost [$/tCS]",
        title_text=f"Levelized cost by end-state compatible technology in {chosen_year}",
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_
