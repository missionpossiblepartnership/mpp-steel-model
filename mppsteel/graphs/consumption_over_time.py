"""Graph fpr the OPEX CAPEX split"""
from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.reference_lists import MPP_COLOR_LIST
from mppsteel.utility.log_utility import get_logger

from mppsteel.graphs.plotly_graphs import bar_chart

logger = get_logger(__name__)

ENERGY_RESOURCES_COT = [
    "biomass_pj",
    "biomethane_pj",
    "bioenergy_pj",
    "electricity_pj",
    "hydrogen_pj",
    "thermal_coal_pj",
    "met_coal_pj",
    "coal_pj",
    "natural_gas_pj",
]

MATERIAL_RESOURCES_COT = [
    "iron_ore_mt",
    "scrap_mt",
    "dri_mt",
    "used_co2_mt",
    "captured_co2_mt",
    "bf_slag_mt",
    "other_slag_mt"
]

def format_cot_graph(
    df: pd.DataFrame, region: list = None, resource_list: list = None
) -> pd.DataFrame:
    """Formats the Consumption over time graph to create a DataFrame ready to be used to create graphs.
    Args:
        df (pd.DataFrame): A DataFrame containing the Production data.
        regions (list, optional): A list of regions you want to subset the DataFrame with. Defaults to None.
        resource_list (list, optional): A list of resources you want to subset the DataFrame with. Defaults to None.

    Returns:
        pd.DataFrame: A subsetted DataFrame.
    """
    df_c = df.copy()
    df_c = pd.melt(
        df,
        id_vars=["year", "region"],
        value_vars=resource_list,
        var_name="metric",
    )
    df_c.reset_index(drop=True, inplace=True)
    df_c = (
        df_c.groupby(["region", "year", "metric"], as_index=False)
        .agg({"value": "sum"})
        .round(2)
    )
    if region:
        df_c = df_c.loc[df_c["region"] == region]
    df_c = (
        df_c.groupby(["year", "metric"], as_index=False)
        .agg({"value": "sum"})
        .round(2)
    )
    return df_c


def consumption_over_time_graph(
    production_resource_usage: pd.DataFrame, resource_type: str = 'energy', region: str = None, save_filepath: str = None, ext: str = "png"
) -> px.bar:
    """Generates a Graph showing the consumption over time of a material resource.

    Args:
        regions (list, optional): The regions you want to graph. Defaults to None.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.bar: A Plotly express bar chart.
    """

    if resource_type == 'energy':
        resource_list = ENERGY_RESOURCES_COT
        y_axis_title = '[PJ/year]'

    elif resource_type == 'material':
        resource_list = MATERIAL_RESOURCES_COT
        y_axis_title = '[Mt/year]'

    production_resource_usage = format_cot_graph(
        production_resource_usage, 
        region, 
        resource_list=resource_list
    )

    color_mapper = dict(zip_longest(resource_list, MPP_COLOR_LIST))

    fig_ = bar_chart(
        data=production_resource_usage,
        x="year",
        y="value",
        color="metric",
        color_discrete_map=color_mapper,
        xaxis_title="Year",
        yaxis_title=y_axis_title,
        title_text="Consumption chart",
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_
