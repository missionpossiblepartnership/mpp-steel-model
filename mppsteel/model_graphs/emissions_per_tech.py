from itertools import zip_longest
import pandas as pd
import plotly.express as px
import numpy as np

from mppsteel.config.reference_lists import (
    GRAPH_COL_ORDER,
    MPP_COLOR_LIST,
    TECH_REFERENCE_LIST,
)
from mppsteel.model_graphs.plotly_graphs import line_chart
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)

BAR_CHART_ORDER_EMISSIVITY = {
    "s1_emissivity": "#59A270",
    "s2_emissivity": "#7F6000",
    "s3_emissivity": "#1E3B63",
}


def generate_emissivity_charts(
    df: pd.DataFrame,
    year: int = None,
    region: str = None,
    scope: str = None,
    save_filepath: str = None,
    ext: str = "png",
):
    """Generates bar chart with emissivity [t CO2/ t steel] per technology. Displays scope1, scope2, scope2 or combination of scopes.
    Scope can be either 's1_emissivity', 's2_emissivity', 's3_emissivity', 's1+s2', or 'combined'.

    Args:
        df (pd.DataFrame): calculated_emissivity_combined DataFrame.
        year (int, optional): The year to subset the DataFrame. Defaults to None.
        region (str, optional): The region to subset the DataFrame. Defaults to None.
        scope (str, optional): The scope(s) to subset the DataFrame. Defaults to None.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        _type_: _description_
    """
    df_c = df.copy()
    df_c = df_c.groupby(["technology", "year", "region"], as_index=False).agg(
        {
            "s1_emissivity": np.mean,
            "s2_emissivity": np.mean,
            "s3_emissivity": np.mean,
            "combined_emissivity": np.mean,
        }
    )
    df_c = pd.melt(
        df_c,
        id_vars=["year", "region", "technology"],
        value_vars=[
            "s1_emissivity",
            "s2_emissivity",
            "s3_emissivity",
            "combined_emissivity",
        ],
        var_name="metric",
    )
    sorterIndex = dict(zip(GRAPH_COL_ORDER, range(len(GRAPH_COL_ORDER))))
    # Note: Scope 1 emissivity only depends on the technology, not on the region
    # Note: Scope 2 emissivity depends on the technology and region

    df_c = df_c.loc[(df_c["region"] == region) & (df_c["year"] == year)]
    scope_label = ""
    if scope in {"s1_emissivity", "s2_emissivity", "s3_emissivity"}:
        df_c = df_c.loc[df_c["metric"] == scope]
        scope_label = scope
        color = "technology"
        color_map = dict(zip_longest(TECH_REFERENCE_LIST, MPP_COLOR_LIST))

    elif scope == "s1+s2":
        df_c = df_c.loc[
            (df_c["metric"] == "s1_emissivity") | (df_c["metric"] == "s2_emissivity")
        ]
        scope_label = "S1&S2 emissivity"
        color = "metric"
        color_map = BAR_CHART_ORDER_EMISSIVITY

    elif scope == "combined":
        df_c = df_c.loc[
            (df_c["metric"] == "s1_emissivity")
            | (df_c["metric"] == "s2_emissivity")
            | (df_c["metric"] == "s3_emissivity")
        ]
        scope_label = "combined emissivity"
        color = "metric"
        color_map = BAR_CHART_ORDER_EMISSIVITY

    text = f"{scope_label} - {region} - {year}" if region else f"{scope} - {year}"

    df_c["tech_order"] = df_c["technology"].map(sorterIndex)
    df_c.sort_values(["tech_order"], ascending=True, inplace=True)
    df_c.drop(labels="tech_order", axis=1, inplace=True)

    fig_ = px.bar(
        df_c,
        x="technology",
        y="value",
        color=color,
        color_discrete_map=color_map,
        text_auto=".2f",
        labels={"value": "[tCO2/t steel]"},
        title=text,
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_


def steel_emissions_line_chart(
    df: pd.DataFrame,
    filepath: str = None,
    region: str = None,
    scenario_name: str = None,
) -> px.line:
    """Creates an Area graph of Steel Production.

    Args:
        df (pd.DataFrame): A DataFrame of Production Stats.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.
        scenario_name (str): The name of the scenario at runtime.

    Returns:
        px.line: A plotly express area graph.
    """
    df_c = df.copy()
    df_c["s1_s2_emissions_mt"] = df_c["s1_emissions_mt"] + df_c["s2_emissions_mt"]
    filename = "scope_1_2_emissions"
    graph_title = "Scope 1 & 2 Emissions"
    if region:
        df_c = df_c[df_c["region"] == region]
        filename = f"{filename}_for_{region}"
        graph_title = f"{graph_title} - {region}"
    if scenario_name:
        graph_title = f"{graph_title} - {scenario_name} scenario"

    logger.info(f"Creating line graph output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"

    df_c = df_c.groupby("year").agg({"s1_s2_emissions_mt": "sum"}).reset_index()

    return line_chart(
        data=df_c,
        x="year",
        y="s1_s2_emissions_mt",
        color_discrete_map={"s1_s2_emissions_mt": "#59A270"},
        name=graph_title,
        x_axis="Year",
        y_axis="Scope 1 & 2 Emisions [Mt/year]",
        save_filepath=filename,
    )
