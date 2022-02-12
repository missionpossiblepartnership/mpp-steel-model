"""Investment Graph"""

from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.model_config import PKL_DATA_FINAL
from mppsteel.config.reference_lists import MPP_COLOR_LIST, TECH_REFERENCE_LIST
from mppsteel.results.investments import create_inv_stats

from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.utility.log_utility import get_logger
from mppsteel.graphs.plotly_graphs import line_chart, bar_chart


def investment_line_chart(
    group: str = "global",
    operation: str = "cumsum",
    save_filepath: str = None,
    ext: str = "png",
) -> px.line:
    """Creates a line graph showing the level of investment across all technologies.

    Args:
        group (str, optional): The group you want: 'global' OR 'regional'. Defaults to "global".
        operation (str, optional): The operation you want to perform on the DataFrame 'sum' or 'cumsum'. Defaults to "cumsum".
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.line: A plotly express line graph.
    """
    investment_results = read_pickle_folder(PKL_DATA_FINAL, "investment_results", "df")
    data = create_inv_stats(
        investment_results, results=group, operation=operation, agg=False
    )

    fig_ = line_chart(
        data=data,
        x="year",
        y="capital_cost",
        color=None,
        name="Investment Over Time",
        x_axis="Year",
        y_axis="Capital Cost",
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")


def investment_per_tech(save_filepath: str = None, ext: str = "png") -> px.bar:
    """Creates a bar graph showing the level of investment per technology.

    Args:
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.bar: A Plotly express bar chart.
    """
    investment_results = read_pickle_folder(PKL_DATA_FINAL, "investment_results", "df")
    tech_investment = (
        investment_results.groupby(["end_tech", "region_wsa_region"])
        .agg({"capital_cost": "sum"})
        .reset_index()
        .copy()
    )
    tech_inv_color_map = dict(
        zip_longest(tech_investment["end_tech"].unique(), MPP_COLOR_LIST)
    )

    fig_ = bar_chart(
        data=tech_investment,
        x="end_tech",
        y="capital_cost",
        color="region_wsa_region",
        color_discrete_map=tech_inv_color_map,
        array_order=TECH_REFERENCE_LIST,
        xaxis_title="End Technology",
        yaxis_title="Capital Cost",
        title_text="Capital Investment Per Technology (Regional Split)",
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_
