"""Graph fpr the OPEX CAPEX split"""
from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.model_config import PKL_DATA_FINAL
from mppsteel.config.reference_lists import MPP_COLOR_LIST

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import read_pickle_folder

from mppsteel.graphs.plotly_graphs import bar_chart


def format_cot_graph(
    df: pd.DataFrame, regions: list = None, resource_list: list = None
) -> pd.DataFrame:
    df_c = df.copy()
    df_c = pd.melt(
        df,
        id_vars=["year", "region_wsa_region"],
        value_vars=resource_list,
        var_name="metric",
    )
    df_c.reset_index(drop=True, inplace=True)
    df_c = (
        df_c.groupby(["region_wsa_region", "year", "metric"], as_index=False)
        .agg({"value": "sum"})
        .round(2)
    )
    if regions:
        df_c = df_c.loc[df_c["region_wsa_region"].isin(regions)]
    else:
        df_c = (
            df_c.groupby(["year", "metric"], as_index=False)
            .agg({"value": "sum"})
            .round(2)
        )
    return df_c


def consumption_over_time_graph(
    regions: list = None, save_filepath: str = None, ext: str = "png"
) -> px.bar:

    resources = [
        "biomass",
        "biomethane",
        "electricity",
        "hydrogen",
        "thermal_coal",
        "met_coal",
        "natural_gas",
    ]

    production_resource_usage = read_pickle_folder(
        PKL_DATA_FINAL, "production_resource_usage", "df"
    )
    production_resource_usage = format_cot_graph(
        production_resource_usage, regions, resource_list=resources
    )

    color_mapper = dict(zip_longest(resources, MPP_COLOR_LIST))

    fig_ = bar_chart(
        data=production_resource_usage,
        x="year",
        y="value",
        color="metric",
        color_discrete_map=color_mapper,
        xaxis_title="Year",
        yaxis_title="[PJ/year]",
        title_text="Consumption chart",
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_
