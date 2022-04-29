"""New Plant Capacity Graph"""

from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.model_config import MEGATON_TO_KILOTON_FACTOR
from mppsteel.config.reference_lists import MPP_COLOR_LIST

from mppsteel.utility.log_utility import get_logger
from mppsteel.model_graphs.plotly_graphs import line_chart, bar_chart

logger = get_logger(__name__)

def create_new_capacity_subset(plant_df: pd.DataFrame, data_agg_type: str = 'sum'):
    df_c = plant_df.copy()
    new_plants = [plant_id for plant_id in df_c['plant_id'] if plant_id[:3] == 'MPP']
    new_plants_df = df_c[df_c['plant_id'].isin(new_plants)].copy()
    if data_agg_type == 'sum':
        new_plants_df = new_plants_df[['plant_name', 'start_of_operation', 'end_of_operation', 'plant_capacity', 'rmi_region']].groupby(['start_of_operation', 'rmi_region']).agg('sum')
    if data_agg_type == 'cumsum':
        regions = new_plants_df["rmi_region"].unique()
        region_dict = {}
        df_list = []
        for region in regions:
            calc = new_plants_df[new_plants_df["rmi_region"] == region].groupby(["start_of_operation"]).sum()
            region_dict[region] = calc.cumsum()
        for region_key in region_dict:
            df_r = region_dict[region_key]
            df_r["rmi_region"] = region_key
            df_list.append(df_r[["rmi_region", "plant_capacity"]])
        new_plants_df = pd.concat(df_list)
    
    new_plants_df['plant_capacity_kt'] = new_plants_df['plant_capacity'] / MEGATON_TO_KILOTON_FACTOR
    return new_plants_df.reset_index()

def new_plant_capacity_graph(
    plant_df: pd.DataFrame,
    graph_type: str,
    save_filepath: str = None,
    ext: str = "png",
):
    regions = plant_df['rmi_region'].unique()
    color_mapper = dict(zip_longest(regions, MPP_COLOR_LIST))
    fig_ = None

    if graph_type == 'line':
        fig_ = line_chart(
            data=create_new_capacity_subset(plant_df, 'cumsum'),
            x="start_of_operation",
            y='plant_capacity_kt',
            color='rmi_region',
            color_discrete_map=color_mapper,
            name=f"New Plant Capacity Per Region",
            x_axis="Year",
            y_axis='Plant Capacity [Mt]',
        )

    if graph_type == 'bar':
        fig_ = bar_chart(
            data=create_new_capacity_subset(plant_df, 'sum'),
            x="start_of_operation",
            y='plant_capacity_kt',
            color='rmi_region',
            color_discrete_map=color_mapper,
            xaxis_title="Year",
            yaxis_title="Plant Capacity [Mt]",
            title_text=f"New Plant Capacity Per Region",
        )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")
