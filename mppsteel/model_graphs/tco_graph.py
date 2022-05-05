"""Script to create the TCO graph"""
from itertools import zip_longest
import pandas as pd
import plotly.express as px

from mppsteel.config.reference_lists import GRAPH_COL_ORDER, MPP_COLOR_LIST, TECH_REFERENCE_LIST

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)

def generate_tco_charts(
    df: pd.DataFrame, year: int = None, region: str = None, 
    tech: str=None, save_filepath: str = None, ext: str = "png"
    ) -> px.bar:
    """Creates a graph showing the TCO for a specific year. Optionally can make the graph more specific according to a specified region and technology. 

    Args:
        df (pd.DataFrame): _description_
        year (int, optional): The year to subset the DataFrame. Defaults to None.
        region (str, optional): The region to subset the Data. Defaults to None.
        tech (str, optional): The technology to subset the data. Defaults to None.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.bar: A plotly express bar graph.
    """

    cost_value_metric = "tco_regular_capex"
    df_c = df.copy()
    df_c = df_c.groupby(
        ['region','year','start_technology','end_technology'], 
        as_index=False).agg({cost_value_metric: 'mean'}).round(2)
    df_c.reset_index(drop=True, inplace=True)
    sorterIndex = dict(zip(GRAPH_COL_ORDER, range(len(GRAPH_COL_ORDER))))
    df_c = df_c.loc[(df_c['year'] == year) & (df_c['start_technology'] == tech)]
    text = ''

    if region:
        df_c = df_c.loc[df_c['region'] == region]
        text = f'{region}; TCO in {year}, switching from {tech} to...'

    else:
        df_c = df_c.groupby(
        ['year','start_technology','end_technology'], 
        as_index=False).agg({cost_value_metric: 'mean'}).round(2)
        text = f'Global; TCO in {year}, switching from {tech} to...'

    df_c['switch_tech_rank'] = df_c['end_technology'].map(sorterIndex)
    df_c.sort_values(['switch_tech_rank'], ascending=True, inplace=True)
    df_c.drop(labels='switch_tech_rank', axis=1, inplace=True)

    color_map = dict(zip_longest(TECH_REFERENCE_LIST, MPP_COLOR_LIST))

    fig_ = px.bar(
        df_c,
        x='end_technology',
        y=cost_value_metric,
        color= 'end_technology',
        color_discrete_map=color_map,
        text_auto='.2f',
        labels={cost_value_metric: '[$/t steel]'},
        title= text
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_
