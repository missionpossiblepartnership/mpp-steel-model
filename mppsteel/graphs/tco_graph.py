from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.model_config import PKL_DATA_FINAL
from mppsteel.config.model_config import PKL_DATA_INTERMEDIATE
from mppsteel.config.reference_lists import MPP_COLOR_LIST, GRAPH_COL_ORDER

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import read_pickle_folder

from mppsteel.graphs.plotly_graphs import bar_chart



def generate_tco_charts (
    df: pd.DataFrame, year: int = None, region: str = None, 
    tech: str=None, save_filepath: str = None, ext: str = "png"
    ):

    df_c = df.copy()
    df_c = df_c.groupby(
        ['region','year','start_technology','end_technology'], 
        as_index=False).agg({"tco": 'mean'}).round(2)
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
        as_index=False).agg({"tco": 'mean'}).round(2)
        text = f'Global; TCO in {year}, switching from {tech} to...'

    df_c['switch_tech_rank'] = df_c['end_technology'].map(sorterIndex)
    df_c.sort_values(['switch_tech_rank'], ascending=True, inplace=True)
    df_c.drop('switch_tech_rank', 1, inplace=True)

    fig_ = px.bar(
        df_c,
        x='end_technology',
        y='tco',
        color= 'end_technology',
        text_auto='.2f',
        labels={'tco': '[$/t steel]'},
        title= text
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_
