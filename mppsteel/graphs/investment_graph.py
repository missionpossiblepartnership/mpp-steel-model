"""Investment Graph"""

from itertools import zip_longest

import pandas as pd
from mppsteel.model_config import PKL_DATA_FINAL
from mppsteel.utility.reference_lists import MPP_COLOR_LIST, TECH_REFERENCE_LIST
from mppsteel.results.investments import create_inv_stats

from mppsteel.utility.utils import read_pickle_folder
from mppsteel.utility.log_utility import get_logger
from mppsteel.graphs.plotly_graphs import line_chart, bar_chart

def investment_line_chart(group: str = 'global', operation: str = 'cumsum', save_filepath:str=None, ext:str='png'):
    investment_results = read_pickle_folder(PKL_DATA_FINAL, "investment_results", "df")
    data = create_inv_stats(investment_results, results=group, operation=operation, agg=False)

    fig_ = line_chart(
        data=data,
        x='year',
        y='capital_cost',
        color=None,
        name='Investment Over Time',
        x_axis='Year',
        y_axis='Capital Cost',
    )

    if save_filepath:
        fig_.write_image(f'{save_filepath}.{ext}')

def investment_per_tech(save_filepath:str=None, ext:str='png'):
    investment_results = read_pickle_folder(PKL_DATA_FINAL, "investment_results", "df")
    tech_investment = investment_results.groupby(['end_tech', 'region_wsa_region']).agg({'capital_cost': 'sum'}).reset_index().copy()
    tech_inv_color_map = dict(zip_longest(tech_investment['end_tech'].unique(), MPP_COLOR_LIST))

    fig_ = bar_chart(
        data=tech_investment,
        x='end_tech',
        y='capital_cost',
        color='region_wsa_region',
        color_discrete_map=tech_inv_color_map,
        array_order=TECH_REFERENCE_LIST,
        xaxis_title='End Technology',
        yaxis_title='Capital Cost',
        title_text='Capital Investment Per Technology (Regional Split)',
    )

    if save_filepath:
        fig_.write_image(f'{save_filepath}.{ext}')
