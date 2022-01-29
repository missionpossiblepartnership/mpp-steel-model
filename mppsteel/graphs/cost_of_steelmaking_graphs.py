"""Graph for (Levelised) Cost of Steelmaking"""
from itertools import zip_longest

import pandas as pd

from mppsteel.model_config import PKL_DATA_FINAL
from mppsteel.utility.reference_lists import TECH_REFERENCE_LIST

from mppsteel.utility.utils import (
    read_pickle_folder,
)

from mppsteel.graphs.plotly_graphs import bar_chart

def get_lcos_lowest_vals(df: pd.DataFrame, chosen_year: int, value_col: str):
    df_c = df.copy()
    df_c.rename(mapper={'levelised_cost_of_steelmaking': value_col}, axis=1, inplace=True)
    df_s = df_c.set_index(['year', 'technology', 'country_code']).copy()
    df_y = df_s.loc[chosen_year]
    tech_delta_dict = {}
    tech_list = []
    for technology in TECH_REFERENCE_LIST:
        df_t = df_y.loc[technology]
        min_region = df_t.idxmin().values[0]
        min_val = df_t[value_col].min()
        max_val = df_t[value_col].max()
        tech_delta_dict[technology] = max_val - min_val
        df_subset = df_c[(df_c['year'] == chosen_year) & (df_c['technology'] == technology) & (df_c['country_code'] == min_region)]
        tech_list.append(df_subset)
    df_combined = pd.concat(tech_list).set_index(['technology'])
    df_combined.drop(['year', 'country_code'], axis=1, inplace=True)
    return df_combined, tech_delta_dict

def assign_country_deltas(df: pd.DataFrame, delta_dict: dict):
    df_c = df.copy()
    tech_values = df_c.index.get_level_values(0).unique()
    for technology in tech_values:
        df_c.loc[technology, 'LCOS delta'] = delta_dict[technology]
    return df_c.reset_index().melt(id_vars=['technology'], var_name='cost_type', value_name='cost')


def lcos_graph(chosen_year:int=2030, save_filepath:str=None, ext:str='png'):

    lcos_data = read_pickle_folder(PKL_DATA_FINAL, "levelised_cost_of_steelmaking", "df")
    lcos_c, country_deltas = get_lcos_lowest_vals(lcos_data, chosen_year, 'LCOS')
    lcos_c = assign_country_deltas(lcos_c, country_deltas)

    bar_chart_order = {
        'LCOS': '#E76B67',
        'LCOS delta': '#1E3B63'
    }

    fig_ = bar_chart(
        data=lcos_c,
        x='technology',
        y='cost',
        color='cost_type',
        color_discrete_map=bar_chart_order,
        xaxis_title='Technology',
        yaxis_title='Levelised Cost of Steel [$/tCS]',
        title_text=f'Levelised cost of steel by end-state compatible  technology in {chosen_year}'
    )

    if save_filepath:
        fig_.write_image(f'{save_filepath}.{ext}')

    return fig_
