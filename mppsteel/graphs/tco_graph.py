from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.model_config import PKL_DATA_FINAL
from mppsteel.config.model_config import PKL_DATA_INTERMEDIATE
from mppsteel.config.reference_lists import MPP_COLOR_LIST

from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.file_handling_utility import read_pickle_folder

from mppsteel.graphs.plotly_graphs import bar_chart

def generate_tco_charts (df: pd.DataFrame, year: int = None, region: str = None, tech: str=None, save_filepath: str = None, ext: str = "png"):
    
    region_list= region

    region_list = ', '.join(region_list)
    
    df_c=df.copy()
    
    df_c=df_c.groupby(['region','year','start_technology','end_technology'], as_index=False).agg({"tco": 'mean'}).round(2)
    df_c.reset_index(drop=True, inplace=True)
    print(df_c)
    sorter=["Avg BF-BOF","BAT BF-BOF","DRI-EAF",
    "BAT BF-BOF_H2 PCI","BAT BF-BOF_bio PCI","DRI-EAF_50% bio-CH4","DRI-EAF_50% green H2","DRI-Melt-BOF","Smelting Reduction",
    "BAT BF-BOF+CCUS","BAT BF-BOF+CCU","BAT BF-BOF+BECCUS","DRI-EAF+CCUS","DRI-EAF_100% green H2","DRI-Melt-BOF+CCUS","DRI-Melt-BOF_100% zero-C H2","Electrolyzer-EAF","Electrowinning-EAF","Smelting Reduction+CCUS",
    "EAF"]
    sorterIndex = dict(zip(sorter, range(len(sorter))))
    
    if region:
        df_c =df_c.loc[(df_c['region']== region)& (df_c['year']==(year))]
        print(df_c)
        df_c=df_c.loc[(df_c['start_technology']==tech)]
        print(df_c)
        df_c['switch_tech_rank']=df_c['end_technology'].map(sorterIndex)
        df_c.sort_values(['switch_tech_rank'], ascending=True, inplace=True)
        df_c.drop('switch_tech_rank',1, inplace=True)
        print(df_c)
        t= f'{region}; TCO in {year}, switching from {tech} to...'
        
    else:
        df_c=df_c.groupby(['year','start_technology','end_technology'],as_index=False).agg({"tco": 'mean'}).round(2)
        df_c=df_c.loc[(df_c['start_technology']==tech)&(df_c['year']==(year))]
        print(df_c)
        df_c['switch_tech_rank']=df_c['end_technology'].map(sorterIndex)
        df_c.sort_values(['switch_tech_rank'], ascending=True, inplace=True)
        df_c.drop('switch_tech_rank',1, inplace=True)
        print(df_c)
        t= f'Global; TCO in {year}, switching from {tech} to...'
        
    fig_ = px.bar(
        df_c,
        x='end_technology',
        y='tco',
        color= 'end_technology',
        text_auto='.2f',
        labels={'tco': '[$/t steel]'},
        title= t
        
        
    )

    

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")
    return fig_