"""Graph fpr the OPEX CAPEX split"""
from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.reference_lists import MPP_COLOR_LIST
from mppsteel.config.model_config import TERAWATT_TO_PETAJOULE_FACTOR, BILLION_NUMBER
from mppsteel.utility.log_utility import get_logger

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.model_graphs.plotly_graphs import line_chart
from mppsteel.utility.file_handling_utility import read_pickle_folder, get_scenario_pkl_path

logger = get_logger(__name__)

def scenario_resource_usage(df: pd.DataFrame, resource: str, save_filepath: str = None, ext: str = "png"):
    name = ''
    if 'electricity' in resource:
        name = 'Electric power'
    if 'hydrogen' in resource:
        name = 'Hydrogen'

    df_c = df[['year', 'scenario', resource]].groupby(['year', 'scenario']).agg('sum').reset_index().copy()
    resource_split = resource.split('_')
    resource_twh = f'{resource_split[0]}_TWh'
    df_c[resource_twh] = df_c[resource] / TERAWATT_TO_PETAJOULE_FACTOR
    
    scenarios = df_c['scenario'].unique()
    color_mapper = dict(zip_longest(scenarios, MPP_COLOR_LIST))

    fig_ = line_chart(
            data=df_c,
            x="year",
            y=resource_twh,
            color='scenario',
            color_discrete_map=color_mapper,
            name=f"{name} | TWh/year",
            x_axis="year",
            y_axis=resource_twh,
        )
    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_



def combined_scenario_investment_chart(df: pd.DataFrame, save_filepath: str = None, ext: str = "png"):
    df_c = df[['year', 'scenario', 'capital_cost']].groupby(['year', 'scenario']).sum().groupby(level=1).cumsum().reset_index().copy()
    df_c['capital_cost'] = df_c['capital_cost'] / BILLION_NUMBER

    scenarios = df_c['scenario'].unique()
    color_mapper = dict(zip_longest(scenarios, MPP_COLOR_LIST))

    fig_ = line_chart(
        data=df_c,
        x="year",
        y='capital_cost',
        color='scenario',
        color_discrete_map=color_mapper,
        name="Cumulative investment | bn $/year",
        x_axis="year",
        y_axis='cumulative_investment',
    )
    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_



def combined_scenario_emissions_chart(df: pd.DataFrame, cumulative: bool = False, save_filepath: str = None, ext: str = "png"):
    unit = 'mt'
    name = 'Annual emissions (scope 1 + 2) | Mt'
    if cumulative:
        unit = 'gt'
        name = 'Cumulative emissions (scope 1 + 2) | Gt'
    s1_emissions_label = f's1_emissions_{unit}'
    s2_emissions_label = f's2_emissions_{unit}'
    s1_s2_emissions_label = f's1_s2_emissions_{unit}'
    df_c = df[['year', s1_emissions_label, s2_emissions_label, 'scenario']].copy()
    df_c[s1_s2_emissions_label] = df_c[s1_emissions_label] + df_c[s2_emissions_label]
    df_c.drop([s1_emissions_label, s2_emissions_label], axis=1, inplace=True)
    final_df = df_c.groupby(['year', 'scenario']).sum().reset_index()
    if cumulative:
        final_df = df_c.groupby(['year', 'scenario']).sum().groupby(level=1).cumsum().reset_index().copy()

    scenarios = final_df['scenario'].unique()
    color_mapper = dict(zip_longest(scenarios, MPP_COLOR_LIST))
    
    fig_ = line_chart(
        data=final_df,
        x="year",
        y=s1_s2_emissions_label,
        color='scenario',
        color_discrete_map=color_mapper,
        name=name,
        x_axis="year",
        y_axis=s1_s2_emissions_label,
    )
    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_


def create_combined_investment_chart(
    investment_df: pd.DataFrame, filepath: str = None) -> px.line:
    filename = "combined_scenario_cumulative_investments"
    logger.info(f"Combined Scenario: Cumulative Investment Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return combined_scenario_investment_chart(investment_df, save_filepath=filename)


def create_combined_emissions_chart(
    emissions_df: pd.DataFrame, cumulative: bool = False, filepath: str = None) -> px.line:
    filename = "combined_scenario_annual_emissions"
    
    if cumulative:
        filename = "combined_scenario_cumulative_emissions"
    logger.info(f"Combined Scenario: Emissions Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return combined_scenario_emissions_chart(emissions_df, cumulative=cumulative, save_filepath=filename)


def create_combined_energy_chart(
    production_df: pd.DataFrame, resource: str, filepath: str = None) -> px.line:
    resource_name = resource.split('_')[0]
    filename = f"combined_scenario_{resource_name}_usage"
    logger.info(f"Combined Scenario: {resource_name} Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return scenario_resource_usage(production_df, resource, save_filepath=filename)
