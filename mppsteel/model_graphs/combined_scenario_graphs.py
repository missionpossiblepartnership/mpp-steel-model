"""Graph fpr the OPEX CAPEX split"""
from itertools import zip_longest

import pandas as pd
import plotly.express as px

from mppsteel.config.reference_lists import MPP_COLOR_LIST
from mppsteel.config.model_config import TERAWATT_TO_PETAJOULE_FACTOR, BILLION_NUMBER
from mppsteel.utility.log_utility import get_logger

from mppsteel.model_graphs.plotly_graphs import line_chart
from mppsteel.model_graphs.consumption_over_time import ENERGY_RESOURCES_COT

logger = get_logger(__name__)

def scenario_resource_usage(df: pd.DataFrame, resource: str, save_filepath: str = None, ext: str = "png") -> px.line:
    """Creates a plotly line chart that compares different scenarios usage of a specified `resource`.

    Args:
        df (pd.DataFrame): The combined production resource usage DataFrame
        resource (str): The resource to compare.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.line: A plotly express line graph figure.
    """
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



def combined_scenario_investment_chart(df: pd.DataFrame, save_filepath: str = None, ext: str = "png") -> px.line:
    """Creates a plotly line chart that compares different scenarios investment amounts.

    Args:
        df (pd.DataFrame): The combined investment DataFrame.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.line: A plotly express line graph figure.
    """
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


def create_total_energy_usage_df(production_resource_usage: pd.DataFrame) -> pd.DataFrame:
    """Formats the Production Resource DataFrame that sums all energy usage by year and scenario.
    Produces output in two different units: Petajoules and Exajoules.

    Args:
        production_resource_usage (pd.DataFrame): The combined production resource usage DataFrame

    Returns:
        pd.DataFrame: Formatted DataFrame.
    """
    cols_of_interest = ['year', 'region_rmi', 'scenario'] + ENERGY_RESOURCES_COT
    df_c = production_resource_usage[cols_of_interest].copy()
    df_c = df_c.groupby(['year', 'scenario']).agg('sum').sum(axis=1).reset_index()
    df_c = df_c.rename({0: 'total_energy_pj'}, axis=1)
    df_c['total_energy_ej'] = df_c['total_energy_pj'] / 1000
    return df_c


def combined_scenario_energy_usage_chart(df: pd.DataFrame, save_filepath: str = None, ext: str = "png") -> px.line:
    """Creates a plotly line chart that compares different energy amounts.

    Args:
        df (pd.DataFrame): The combined energy chart DataFrame.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.line: A plotly express line graph figure.
    """
    energy_df = create_total_energy_usage_df(df)
    scenarios = energy_df['scenario'].unique()
    color_mapper = dict(zip_longest(scenarios, MPP_COLOR_LIST))

    fig_ = line_chart(
        data=energy_df,
        x="year",
        y='total_energy_ej',
        color='scenario',
        color_discrete_map=color_mapper,
        name="Total Energy | Exajoules",
        x_axis="Year",
        y_axis='Total Energy [EJ]',
    )
    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_



def combined_scenario_emissions_chart(df: pd.DataFrame, cumulative: bool = False, save_filepath: str = None, ext: str = "png") -> px.line:
    """Creates a plotly line chart that compares different emissions of the various scenarios.

    Args:
        df (pd.DataFrame): The combined emissions usages DataFrame.
        cumulative (bool): A boolean that optionally creates a cumulative dataset.
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.line: A plotly express line graph figure.
    """
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
    """Handler function that takes the combined investment DataFrame and a filepath string and outputs a line graph. 

    Args:
        investment_df (pd.DataFrame): The combined investment DataFrame.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph figure.
    """
    filename = "combined_scenario_cumulative_investments"
    logger.info(f"Combined Scenario: Cumulative Investment Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return combined_scenario_investment_chart(investment_df, save_filepath=filename)


def create_combined_emissions_chart(
    emissions_df: pd.DataFrame, cumulative: bool = False, filepath: str = None) -> px.line:
    """Handler function that takes the combined emissions DataFrame and a filepath string and outputs a line graph. 

    Args:
        emissions_df (pd.DataFrame): The combined investment DataFrame.
        cumulative (bool): A boolean that optionally creates a cumulative dataset.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph figure.
    """
    filename = "combined_scenario_annual_emissions"
    if cumulative:
        filename = "combined_scenario_cumulative_emissions"
    logger.info(f"Combined Scenario: Emissions Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return combined_scenario_emissions_chart(emissions_df, cumulative=cumulative, save_filepath=filename)


def create_combined_resource_chart(
    production_df: pd.DataFrame, resource: str, filepath: str = None) -> px.line:
    """Creates a line graph for a specified resource across scenarios.

    Args:
        production_df (pd.DataFrame): The combined production resource usage DataFrame.
        resource (str): The resource to subset the combined production resource usage DataFrame.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph figure.
    """
    resource_name = resource.split('_')[0]
    filename = f"combined_scenario_{resource_name}_usage"
    logger.info(f"Combined Scenario: {resource_name} Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return scenario_resource_usage(production_df, resource, save_filepath=filename)


def create_total_energy_usage_chart(
    production_df: pd.DataFrame, filepath: str = None) -> px.line:
    """Creates a line graph for combined scenario energy consumption.

    Args:
        production_df (pd.DataFrame): The combined production resource usage DataFrame.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph figure.
    """
    filename = "combined_scenario_total_energy_usage"
    logger.info(f"Combined Scenario | Total Energy Usage | {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return combined_scenario_energy_usage_chart(production_df, save_filepath=filename)
