"""Creates graphs from model outputs"""
import pandas as pd

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger, timer_func
)

from mppsteel.model_config import (
    PKL_DATA_FINAL, OUTPUT_FOLDER
)

from mppsteel.results.plotly_graphs import (
    line_chart, area_chart, bar_chart, bar_chart_vertical, line_graph, ARCHETYPE_COLORS
)

# Create logger
logger = get_logger("Graph Production")

INITIAL_COLS = ['year', 'steel_plant', 'technology', 'capacity',
    'country_code', 'production', 'low_carbon_tech']

EMISSION_COLS = ['s1_emissions', 's2_emissions', 's3_emissions']

RESOURCE_COLS = ['bf_gas', 'bf_slag', 'bof_gas',
    'biomass', 'biomethane', 'cog', 'coke', 'dri', 'electricity',
    'hydrogen', 'iron_ore', 'met_coal', 'natural_gas', 'other_slag',
    'plastic_waste', 'process_emissions', 'scrap', 'steam', 'thermal_coal',
    'captured_co2', 'coal', 'used_co2', 'power', 'bioenergy']

REGION_COLS = ['region_wsa_region', 'region_continent', 'region_region']

SCENARIO_COLS = ['scenario_tech_moratorium', 'scenario_carbon_tax',
    'scenario_green_premium', 'scenario_electricity_cost_scenario',
    'scenario_hydrogen_cost_scenario', 'scenario_steel_demand_scenario']

CAPACITY_PRODUCTION_COLS = ['capacity', 'production']

def generate_production_emissions(df: pd.DataFrame, grouping_col: str, value_cols: list):
    df_c = df.copy()
    df_c = pd.melt(df, id_vars=['year', 'steel_plant', grouping_col], value_vars=value_cols, var_name='metric')
    df_c.reset_index(drop=True, inplace=True)
    return df_c.groupby([grouping_col, 'year', 'metric'], as_index=False).agg({"value": 'sum'}).round(2)

def generate_production_stats(df: pd.DataFrame, grouping_col: str, value_cols: list):
    df = pd.melt(df, id_vars=['year', 'steel_plant', grouping_col],value_vars=value_cols, var_name='metric')
    return df.reset_index(drop=True)

def generate_subset(df: pd.DataFrame, grouping_col: str, value_col: str, region_select: list = None):
    df_c = df.copy()
    df_c = pd.melt(df, id_vars=['year', 'steel_plant', grouping_col], value_vars=value_col, var_name='metric')
    df_c.reset_index(drop=True, inplace=True)
    if region_select != None:
        df_c=df_c.groupby([grouping_col, 'year', 'metric'], as_index=False).agg({"value": 'sum'}).round(2)
        df_c=df_c[df_c[grouping_col].isin(region_select)]
    else:
        df_c = df_c.groupby([grouping_col, 'year'],as_index=False).agg({"value": 'sum'}).round(2)
    return df_c


def create_area_chart_1(df: pd.DataFrame, filepath: str = None):
    filename = 'steel_production_per_technology'
    logger.info(f'Creating area graph output: {filename}')
    if filepath:
        filename = f'{filepath}/{filename}'
    return area_chart(
        data=generate_production_emissions(df, 'technology', ['production']),
        x='year',
        y='value',
        color='technology',
        name='Steel production per tech for run scenario',
        x_axis='year',
        y_axis='Steel Production',
        hoverdata=None,
        save_filepath=filename
    )


def create_area_chart_2(df: pd.DataFrame, filepath: str = None):
    filename = 'steel_production_emissions_per_technology'
    logger.info(f'Creating area graph output: {filename}')
    if filepath:
        filename = f'{filepath}/{filename}'
    return area_chart(
        data=generate_production_emissions(df, 'technology', EMISSION_COLS).groupby(['year', 'technology']).agg('sum').reset_index(),
        x='year',
        y='value',
        color='technology',
        name='Steel production emissions per tech for run scenario',
        x_axis='year',
        y_axis='Carbon Emissions',
        hoverdata=None,
        save_filepath=filename
    )

def create_line_graph_1(df: pd.DataFrame, resource: str, regions: list = None, filepath: str = None):
    region_list = regions
    filename = f'{resource}_multiregional_line_graph'
    if not regions:
        region_list = ['Global']
        filename = f'{resource}_global_line_graph'
    region_list = ', '.join(region_list)
    resource_string = resource.replace('_', ' ').capitalize()
    logger.info(f'Creating line graph output: {filename}')
    if filepath:
        filename = f'{filepath}/{filename}'
    return line_chart(
        data=generate_subset(df, 'region_wsa_region', resource, regions),
        x='year',
        y='value',
        color='region_wsa_region',
        name=f'{resource_string} consumption in {region_list}',
        x_axis='year',
        y_axis=resource_string,
        save_filepath=filename
    )

@timer_func
def create_graphs(filepath: str, ):
    production_stats_all = read_pickle_folder(PKL_DATA_FINAL, 'production_stats_all', 'df')
    production_emissions = read_pickle_folder(PKL_DATA_FINAL, 'production_emissions', 'df')
    investment_results = read_pickle_folder(PKL_DATA_FINAL, 'investment_results_df', 'df')

    create_area_chart_1(production_emissions, filepath)
    create_area_chart_2(production_emissions, filepath)
    create_line_graph_1(production_stats_all, 'electricity', ['EU + UK', 'China', 'India', 'USMCA'], filepath)

    for resource in RESOURCE_COLS:
        create_line_graph_1(df=production_stats_all, resource=resource, filepath=filepath)