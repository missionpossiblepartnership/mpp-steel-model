"""Creates graphs from model outputs"""
import pandas as pd

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger, timer_func
)

from mppsteel.model_config import (
    PKL_DATA_FINAL, OUTPUT_FOLDER
)

from mppsteel.results.plotly_graphs import (
    line_chart, area_chart, bar_chart, bar_chart_vertical
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


def create_area_chart_1(df: pd.DataFrame, timestamp: str):
    filename = 'area_chart_1'
    logger.info(f'Creating graph output: {filename}')
    if timestamp:
        filename = f'{OUTPUT_FOLDER}/{timestamp}/{filename}'
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


def create_area_chart_2(df: pd.DataFrame, timestamp: str):
    filename = 'area_chart_2'
    logger.info(f'Creating graph output: {filename}')
    if timestamp:
        filename = f'{OUTPUT_FOLDER}/{timestamp}/{filename}'
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


@timer_func
def create_graphs(timestamp: str):
    production_stats_all = read_pickle_folder(PKL_DATA_FINAL, 'production_stats_all', 'df')
    production_emissions = read_pickle_folder(PKL_DATA_FINAL, 'production_emissions', 'df')
    investment_results = read_pickle_folder(PKL_DATA_FINAL, 'investment_results_df', 'df')

    tech_emissions = generate_production_emissions(production_emissions, 'technology', EMISSION_COLS)
    region_emissions = generate_production_emissions(production_emissions, 'region_wsa_region', EMISSION_COLS)
    tech_prod_cap = generate_production_emissions(production_emissions, 'technology', CAPACITY_PRODUCTION_COLS)
    region_prod_cap = generate_production_emissions(production_emissions, 'region_wsa_region', CAPACITY_PRODUCTION_COLS)

    create_area_chart_1(production_emissions, timestamp)
    create_area_chart_2(production_emissions, timestamp)
