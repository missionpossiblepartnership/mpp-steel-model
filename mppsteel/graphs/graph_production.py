"""Creates graphs from model outputs"""
import itertools
import pandas as pd
import plotly.express as px
from mppsteel.config.model_config import PKL_DATA_FORMATTED, PKL_DATA_IMPORTS, MAIN_REGIONAL_SCHEMA
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import read_pickle_folder, get_scenario_pkl_path
from mppsteel.utility.log_utility import get_logger
from mppsteel.graphs.plotly_graphs import (
    line_chart,
    area_chart,
)
from mppsteel.graphs.opex_capex_graph import opex_capex_graph, opex_capex_graph_regional
from mppsteel.graphs.consumption_over_time import consumption_over_time_graph
from mppsteel.graphs.cost_of_steelmaking_graphs import lcost_graph
from mppsteel.graphs.investment_graph import investment_line_chart, investment_per_tech
from mppsteel.graphs.emissions_per_tech import generate_emissivity_charts
from mppsteel.graphs.tco_graph import generate_tco_charts

logger = get_logger(__name__)

INITIAL_COLS = [
    "year",
    "plant_name",
    "technology",
    "capacity",
    "country_code",
    "production",
    "low_carbon_tech",
]

EMISSION_COLS = ["s1_emissions", "s2_emissions", "s3_emissions"]

RESOURCE_COLS = [
    "bf_gas",
    "bf_slag",
    "bof_gas",
    "biomass",
    "biomethane",
    "cog",
    "coke",
    "dri",
    "electricity",
    "hydrogen",
    "iron_ore",
    "met_coal",
    "natural_gas",
    "other_slag",
    "plastic_waste",
    "process_emissions",
    "scrap",
    "steam",
    "thermal_coal",
    "captured_co2",
    "coal",
    "used_co2",
    "bioenergy",
]

SCENARIO_COLS = [
    "scenario_tech_moratorium",
    "scenario_carbon_tax",
    "scenario_green_premium",
    "scenario_electricity_cost_scenario",
    "scenario_hydrogen_cost_scenario",
    "scenario_biomass_cost_scenario",
    "scenario_steel_demand_scenario",
]

CAPACITY_PRODUCTION_COLS = ["capacity", "production"]


def generate_production_emissions(
    df: pd.DataFrame, grouping_col: str, value_cols: list
) -> pd.DataFrame:
    """Produces the DataFrame to be used to created the emissions graph.

    Args:
        df (pd.DataFrame): A DataFrame of the production emissions.
        grouping_col (str): A region column for grouping the value columns.
        value_cols (list): The columns you want to use as values (S1, S2, S3, Combined).

    Returns:
        pd.DataFrame: An aggregated DataFrame by the `grouping col`.
    """
    df_c = df.copy()
    df_c = pd.melt(
        df,
        id_vars=["year", "plant_name", grouping_col],
        value_vars=value_cols,
        var_name="metric",
    )
    df_c.reset_index(drop=True, inplace=True)
    return (
        df_c.groupby([grouping_col, "year", "metric"], as_index=False)
        .agg({"value": "sum"})
        .round(2)
    )


def generate_production_stats(
    df: pd.DataFrame, grouping_col: str, value_cols: list
) -> pd.DataFrame:
    """[summary]

    Args:
        df (pd.DataFrame): A DataFrame of production statistics.
        grouping_col (str): A region column for grouping the value columns.
        value_cols (list): The columns you want to use as values (resources).

    Returns:
        pd.DataFrame: An aggregated DataFrame by the `grouping col`.
    """
    df = pd.melt(
        df,
        id_vars=["year", "plant_name", grouping_col],
        value_vars=value_cols,
        var_name="metric",
    )
    return df.reset_index(drop=True)


def generate_subset(
    df: pd.DataFrame, grouping_col: str, value_col: str, region_select: list = None
) -> pd.DataFrame:
    """Subsets a Production DataFrame based on parameters.

    Args:
        df (pd.DataFrame):
        grouping_col (str): A region column for grouping the value columns.
        value_col (str): The columns you want to use as values (resources).
        region_select (list, optional): The list of regions you want to match. Defaults to None.

    Returns:
        pd.DataFrame: An aggregated DataFrame by the `grouping col`.
    """
    df_c = df.copy()
    df_c = pd.melt(
        df,
        id_vars=["year", "plant_name", grouping_col],
        value_vars=value_col,
        var_name="metric",
    )
    df_c.reset_index(drop=True, inplace=True)
    if region_select != None:
        df_c = (
            df_c.groupby([grouping_col, "year", "metric"], as_index=False)
            .agg({"value": "sum"})
            .round(2)
        )
        df_c = df_c[df_c[grouping_col].isin(region_select)]
    else:
        df_c = (
            df_c.groupby([grouping_col, "year"], as_index=False)
            .agg({"value": "sum"})
            .round(2)
        )
    return df_c


def steel_production_area_chart(df: pd.DataFrame, filepath: str = None, region: str = None, scenario_name: str = None) -> px.area:
    """Creates an Area graph of Steel Production.

    Args:
        df (pd.DataFrame): A DataFrame of Production Stats.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.
        region (str, optional): The region you want to model. Defaults to None.

    Returns:
        px.area: A plotly express area graph.
    """
    filename = "steel_production_per_technology"
    graph_title = "Steel production per tech"
    df_c = df.copy()
    region_list = df_c[MAIN_REGIONAL_SCHEMA].unique()
    if region and (region in region_list):
        graph_title = f"{graph_title} - {region}"
        df_c = df_c[df_c[MAIN_REGIONAL_SCHEMA] == region]
        filename = f'{filename}_for_{region}'
    elif region and (region not in region_list):
        raise ValueError(f'Incorrect region listed {region}')
    logger.info(f"Creating area graph output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    if scenario_name:
        graph_title = f"{graph_title} - {scenario_name} scenario"
    return area_chart(
        data=generate_production_emissions(df_c, "technology", ["production"]),
        x="year",
        y="value",
        color="technology",
        name=graph_title,
        x_axis="year",
        y_axis="Steel Production (Mt)",
        hoverdata=None,
        save_filepath=filename,
    )


def emissions_area_chart(
    df: pd.DataFrame, filepath: str = None, scope: str = "combined"
) -> px.area:
    """Creates an Emissions Area chart.

    Args:
        df (pd.DataFrame): The Production Emissions area graph.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.
        scope (str, optional): The scope of the emissions you want to graph (S1, S2, S3, combined). Defaults to "combined".

    Returns:
        px.area: A plotly express area graph.
    """
    scope_mapper = dict(zip(["s1", "s2", "s3"], EMISSION_COLS))
    emission_cols = EMISSION_COLS
    if scope in scope_mapper:
        emission_cols = [scope_mapper[scope]]
    filename_string = "".join(emission_cols)
    filename = f"{scope}_emissions_per_technology"
    logger.info(f"Creating area graph output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"

    return area_chart(
        data=generate_production_emissions(df, "technology", emission_cols)
        .groupby(["year", "technology"])
        .agg("sum")
        .reset_index(),
        x="year",
        y="value",
        color="technology",
        name="Steel production emissions per tech for run scenario",
        x_axis="year",
        y_axis="Carbon Emissions",
        hoverdata=None,
        save_filepath=filename,
    )


def steel_emissions_line_chart(df: pd.DataFrame, filepath: str = None, region: str = None, scenario_name: str = None) -> px.line:
    """Creates an Area graph of Steel Production.

    Args:
        df (pd.DataFrame): A DataFrame of Production Stats.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.area: A plotly express area graph.
    """
    
    df_c = df.copy()
    df_c['s1_s2_emissions'] = df_c['s1_emissions'] + df_c['s2_emissions']
    filename = "scope_1_2_emissions"
    graph_title = "Scope 1 & 2 Emissions"
    if region:
        df_c = df_c[df_c[MAIN_REGIONAL_SCHEMA] == region]
        filename = f'{filename}_for_{region}'
        graph_title = f'{graph_title} - {region}'
    if scenario_name:
        graph_title = f"{graph_title} - {scenario_name} scenario"
    
    logger.info(f"Creating line graph output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"

    df_c = df_c.groupby('year').agg({'s1_s2_emissions': 'sum'}).reset_index()

    return line_chart(
        data=df_c,
        x="year",
        y="s1_s2_emissions",
        color=None,
        name=graph_title,
        x_axis="Year",
        y_axis="Scope 1 & 2 Emisions",
        save_filepath=filename,
    )


def resource_line_charts(
    df: pd.DataFrame, resource: str, regions: list = None, filepath: str = None
) -> px.line:
    """[summary]

    Args:
        df (pd.DataFrame): The Production Stats DataFrame.
        resource (str): The name of the resource you want to graph.
        regions (list, optional): The region(s) you want to graph. Defaults to None.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph.
    """
    region_list = regions
    filename = f"{resource}_multiregional_line_graph"
    if not regions:
        region_list = ["Global"]
        filename = f"{resource}_global_line_graph"
    region_list = ", ".join(region_list)
    resource_string = resource.replace("_", " ").capitalize()
    logger.info(f"Creating line graph output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return line_chart(
        data=generate_subset(df, MAIN_REGIONAL_SCHEMA, resource, regions),
        x="year",
        y="value",
        color=MAIN_REGIONAL_SCHEMA,
        name=f"{resource_string} consumption in {region_list}",
        x_axis="year",
        y_axis=resource_string,
        save_filepath=filename,
    )


def create_opex_capex_graph(
    variable_cost_df: pd.DataFrame, capex_dict: dict, country_mapper: dict, filepath: str = None) -> px.bar:
    """Creates a Opex Capex split graph.

    Args:
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = "opex_capex_graph_2050"
    logger.info(f"Creating Opex Capex Graph Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return opex_capex_graph(variable_cost_df, capex_dict, country_mapper, save_filepath=filename)

def create_opex_capex_graph_regional(vcsmb: pd.DataFrame, capex_dict: dict, country_mapper: dict, filepath: str = None, year: int = 2050, region: str = 'NAFTA') -> px.bar:
    """Creates a Opex Capex split graph.
    Args:
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.
    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = f"{region}_opex_capex_graph_{year}"
    logger.info(f"Creating Opex Capex Graph Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return opex_capex_graph_regional(vcsmb, capex_dict, country_mapper, save_filepath=filename, year=year, region=region)


def create_investment_line_graph(
    investment_results: pd.DataFrame, group: str, operation: str, filepath: str = None
) -> px.line:
    """Creates a line graph showing the level of investment across all technologies and saves it.

    Args:
        group (str, optional): The group you want: 'global' OR 'regional'. Defaults to "global".
        operation (str, optional): The operation you want to perform on the DataFrame 'sum' or 'cumsum'. Defaults to "cumsum".
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph.
    """
    filename = f"investment_graph_{group}_{operation}"
    logger.info(f"Regional Investment Graph Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return investment_line_chart(
        investment_results, group=group, operation=operation, save_filepath=filename
    )


def create_investment_per_tech_graph(investment_results: pd.DataFrame, filepath: str = None) -> px.bar:
    """Creates a line graph showing the level of investment across all technologies.

    Args:
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = "investment_graph_per_technology"
    logger.info(f"Technology Investment Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return investment_per_tech(investment_results, save_filepath=filename)


def create_cot_graph(production_resource_usage: pd.DataFrame, regions: list = None, filepath: str = None) -> px.bar:
    """Generates a Graph showing the consumption over time of a material resource.

    Args:
        regions (list, optional): The regions you want to graph. Defaults to None.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    region_ref = "global"
    filename = "consumption_over_time"
    if regions:
        region_ref = ", ".join(regions)
        filename = f"{filename}_for_{region_ref}"
    logger.info(f"Consumption Over Time Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return consumption_over_time_graph(production_resource_usage, regions=regions, save_filepath=filename)


def create_lcost_graph(lcost_df: pd.DataFrame, chosen_year: int, filepath: str = None) -> px.bar:
    """Creates a bar graph for the Levelised Cost.

    Args:
        chosen_year (int): The year you want to set the Lcost values in the graph.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = "levelised_cost"
    logger.info(f"Levelised Cost Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return lcost_graph(lcost_df, chosen_year=chosen_year, save_filepath=filename)

def create_tco_graph(
    df: pd.DataFrame, year: int = None, region: str = None, 
    tech: str = None, filepath: str = None) -> px.bar:
    """_summary_

    Args:
        filepath (str, optional): _description_. Defaults to None.

    Returns:
        px.bar: _description_
    """
    filename = f'TCO_{year}_from_{tech}'
    logger.info(f"TCO Output: {filename}")
    if region:
        filename = f'TCO_{year}_in_{region}_from_{tech}_'
    if filepath:
        filename = f"{filepath}/{filename}"
    
    return generate_tco_charts(df, year,region, tech, save_filepath=filename)


def create_emissions_graph(df: pd.DataFrame, year: int = None, region: str = None, scope: str= None, filepath: str =None) -> px.bar:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        year (int, optional): _description_. Defaults to None.
        region (str, optional): _description_. Defaults to None.
        scope (str, optional): _description_. Defaults to None.
        filepath (str, optional): _description_. Defaults to None.

    Returns:
        px.bar: _description_
    """
    filename = f'emissions_chart {scope}'
    if region:
        filename = f'emissions_chart_{scope}_in_{region}'
    logger.info(f"Emissions_chart: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return generate_emissivity_charts(df, year, region, scope, save_filepath=filename)

@timer_func
def create_graphs(filepath: str, scenario_dict: dict) -> None:
    """Graph creation flow.

    Args:
        filepath (str): The folder path you want to save the chart to. Defaults to None.
    """
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    final_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'final')
    production_resource_usage = read_pickle_folder(
        final_path, "production_resource_usage", "df"
    )
    production_emissions = read_pickle_folder(
        final_path, "production_emissions", "df"
    )
    tco_ref = read_pickle_folder(
        intermediate_path, "tco_summary_data", "df"
    )
    calculated_emissivity_combined_df = read_pickle_folder(
        intermediate_path, "calculated_emissivity_combined", "df"
    )
    lcost_data = read_pickle_folder(
        intermediate_path, "levelized_cost_updated", "df"
    )
    investment_results = read_pickle_folder(final_path, "investment_results", "df")
    capex_dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    variable_cost_df = read_pickle_folder(
        intermediate_path, "variable_costs_regional_material_breakdown", "df"
    )
    rmi_mapper = create_country_mapper()

    steel_production_area_chart(
        production_emissions,
        filepath=filepath,
        scenario_name=scenario_dict['scenario_name']
    )
    for region in ['India', 'China', 'NAFTA', 'Europe', 'Japan, South Korea, and Taiwan']:
        steel_production_area_chart(
            df=production_emissions,
            filepath=filepath,
            region=region,
            scenario_name=scenario_dict['scenario_name']
        )
        steel_emissions_line_chart(
            df=production_emissions,
            filepath=filepath,
            region=region,
            scenario_name=scenario_dict['scenario_name']
        )

    resource_line_charts(
        production_resource_usage,
        "electricity",
        ["Europe", "China", "India", "NAFTA"],
        filepath,
    )

    for scope in ["s1", "s2", "s3", "combined"]:
        emissions_area_chart(production_emissions, filepath, scope)

    for resource in RESOURCE_COLS:
        resource_line_charts(
            df=production_resource_usage, resource=resource, filepath=filepath
        )

    create_opex_capex_graph(variable_cost_df, capex_dict, rmi_mapper, filepath)

    for year, region in list(itertools.product({2030, 2050}, {'China', 'India', 'Europe', 'NAFTA'})):
        create_opex_capex_graph_regional(variable_cost_df, capex_dict, rmi_mapper, year=year, region=region, filepath=filepath)

    create_investment_line_graph(investment_results, group="global", operation="cumsum", filepath=filepath)

    create_investment_per_tech_graph(investment_results, filepath=filepath)

    create_cot_graph(production_resource_usage, regions=['China', 'India', 'Europe', 'NAFTA'], filepath=filepath)

    create_lcost_graph(lcost_data, 2030, filepath=filepath)

    for year, region in list(itertools.product({2020, 2030, 2040, 2050}, {'China', 'India', 'Europe', 'NAFTA'})):
        create_tco_graph(tco_ref, year, region, 'Avg BF-BOF', filepath=filepath)

    for year, region, scope in list(itertools.product(
        {2020, 2030, 2050},
        {'China', 'India', 'Europe', 'NAFTA'},
        {'s1_emissivity', 's2_emissivity', 's3_emissivity', 'combined'}
    )):
        create_emissions_graph(calculated_emissivity_combined_df, year, region, scope, filepath=filepath)
