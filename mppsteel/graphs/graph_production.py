"""Creates graphs from model outputs"""
import pandas as pd
import plotly.express as px

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import read_pickle_folder

from mppsteel.utility.log_utility import get_logger

from mppsteel.config.model_config import PKL_DATA_FINAL

from mppsteel.graphs.plotly_graphs import (
    line_chart,
    area_chart,
    bar_chart,
    bar_chart_vertical,
    ARCHETYPE_COLORS,
)

from mppsteel.graphs.opex_capex_graph import opex_capex_graph
from mppsteel.graphs.consumption_over_time import consumption_over_time_graph
from mppsteel.graphs.cost_of_steelmaking_graphs import lcost_graph
from mppsteel.graphs.investment_graph import investment_line_chart, investment_per_tech

# Create logger
logger = get_logger("Graph Production")

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

REGION_COLS = ["region_wsa_region", "region_continent", "region_region"]

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


def steel_production_area_chart(df: pd.DataFrame, filepath: str = None) -> px.area:
    """Creates an Area graph of Steel Production.

    Args:
        df (pd.DataFrame): A DataFrame of Production Stats.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.


    Returns:
        px.area: A plotly express area graph.
    """
    filename = "steel_production_per_technology"
    logger.info(f"Creating area graph output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return area_chart(
        data=generate_production_emissions(df, "technology", ["production"]),
        x="year",
        y="value",
        color="technology",
        name="Steel production per tech for run scenario",
        x_axis="year",
        y_axis="Steel Production",
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
        data=generate_subset(df, "region_wsa_region", resource, regions),
        x="year",
        y="value",
        color="region_wsa_region",
        name=f"{resource_string} consumption in {region_list}",
        x_axis="year",
        y_axis=resource_string,
        save_filepath=filename,
    )


def create_opex_capex_graph(filepath: str = None) -> px.bar:
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
    return opex_capex_graph(save_filepath=filename)


def create_investment_line_graph(
    group: str, operation: str, filepath: str = None
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
        group=group, operation=operation, save_filepath=filename
    )


def create_investment_per_tech_graph(filepath: str = None) -> px.bar:
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
    return investment_per_tech(save_filepath=filename)


def create_cot_graph(regions: list = None, filepath: str = None) -> px.bar:
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
    return consumption_over_time_graph(regions=regions, save_filepath=filename)


def create_lcost_graph(chosen_year: int, filepath: str = None) -> px.bar:
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
    return lcost_graph(chosen_year=chosen_year, save_filepath=filename)


@timer_func
def create_graphs(filepath: str) -> None:
    """Graph creation flow.

    Args:
        filepath (str): The folder path you want to save the chart to. Defaults to None.
    """
    production_resource_usage = read_pickle_folder(
        PKL_DATA_FINAL, "production_resource_usage", "df"
    )
    production_emissions = read_pickle_folder(
        PKL_DATA_FINAL, "production_emissions", "df"
    )
    tco_ref = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "tco_summary_data", "df"
    )

    steel_production_area_chart(production_emissions, filepath)
    resource_line_charts(
        production_resource_usage,
        "electricity",
        ["EU + UK", "China", "India", "USMCA"],
        filepath,
    )

    for scope in ["s1", "s2", "s3", "combined"]:
        emissions_area_chart(production_emissions, filepath, scope)

    for resource in RESOURCE_COLS:
        resource_line_charts(
            df=production_resource_usage, resource=resource, filepath=filepath
        )

    create_opex_capex_graph(filepath)

    create_investment_line_graph(group="global", operation="cumsum", filepath=filepath)

    create_investment_per_tech_graph(filepath=filepath)

    create_cot_graph(filepath=filepath)

    create_lcost_graph(2030, filepath=filepath)

    for year in [2020,2030,2040,2050]:
        for reg in ['China', 'NAFTA', 'India','Europe', None]:
            generate_tco_charts(tco_ref, year,reg,'Avg BF-BOF', filepath=filepath)
