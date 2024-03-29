"""Creates graphs from model outputs"""
import itertools
from typing import Union
import pandas as pd
import plotly.express as px
from mppsteel.config.model_config import (
    MID_MODEL_CHECKPOINT_YEAR_FOR_GRAPHS,
    MODEL_YEAR_RANGE,
    NET_ZERO_TARGET_YEAR,
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    get_scenario_pkl_path,
    return_pkl_paths,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.model_graphs.plotly_graphs import TECHNOLOGY_ARCHETYPE_COLORS, area_chart
from mppsteel.model_graphs.opex_capex_graph import (
    opex_capex_graph,
    opex_capex_graph_regional,
)
from mppsteel.model_graphs.consumption_over_time import (
    consumption_over_time_graph,
    resource_line_charts,
)
from mppsteel.model_graphs.cost_of_steelmaking_graphs import lcost_graph
from mppsteel.model_graphs.investment_graph import (
    investment_line_chart,
    investment_per_tech,
)
from mppsteel.model_graphs.emissions_per_tech import (
    generate_emissivity_charts,
    steel_emissions_line_chart,
)
from mppsteel.model_graphs.tco_graph import generate_tco_charts
from mppsteel.model_graphs.new_plant_capacity import (
    new_plant_capacity_graph,
    trade_balance_graph,
)
from mppsteel.model_graphs.combined_scenario_graphs import (
    create_combined_investment_chart,
    create_combined_emissions_chart,
    create_combined_resource_chart,
    create_total_energy_usage_chart,
)
from mppsteel.utility.utils import decades_between_dates, join_list_as_string

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

EMISSION_COLS = ["s1_emissions_mt", "s2_emissions_mt", "s3_emissions_mt"]

RESOURCE_COLS = [
    "bf_gas_pj",
    "bf_slag_mt",
    "bof_gas_pj",
    "biomass_pj",
    "biomethane_pj",
    "cog_pj",
    "coke_pj",
    "dri_mt",
    "electricity_pj",
    "hydrogen_pj",
    "iron_ore_mt",
    "met_coal_pj",
    "met_coal_mt",
    "natural_gas_pj",
    "other_slag_mt",
    "plastic_waste_pj",
    "process_emissions_mt",
    "scrap_mt",
    "steam_pj",
    "thermal_coal_pj",
    "captured_co2_mt",
    "coal_pj",
    "used_co2_mt",
    "bioenergy_pj",
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
    """Preprocesses a production resource usage DataFrame for the purpose of creating a graph.

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


def steel_production_area_chart(
    df: pd.DataFrame,
    filepath: str = None,
    region: str = None,
    scenario_name: str = None,
) -> px.area:
    """Handler function for the Area graph of Steel Production.

    Args:
        df (pd.DataFrame): A DataFrame of Production Stats.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.
        region (str, optional): The region you want to model. Defaults to None.
        scenario_name (str): The name of the scenario at runtime.

    Returns:
        px.area: A plotly express area graph.
    """
    filename = "steel_production_per_technology"
    graph_title = "Steel production per tech"
    df_c = df.copy()
    region_list = df_c["region"].unique()
    if region and (region in region_list):
        graph_title = f"{graph_title} - {region}"
        df_c = df_c[df_c["region"] == region]
        filename = f"{filename}_for_{region}"
    elif region and (region not in region_list):
        raise ValueError(f"Incorrect region listed {region}")
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
        color_discrete_map=TECHNOLOGY_ARCHETYPE_COLORS,
        name=graph_title,
        x_axis="year",
        y_axis="Steel Production (gt)",
        hoverdata=None,
        save_filepath=filename,
    )


def emissions_area_chart(
    df: pd.DataFrame, filepath: str = None, scope: str = "combined"
) -> px.area:
    """Handler function for the Emissions Area chart.

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
    filename = f"{scope}_emissions_per_technology"
    logger.info(f"Creating area graph output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"

    data = (
        generate_production_emissions(df, "technology", emission_cols)
        .groupby(["year", "technology"])
        .agg("sum")
        .reset_index()
    )

    return area_chart(
        data=data,
        x="year",
        y="value",
        color="technology",
        color_discrete_map=TECHNOLOGY_ARCHETYPE_COLORS,
        name=f"{join_list_as_string(emission_cols)} per tech for run scenario",
        x_axis="year",
        y_axis="CO2 Emissions [CO2/year]",
        hoverdata=None,
        save_filepath=filename,
    )


def create_opex_capex_graph(
    variable_cost_df: pd.DataFrame,
    carbon_tax_timeseries: pd.DataFrame,
    emissivity_df: pd.DataFrame,
    capex_dict: dict,
    country_mapper: dict,
    year: int,
    filepath: str = None,
) -> px.bar:
    """Handler function for the Opex Capex split graph.

    Args:
        variable_cost_df (pd.DataFrame): The variable costs DataFrame.
        carbon_tax_timeseries (pd.DataFrame): The carbon tax timeseries DataFrame.
        emissivity_df (pd.DataFrame): The emissivity DataFrame.
        capex_dict (dict): The capex dictionary reference.
        country_mapper (dict): Mapper for coutry_codes to regions.
        year (int): The year to subset the DataFrame.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = f"global_opex_capex_graph_{year}"
    logger.info(f"Creating Opex Capex Graph Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return opex_capex_graph(
        variable_cost_df,
        carbon_tax_timeseries,
        emissivity_df,
        capex_dict,
        country_mapper,
        year,
        save_filepath=filename,
    )


def create_opex_capex_graph_regional(
    vcsmb: pd.DataFrame,
    carbon_tax_timeseries: pd.DataFrame,
    emissivity_df: pd.DataFrame,
    capex_dict: dict,
    country_mapper: dict,
    filepath: str = None,
    year: int = NET_ZERO_TARGET_YEAR,
    region: str = "NAFTA",
) -> px.bar:
    """Handler function for the Opex Capex split graph.

    Args:
        vcsmb (pd.DataFrame): The variable costs DataFrame.
        carbon_tax_timeseries (pd.DataFrame): The carbon tax timeseries DataFrame.
        emissivity_df (pd.DataFrame): The emissivity DataFrame.
        capex_dict (dict): The capex dictionary reference.
        country_mapper (dict): Mapper for coutry_codes to regions.
        year (int): The year to subset the DataFrame. Defaults to NET_ZERO_TARGET_YEAR.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None. Defaults to 'NAFTA'.

    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = f"{region}_opex_capex_graph_{year}"
    logger.info(f"Creating Opex Capex Graph Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return opex_capex_graph_regional(
        vcsmb,
        carbon_tax_timeseries,
        emissivity_df,
        capex_dict,
        country_mapper,
        save_filepath=filename,
        year=year,
        region=region,
    )


def create_investment_line_graph(
    investment_results: pd.DataFrame, group: str, operation: str, filepath: str = None
) -> px.line:
    """Handler function for the investment graph which shows the level of investment across all technologies.

    Args:
        investment_results (pd.DataFrame): The investment DataFrame.
        group (str, optional): The group you want: 'global' OR 'regional'. Defaults to "global".
        operation (str, optional): The operation you want to perform on the DataFrame 'sum' or 'cumsum'. Defaults to "cumsum".
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph.
    """
    global_results = True if group == "global" else False
    filename = f"investment_graph_{group}_{operation}"
    logger.info(f"Regional Investment Graph Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return investment_line_chart(
        investment_results,
        global_results=global_results,
        operation=operation,
        save_filepath=filename,
    )


def create_investment_per_tech_graph(
    investment_results: pd.DataFrame, filepath: str = None
) -> px.bar:
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


def create_new_plant_capacity_graph(
    plant_df: pd.DataFrame, graph_type: str, filepath: str = None
) -> px.line:
    """Handler function for the New Plant Capacity graph.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        graph_type (str): Specify the type of graph to return, either 'area' or 'bar'.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph.
    """
    filename = f"new_capacity_graph_{graph_type}"
    logger.info(f"New Capacity Graph Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    new_plant_capacity_graph(plant_df, graph_type, save_filepath=filename)


def create_trade_balance_graph(trade_df: pd.DataFrame, filepath: str = None) -> px.line:
    """Handler function for the Trade Balance graph.

    Args:
        trade_df (pd.DataFrame): The Trade results DataFrame.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.line: A plotly express line graph.
    """
    filename = f"trade_balance_graph"
    logger.info(f"Trade Balance Graph Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return trade_balance_graph(trade_df, save_filepath=filename)


def create_cot_graph(
    production_resource_usage: pd.DataFrame,
    resource_type: str,
    region: str = None,
    filepath: str = None,
) -> px.bar:
    """Generates a Graph showing the consumption over time of a material resource.

    Args:
        production_resource_usage (pd.DataFrame): The production Resource Usage DataFrame.
        resource (str): The name of the resource you want to graph.
        region (list, optional): The region to subset the DataFrame for. Defaults to None.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = f"{resource_type}_consumption_over_time"
    if region:
        filename = f"{filename}_in_{region}"
    logger.info(f"Consumption Over Time Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return consumption_over_time_graph(
        production_resource_usage,
        resource_type=resource_type,
        region=region,
        save_filepath=filename,
    )


def create_lcost_graph(
    lcost_df: pd.DataFrame, chosen_year: int, filename: str, filepath: str = None
) -> px.bar:
    """Handler function for the Levelized Cost graph.

    Args:
        lcost_df (pd.DataFrame): The levelized cost DataFrame.
        chosen_year (int): The year you want to set the Levelized cost values in the graph.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    logger.info(f"Levelized Cost Output: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return lcost_graph(lcost_df, chosen_year=chosen_year, save_filepath=filename)


def create_tco_graph(
    df: pd.DataFrame,
    year: int = None,
    region: str = None,
    tech: str = None,
    filepath: str = None,
) -> px.bar:
    """Handler function for the TCO graph.

    Args:
        df (pd.DataFrame): The TCO DataFrame.
        year (int, optional): The year to model. If not specified, all years will be graphed. Defaults to None.
        region (str, optional): The region to graph. If not specified, all regions will be graphed. Defaults to None.
        tech (str, optional): The technology to graph. If not specified, all technologies will be graphed. Defaults to None.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = f"TCO_{year}_from_{tech}"
    logger.info(f"TCO Output: {filename}")
    if region:
        filename = f"TCO_{year}_in_{region}_from_{tech}_"
    if filepath:
        filename = f"{filepath}/{filename}"

    return generate_tco_charts(df, year, region, tech, save_filepath=filename)


def create_emissions_graph(
    df: pd.DataFrame,
    year: int = None,
    region: str = None,
    scope: str = None,
    filepath: str = None,
) -> px.bar:
    """Creates an emissions graph.

    Args:
        df (pd.DataFrame): _description_
        year (int, optional): The year to model. If not specified, all years will be graphed. Defaults to None.
        region (str, optional): The region to graph. If not specified, all regions will be graphed. Defaults to None.
        scope (str, optional): The emissions scope to graph. If not specified, all scopes will be graphed. Defaults to None.
        filepath (str, optional): The folder path you want to save the chart to. Defaults to None.

    Returns:
        px.bar: A plotly express bar graph.
    """
    filename = f"emissivity_chart {scope}"
    if region:
        filename = f"emissivity_chart_{scope}_in_{region}"
    logger.info(f"Emissivity_chart: {filename}")
    if filepath:
        filename = f"{filepath}/{filename}"
    return generate_emissivity_charts(df, year, region, scope, save_filepath=filename)


@timer_func
def create_graphs(
    filepath: str, scenario_dict: dict, pkl_paths: Union[dict, None] = None
) -> None:
    """The complete creation flow for all graphs.

    Args:
        filepath (str): The folder path you want to save the chart to. Defaults to None.
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        pkl_paths (Union[dict, None], optional): A dictionary containing custom pickle paths. Defaults to {}.

    """
    _, intermediate_path, final_path = return_pkl_paths(
        scenario_name=scenario_dict["scenario_name"], paths=pkl_paths
    )

    production_resource_usage = read_pickle_folder(
        final_path, "production_resource_usage", "df"
    )
    production_emissions = read_pickle_folder(final_path, "production_emissions", "df")
    tco_ref = read_pickle_folder(intermediate_path, "tco_summary_data", "df")
    calculated_emissivity_combined_df = read_pickle_folder(
        intermediate_path, "calculated_emissivity_combined", "df"
    )
    levelized_cost_standardized = read_pickle_folder(
        intermediate_path, "levelized_cost_standardized", "df"
    )
    investment_results = read_pickle_folder(final_path, "investment_results", "df")
    capex_dict: dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    variable_cost_df: pd.DataFrame = read_pickle_folder(
        intermediate_path, "variable_costs_regional_material_breakdown", "df"
    )
    variable_cost_df.sort_index(ascending=True, inplace=True)
    plant_result_df = read_pickle_folder(intermediate_path, "plant_result_df", "df")
    full_trade_summary = read_pickle_folder(
        intermediate_path, "full_trade_summary", "df"
    )
    model_decades = decades_between_dates(MODEL_YEAR_RANGE)

    carbon_tax_timeseries = read_pickle_folder(
        intermediate_path, "carbon_tax_timeseries"
    )
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref", "df")
    rmi_mapper = create_country_mapper(country_ref)
    steel_production_area_chart(
        production_emissions,
        filepath=filepath,
        scenario_name=scenario_dict["scenario_name"],
    )
    for region in [
        "India",
        "China",
        "NAFTA",
        "Europe",
        "Japan, South Korea, and Taiwan",
    ]:
        steel_production_area_chart(
            df=production_emissions,
            filepath=filepath,
            region=region,
            scenario_name=scenario_dict["scenario_name"],
        )
        steel_emissions_line_chart(
            df=production_emissions,
            filepath=filepath,
            region=region,
            scenario_name=scenario_dict["scenario_name"],
        )

    for scope in ["s1", "s2", "s3", "combined"]:
        emissions_area_chart(production_emissions, filepath, scope)

    for resource in RESOURCE_COLS:
        resource_line_charts(
            df=production_resource_usage, resource=resource, filepath=filepath
        )
    for resource, region in list(
        itertools.product(RESOURCE_COLS, ["Europe", "China", "India", "NAFTA"])
    ):
        resource_line_charts(
            df=production_resource_usage,
            resource=resource,
            region=region,
            filepath=filepath,
        )
    create_opex_capex_graph(
        variable_cost_df,
        carbon_tax_timeseries,
        calculated_emissivity_combined_df,
        capex_dict,
        rmi_mapper,
        year=NET_ZERO_TARGET_YEAR,
        filepath=filepath,
    )

    for year, region in list(
        itertools.product(
            {MID_MODEL_CHECKPOINT_YEAR_FOR_GRAPHS, NET_ZERO_TARGET_YEAR},
            {"China", "India", "Europe", "NAFTA"},
        )
    ):
        create_opex_capex_graph_regional(
            variable_cost_df,
            carbon_tax_timeseries,
            calculated_emissivity_combined_df,
            capex_dict,
            rmi_mapper,
            year=year,
            region=region,
            filepath=filepath,
        )

    create_investment_line_graph(
        investment_results, group="global", operation="cumsum", filepath=filepath
    )

    create_investment_per_tech_graph(investment_results, filepath=filepath)

    create_new_plant_capacity_graph(plant_result_df, "area", filepath=filepath)
    create_new_plant_capacity_graph(plant_result_df, "bar", filepath=filepath)

    create_trade_balance_graph(full_trade_summary, filepath=filepath)

    for resource_type, region in list(
        itertools.product({"energy", "material"}, {"China", "India", "Europe", "NAFTA"})
    ):
        create_cot_graph(
            production_resource_usage,
            resource_type=resource_type,
            region=region,
            filepath=filepath,
        )

    create_lcost_graph(
        lcost_df=levelized_cost_standardized,
        chosen_year=MID_MODEL_CHECKPOINT_YEAR_FOR_GRAPHS,
        filename="levelized_cost_standardized",
        filepath=filepath,
    )

    for year, region in list(
        itertools.product(model_decades, {"China", "India", "Europe", "NAFTA"})
    ):
        create_tco_graph(tco_ref, year, region, "Avg BF-BOF", filepath=filepath)

    for year, region, scope in list(
        itertools.product(
            model_decades,
            {"China", "India", "Europe", "NAFTA"},
            {"s1_emissivity", "s2_emissivity", "s3_emissivity", "s1+s2", "combined"},
        )
    ):
        create_emissions_graph(
            calculated_emissivity_combined_df, year, region, scope, filepath=filepath
        )


@timer_func
def create_combined_scenario_graphs(filepath: str):
    combined_path = get_scenario_pkl_path(pkl_folder_type="combined")
    production_resource_usage = read_pickle_folder(
        combined_path, "production_resource_usage", "df"
    )
    production_emissions = read_pickle_folder(
        combined_path, "production_emissions", "df"
    )
    investment_results = read_pickle_folder(combined_path, "investment_results", "df")

    create_combined_investment_chart(investment_results, filepath=filepath)

    create_combined_emissions_chart(
        production_emissions, cumulative=False, filepath=filepath
    )
    create_combined_emissions_chart(
        production_emissions, cumulative=True, filepath=filepath
    )

    create_combined_resource_chart(
        production_resource_usage, "hydrogen_pj", filepath=filepath
    )
    create_combined_resource_chart(
        production_resource_usage, "electricity_pj", filepath=filepath
    )
    create_total_energy_usage_chart(production_resource_usage, filepath=filepath)
