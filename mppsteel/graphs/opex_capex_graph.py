"""Graph fpr the OPEX CAPEX split"""
import itertools
from typing import Union
import pandas as pd
import plotly.express as px
import numpy as np
import numpy_financial as npf
from mppsteel.config.model_config import DISCOUNT_RATE
from mppsteel.config.reference_lists import SWITCH_DICT, TECH_REFERENCE_LIST

from mppsteel.utility.utils import cast_to_float
from mppsteel.utility.dataframe_utility import column_sorter
from mppsteel.utility.log_utility import get_logger
from mppsteel.graphs.plotly_graphs import bar_chart

logger = get_logger(__name__)

BAR_CHART_ORDER = {
    "GF Capex": "#A0522D",
    "BF Capex": "#7F6000",
    "Other Opex": "#1E3B63",
    "CCS": "#9DB1CF",
    "Electricity": "#FFC000",
    "Hydrogen": "#59A270",
    "Bio Fuels": "#BCDAC6",
    "Fossil Fuels": "#E76B67",
    "Feedstock": "#A5A5A5",
    "Region Cost Delta": "#F2F2F2",
}

def return_capex_values(
    capex_dict: dict, year: str, investment_cycle: int, discount_rate: float
) -> pd.DataFrame:
    """This function takes in a dictionary of capex values, a year, an investment cycle, and a discount
    rate. It returns a dataframe of the capex values for the year, discounted to the investment cycle.

    Args:
        capex_dict (dict): A dictionary containing the capex values for each technology.
        year (str): The year for which we want to calculate the capex values.
        investment_cycle (int): The length of the investment cycle.
        discount_rate (float): The discount rate used to calculated the present values.

    Returns:
        pd.DataFrame: A dataframe with the following columns: greenfield_capex, brownfield_capex, other_opex, renovation_capex.
    """

    brownfield_values = capex_dict["brownfield"].xs(key=year, level="Year").copy()
    greenfield_values = capex_dict["greenfield"].xs(key=year, level="Year").copy()
    other_opex_values = capex_dict["other_opex"].xs(key=year, level="Year").copy()
    for df in [greenfield_values, brownfield_values, other_opex_values]:
        df.drop(["Close plant", "Charcoal mini furnace"], axis=0, inplace=True)

    brownfield_values["value"] = -brownfield_values["value"].apply(lambda x: npf.pmt(discount_rate, investment_cycle, x))
    greenfield_values["value"] = -greenfield_values["value"].apply(lambda x: npf.pmt(discount_rate, investment_cycle, x))

    brownfield_values.rename(mapper={"value": "brownfield_capex"}, axis=1, inplace=True)
    greenfield_values.rename(mapper={"value": "greenfield_capex"}, axis=1, inplace=True)
    other_opex_values.rename(mapper={"value": "other_opex"}, axis=1, inplace=True)
    combined_values = greenfield_values.join(brownfield_values) / investment_cycle
    combined_values = combined_values.join(other_opex_values)
    combined_values["renovation_capex"] = (
        combined_values["greenfield_capex"] + combined_values["brownfield_capex"]
    )
    return combined_values


def add_opex_values(vdf: pd.DataFrame, co_df: pd.DataFrame) -> pd.DataFrame:
    """Adds opex values to the variable costs DataFrame as an additional column.

    Args:
        vdf (pd.DataFrame): The variable costs DataFrame you want to modify.
        co_df (pd.DataFrame): The Capex DataFrame.

    Returns:
        pd.DataFrame: The updated variable costs dataframe with the other opex values.
    """
    vdf_c = vdf.copy()
    for technology in SWITCH_DICT.keys():
        vdf_c.loc[technology, "Other Opex"]["cost"] = (
            vdf_c.loc[technology, "Other Opex"]["cost"]
            + co_df["other_opex"][technology]
        )
    return vdf_c


def add_capex_values(vdf: pd.DataFrame, co_df: pd.DataFrame) -> pd.DataFrame:
    """Adds capex values to the variable costs DataFrame as additional columns.

    Args:
        vdf (pd.DataFrame): The variable costs DataFrame you want to use.
        co_df (pd.DataFrame): The Capex DataFrame.

    Returns:
        pd.DataFrame: The updated variable costs dataframe with the capex valuess.
    """
    vdf_c = vdf.copy()
    country_values = vdf_c.index.get_level_values(2).unique()
    for technology in SWITCH_DICT.keys():
        bf_value = co_df["brownfield_capex"][technology]
        gf_value = co_df["greenfield_capex"][technology]
        for country in country_values:
            vdf_c.loc[(technology, "BF Capex", country), "cost"] = bf_value
            vdf_c.loc[(technology, "GF Capex", country), "cost"] = gf_value
    return vdf_c


def get_country_deltas(df: pd.DataFrame) -> Union[pd.DataFrame, dict]:
    """Gets the lowest regional Levelised Cost of Steelmaking values.

    Args:
        df (pd.DataFrame): The DataFrame containing the costs.

    Returns:
        Union[pd.DataFrame, dict]: Returns the subsetted DataFrame with the lowest costs and 
        also a dictionary with the delta values between the lowest and highest cost regions.
    """
    df["cost"] = df["cost"].apply(lambda x: cast_to_float(x))
    df_s = df.reset_index()
    df_c = df.groupby(["technology", "country_code"]).sum()
    df_c["cost"] = pd.to_numeric(df_c["cost"])
    technologies = df_c.index.get_level_values(0).unique()
    tech_delta_dict = {}
    tech_list = []
    for technology in technologies:
        min_val = df_c.loc[technology]["cost"].min()
        max_val = df_c.loc[technology]["cost"].max()
        tech_delta_dict[technology] = max_val - min_val
        min_country_code = df_c.loc[technology].idxmin().values[0]
        df_subset = df_s[
            (df_s["technology"] == technology)
            & (df_s["country_code"] == min_country_code)
        ].copy()
        tech_list.append(df_subset)
    df_combined = (
        pd.concat(tech_list)
        .reset_index(drop=True)
        .set_index(["technology", "cost_type", "country_code"])
    )
    return df_combined, tech_delta_dict


def assign_country_deltas(df: pd.DataFrame, delta_dict: dict) -> pd.DataFrame:
    """Assigns the delta values to each respective dictionary.

    Args:
        df (pd.DataFrame): A DataFrame containing the lowest region cost values.
        delta_dict (dict): A dictionary containing the delta values between low and high.

    Returns:
        pd.DataFrame: A DataFrame with a new column `LCOS delta` containing the delat for each technology between
        the lowest and highest cost values.
    """
    df_c = df.copy()
    country_values = df_c.index.get_level_values(2).unique()
    for country, technology in list(itertools.product(country_values, SWITCH_DICT.keys())):
        df_c.loc[(technology, "Region Cost Delta", country), "cost"] = delta_dict[technology]
    return df_c


def create_capex_opex_split_data(variable_cost_df: pd.DataFrame, capex_dict: dict, country_mapper: dict) -> pd.DataFrame:
    """Creates a DataFrame split by cost type for the purpose of creating a graph.

    Returns:
        pd.DataFrame: A DataFrame containing the split of costs and the associated metadata.
    """
    vcsmb_c = variable_cost_df.copy()
    index_sort = ["technology", "cost_type", "country_code"]
    vcsmb_c = (
        vcsmb_c.set_index("year")
        .loc[2050]
        .drop(["material_category", "unit", "value"], axis=1)
        .set_index(index_sort)
        .sort_values(index_sort)
    ).copy()
    capex_opex_df = return_capex_values(
        capex_dict=capex_dict, year=2050, investment_cycle=20, discount_rate=DISCOUNT_RATE
    )
    vcsmb_c = add_opex_values(vcsmb_c, capex_opex_df)
    vcsmb_c = add_capex_values(vcsmb_c, capex_opex_df).sort_values(index_sort)
    vcsmb_c, country_deltas = get_country_deltas(vcsmb_c)
    vcsmb_c = assign_country_deltas(vcsmb_c, country_deltas)
    vcsmb_c.reset_index(inplace=True)
    vcsmb_c["region"] = vcsmb_c["country_code"].apply(
        lambda x: country_mapper[x])
    vcsmb_cocd = vcsmb_c.reset_index(drop=True).drop(["country_code", "region"], axis=1)
    return (
        vcsmb_cocd.groupby(["technology", "cost_type"])
        .sum()
        .groupby(["technology", "cost_type"])
        .mean()
        .reset_index()
    ).copy()


def opex_capex_graph(variable_cost_df: pd.DataFrame, capex_dict: dict, country_mapper: dict, save_filepath: str = None, ext: str = "png") -> px.bar:
    """Creates a bar graph for the Opex Capex split graph.

    Args:
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".

    Returns:
        px.bar: A plotly express bar chart.
    """
    final_opex_capex_dataset = create_capex_opex_split_data(variable_cost_df, capex_dict, country_mapper)
    final_opex_capex_dataset_c = column_sorter(
        final_opex_capex_dataset, "cost_type", BAR_CHART_ORDER.keys()
    )

    fig_ = bar_chart(
        data=final_opex_capex_dataset_c,
        x="technology",
        y="cost",
        color="cost_type",
        color_discrete_map=BAR_CHART_ORDER,
        array_order=TECH_REFERENCE_LIST,
        xaxis_title="Technology",
        yaxis_title="Cost",
        title_text="Capex / OPEX breakdown in 2050",
    )

    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")

    return fig_

def add_capex_values_regional(vdf: pd.DataFrame, co_df: pd.DataFrame) -> pd.DataFrame:
    """Adds capex values to the variable costs DataFrame as additional columns.
    Args:
        vdf (pd.DataFrame): The variable costs DataFrame you want to use.
        co_df (pd.DataFrame): The Capex DataFrame.
    Returns:
        pd.DataFrame: The updated variable costs dataframe with the capex valuess.
    """
    vdf_c = vdf.copy()
    country_values = vdf_c.index.get_level_values(2).unique()
    for technology in SWITCH_DICT.keys():
        bf_value = co_df["brownfield_capex"][technology]
        #gf_value = co_df["greenfield_capex"][technology]
        for country in country_values:
            vdf_c.loc[(technology, "BF Capex", country), "cost"] = bf_value
            #vdf_c.loc[(technology, "GF Capex", country), "cost"] = gf_value
    return vdf_c

def add_opex_values_regional(vdf: pd.DataFrame, co_df: pd.DataFrame) -> pd.DataFrame:
    """Adds opex values to the variable costs DataFrame as an additional column.
    Args:
        vdf (pd.DataFrame): The variable costs DataFrame you want to modify.
        co_df (pd.DataFrame): The Capex DataFrame.
    Returns:
        pd.DataFrame: The updated variable costs dataframe with the other opex values.
    """
    vdf_c = vdf.copy()
    country_values = vdf_c.index.get_level_values(2).unique()
    for technology in SWITCH_DICT.keys():
        other_opex=co_df["other_opex"][technology]
        for country in country_values:
            vdf_c.loc[(technology, "Other_Opex", country), "cost"] = other_opex
    return vdf_c

def return_capex_values_regional(
    capex_dict: dict, year: str, investment_cycle: int, discount_rate: float
) -> pd.DataFrame:
    """This function takes in a dictionary of capex values, a year, an investment cycle, and a discount
    rate. It returns a dataframe of the capex values for the year, discounted to the investment cycle.
    Args:
        capex_dict (dict): A dictionary containing the capex values for each technology.
        year (str): The year for which we want to calculate the capex values.
        investment_cycle (int): The length of the investment cycle.
        discount_rate (float): The discount rate used to calculated the present values.
    Returns:
        pd.DataFrame: A dataframe with the following columns: greenfield_capex, brownfield_capex, other_opex, renovation_capex.
    """
    brownfield_values = capex_dict["brownfield"].xs(key=year, level="Year").copy()
    greenfield_values = capex_dict["greenfield"].xs(key=year, level="Year").copy()
    other_opex_values = capex_dict["other_opex"].xs(key=year, level="Year").copy()
    for df in [greenfield_values, brownfield_values, other_opex_values]:
        df.drop(["Close plant", "Charcoal mini furnace"], axis=0, inplace=True)
    brownfield_values["value"]= -brownfield_values["value"].apply(lambda x: npf.pmt(discount_rate, investment_cycle, x))
    greenfield_values["value"]= -greenfield_values["value"].apply(lambda x: npf.pmt(discount_rate, investment_cycle, x))
    brownfield_values.rename(mapper={"value": "brownfield_capex"}, axis=1, inplace=True)
    greenfield_values.rename(mapper={"value": "greenfield_capex"}, axis=1, inplace=True)
    other_opex_values.rename(mapper={"value": "other_opex"}, axis=1, inplace=True)
    combined_values = greenfield_values.join(brownfield_values) 
    combined_values = combined_values.join(other_opex_values)
    return combined_values

def regional_split_of_preprocessed_data(vcsmb: pd.DataFrame, capex_dict: dict, country_mapper: dict, year: int = 2050, region: str = None):
    """Creates a DataFrame split by cost type for the purpose of creating a graph.
    Returns:
        pd.DataFrame: A DataFrame containing the split of costs and the associated metadata.
    """
    vcsmb_c = vcsmb.copy()
    index_sort = ["technology", "cost_type", "country_code"]
    vcsmb_c = (
        vcsmb_c.set_index("year")
        .loc[year]
        .drop(["unit", "value"], axis=1)
        .set_index(index_sort)
        .sort_values(index_sort)
    )
    capex_opex_df = return_capex_values_regional(
        capex_dict=capex_dict, year=year, investment_cycle=20, discount_rate=0.07
    )
    vcsmb_c = add_opex_values_regional(vcsmb_c, capex_opex_df)
    vcsmb_c = add_capex_values_regional(vcsmb_c, capex_opex_df).sort_values(index_sort)
    vcsmb_c.reset_index(inplace=True)
    vcsmb_c['cost_type'].replace('Other_Opex','Other Opex', inplace=True)
    vcsmb_c["region"] = vcsmb_c["country_code"].apply(lambda x: country_mapper[x])
    vcsmb_c.reset_index(drop=True)
    if region:
        vcsmb_c = vcsmb_c.loc[(vcsmb_c['region'] == region)]
        vcsmb_c = vcsmb_c.drop_duplicates(subset=['technology','cost_type','material_category'])
        vcsmb_c = vcsmb_c.drop(["material_category",'country_code', "region"], axis=1)
    vcsmb_c = vcsmb_c.reset_index(drop=True)
    vcsmb_c = vcsmb_c.groupby(["technology", "cost_type"]).sum().reset_index()
    return vcsmb_c


def opex_capex_graph_regional(
    vcsmb: pd.DataFrame, capex_dict: dict, country_mapper: dict, save_filepath: str = None, ext: str = "png", year: int = 2050, region: str = None) -> px.bar:
    """Creates a bar graph for the Opex Capex split graph.
    Args:
        save_filepath (str, optional): The filepath that you save the graph to. Defaults to None.
        ext (str, optional): The extension of the image you are creating. Defaults to "png".
    Returns:
        px.bar: A plotly express bar chart.
    """
    final_opex_capex_dataset = regional_split_of_preprocessed_data(vcsmb, capex_dict, country_mapper, year=year, region=region)
    final_opex_capex_dataset = column_sorter(
        final_opex_capex_dataset, "cost_type", BAR_CHART_ORDER.keys()
    )
    fig_ = bar_chart(
        data=final_opex_capex_dataset,
        x="technology",
        y="cost",
        color="cost_type",
        color_discrete_map=BAR_CHART_ORDER,
        array_order=TECH_REFERENCE_LIST,
        xaxis_title="Technology",
        yaxis_title="Cost",
        title_text= f"{region} - Capex / OPEX breakdown in {year}",
    )
    if save_filepath:
        fig_.write_image(f"{save_filepath}.{ext}")
    return fig_
