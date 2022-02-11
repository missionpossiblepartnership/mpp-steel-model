"""Graph fpr the OPEX CAPEX split"""
from typing import Union
import pandas as pd
import plotly.express as px
import numpy as np
import numpy_financial as npf
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST

from mppsteel.utility.utils import cast_to_float
from mppsteel.config.model_config import PKL_DATA_INTERMEDIATE
from mppsteel.utility.dataframe_utility import column_sorter
from mppsteel.utility.location_utility import get_region_from_country_code
from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.utility.log_utility import get_logger
from mppsteel.graphs.plotly_graphs import bar_chart

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
    brownfield_values = capex_dict["brownfield"].xs(key=year, level="Year").copy()
    greenfield_values = capex_dict["greenfield"].xs(key=year, level="Year").copy()
    other_opex_values = capex_dict["other_opex"].xs(key=year, level="Year").copy()
    for df in [greenfield_values, brownfield_values, other_opex_values]:
        df.drop(["Close plant", "Charcoal mini furnace"], axis=0, inplace=True)
    brownfield_values["value"].apply(
        lambda x: npf.npv(discount_rate, np.full((investment_cycle), x))
    )
    greenfield_values["value"].apply(
        lambda x: npf.npv(discount_rate, np.full((investment_cycle), x))
    )
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
    vdf_c = vdf.copy()
    tech_values = vdf_c.index.get_level_values(0).unique()
    for technology in tech_values:
        vdf_c.loc[technology, "Other Opex"]["cost"] = (
            vdf_c.loc[technology, "Other Opex"]["cost"]
            + co_df["other_opex"][technology]
        )
    return vdf_c


def add_capex_values(vdf: pd.DataFrame, co_df: pd.DataFrame) -> pd.DataFrame:
    vdf_c = vdf.copy()
    tech_values = vdf_c.index.get_level_values(0).unique()
    country_values = vdf_c.index.get_level_values(2).unique()
    for technology in tech_values:
        bf_value = co_df["brownfield_capex"][technology]
        gf_value = co_df["greenfield_capex"][technology]
        for country in country_values:
            vdf_c.loc[(technology, "BF Capex", country), "cost"] = bf_value
            vdf_c.loc[(technology, "GF Capex", country), "cost"] = gf_value
    return vdf_c


def get_country_deltas(df: pd.DataFrame) -> Union[pd.DataFrame, dict]:
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
    df_c = df.copy()
    tech_values = df_c.index.get_level_values(0).unique()
    country_values = df_c.index.get_level_values(2).unique()
    for technology in tech_values:
        for country in country_values:
            df_c.loc[(technology, "Region Cost Delta", country), "cost"] = delta_dict[
                technology
            ]
    return df_c


def create_capex_opex_split_data() -> pd.DataFrame:
    capex_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_dict", "df")
    vcsmb = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "variable_costs_regional_material_breakdown", "df"
    )
    vcsmb_c = vcsmb.copy()
    index_sort = ["technology", "cost_type", "country_code"]
    # Eletricity PJ to Twh
    def value_mapper(row):
        if row["material_category"] in ["Electricity", "Hydrogen"]:
            row["cost"] = row["cost"] / 3.6
        return row

    vcsmb_c = vcsmb_c.apply(value_mapper, axis=1)
    vcsmb_c = (
        vcsmb_c.set_index("year")
        .loc[2050]
        .drop(["material_category", "unit", "value"], axis=1)
        .set_index(index_sort)
        .sort_values(index_sort)
    )
    capex_opex_df = return_capex_values(
        capex_dict=capex_dict, year=2050, investment_cycle=20, discount_rate=0.07
    )
    vcsmb_c = add_opex_values(vcsmb_c, capex_opex_df)
    vcsmb_c = add_capex_values(vcsmb_c, capex_opex_df).sort_values(index_sort)
    vcsmb_c, country_deltas = get_country_deltas(vcsmb_c)
    vcsmb_c = assign_country_deltas(vcsmb_c, country_deltas)
    vcsmb_c.reset_index(inplace=True)
    country_ref_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    vcsmb_c["region"] = vcsmb_c["country_code"].apply(
        lambda x: get_region_from_country_code(x, "rmi_region", country_ref_dict)
    )
    vcsmb_cocd = vcsmb_c.reset_index(drop=True).drop(["country_code", "region"], axis=1)
    return (
        vcsmb_cocd.groupby(["technology", "cost_type"])
        .sum()
        .groupby(["technology", "cost_type"])
        .mean()
        .reset_index()
    )


def opex_capex_graph(save_filepath: str = None, ext: str = "png") -> px.bar:
    final_opex_capex_dataset = create_capex_opex_split_data()
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
