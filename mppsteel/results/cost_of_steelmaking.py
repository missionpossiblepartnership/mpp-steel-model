"""Calculation Functions used to derive various forms of Cost of Steelmaking."""

import pandas as pd
import numpy as np
import numpy_financial as npf

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.model.solver import create_plant_capacities_dict
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_FINAL,
    PKL_DATA_INTERMEDIATE,
    DISCOUNT_RATE,
    INVESTMENT_CYCLE_DURATION_YEARS,
    STEEL_PLANT_LIFETIME_YEARS,
)
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST

logger = get_logger("Cost of Steelmaking")


def create_region_plant_ref(df: pd.DataFrame, region_string: str) -> dict:
    region_dict = {}
    for region in df[region_string].unique():
        region_dict[region] = list(
            df[df[region_string] == region]["plant_name"].unique()
        )
    return region_dict


def extract_dict_values(
    main_dict: dict, key_to_extract: str, reference_dict: dict = None, ref: str = None
) -> float:
    if reference_dict and ref:
        ref_list = reference_dict[ref]
        return sum(
            [
                main_dict[key][key_to_extract]
                for key in main_dict
                if key in ref_list
            ]
        )
    return sum([main_dict[key][key_to_extract] for key in main_dict])


def calculate_cc(
    capex_df: pd.DataFrame,
    year: int,
    year_span: range,
    technology: str,
    discount_rate: float,
    cost_type: str,
) -> float:
    year_range = range(year, year + year_span)
    value_arr = np.array([])
    for eval_year in year_range:
        year_loop_val = min(MODEL_YEAR_END, eval_year)
        value = capex_df[cost_type].loc[technology, year_loop_val]["value"]
        value_arr = np.append(value_arr, value)
    return npf.npv(discount_rate, value_arr)


def apply_cos(
    row,
    year: int,
    cap_dict: dict,
    v_costs: pd.DataFrame,
    capex_costs: dict,
    steel_demand: pd.DataFrame,
    steel_scenario: pd.DataFrame,
    capital_charges: bool,
) -> float:

    primary_capacity = cap_dict[row.plant_name]["primary_capacity"]
    secondary_capacity = cap_dict[row.plant_name]["secondary_capacity"]
    variable_cost = v_costs.loc[row.country_code, year, row.technology]["cost"]
    other_opex_cost = capex_costs["other_opex"].loc[row.technology, year]["value"]
    steel_demand_value = steel_demand_getter(
        steel_demand, year, steel_scenario, "crude", "World"
    )
    discount_rate = DISCOUNT_RATE
    relining_year_span = INVESTMENT_CYCLE_DURATION_YEARS

    # cuf = steel_demand_value / row.capacity
    relining_cost = 0

    if capital_charges:
        relining_cost = calculate_cc(
            capex_costs,
            year,
            relining_year_span,
            row.technology,
            discount_rate,
            "brownfield",
        )

    result_1 = (primary_capacity + secondary_capacity) * (
        (variable_cost * row.capacity_utilization) + other_opex_cost + relining_cost
    )

    if not capital_charges:
        return result_1

    gf_value = capex_costs["greenfield"].loc[row.technology, year]["value"]
    result_2 = npf.pmt(discount_rate, relining_year_span, gf_value) / steel_demand_value

    return result_1 - result_2


def apply_lcos(
    row, v_costs: pd.DataFrame, capex_costs: dict, include_greenfield: bool = True
):
    variable_cost = v_costs.loc[row.country_code, row.year, row.technology]["cost"]
    other_opex_cost = capex_costs["other_opex"].loc[row.technology, row.year]["value"]
    discount_rate = DISCOUNT_RATE
    relining_year_span = INVESTMENT_CYCLE_DURATION_YEARS
    life_of_plant = STEEL_PLANT_LIFETIME_YEARS
    greenfield_cost = 0

    renovation_cost = calculate_cc(
        capex_costs,
        row.year,
        relining_year_span,
        row.technology,
        discount_rate,
        "brownfield",
    )
    if include_greenfield:
        greenfield_cost = calculate_cc(
            capex_costs,
            row.year,
            life_of_plant,
            row.technology,
            discount_rate,
            "greenfield",
        )
    row["levelised_cost_of_steelmaking"] = (
        other_opex_cost + variable_cost + renovation_cost + greenfield_cost
    )
    return row


def cost_of_steelmaking(
    production_stats: pd.DataFrame,
    variable_costs: pd.DataFrame,
    capex_df: pd.DataFrame,
    steel_demand: pd.DataFrame,
    capacities_dict: dict,
    steel_scenario: str = "bau",
    region_group: str = "region_wsa_region",
    regional: bool = False,
    capital_charges: bool = False,
) -> dict:

    regions = production_stats[region_group].unique()
    years = production_stats["year"].unique()
    cols_to_keep = [
        "year",
        "plant_name",
        "country_code",
        "technology",
        "capacity",
        "production",
        "capacity_utilization",
        "region_wsa_region",
        "region_continent",
        "region_region",
    ]
    production_stats = production_stats[cols_to_keep].set_index("year").copy()
    plant_region_ref = create_region_plant_ref(production_stats, region_group)
    cos_year_list = []

    def calculate_cos(df, ref=None) -> float:
        df_c = df.copy()
        cos_values = df_c.apply(
            apply_cos,
            year=year,
            cap_dict=capacities_dict,
            v_costs=variable_costs,
            capex_costs=capex_df,
            steel_demand=steel_demand,
            steel_scenario=steel_scenario,
            capital_charges=capital_charges,
            axis=1,
        )
        cos_sum = cos_values.sum()
        primary_sum = extract_dict_values(
            capacities_dict, "primary_capacity", plant_region_ref, ref
        )
        secondary_sum = extract_dict_values(
            capacities_dict, "secondary_capacity", plant_region_ref, ref
        )
        return cos_sum / (primary_sum + secondary_sum)

    for year in tqdm(years, total=len(years), desc="Cost of Steelmaking: Year Loop"):
        ps_y = production_stats.loc[year]

        if regional:
            ps_y = ps_y.set_index(region_group)
            region_dict = {}
            for region in regions:
                ps_r = ps_y.loc[region]
                cos_r = calculate_cos(ps_r, region)
                region_dict[region] = cos_r
            cos_year_list.append(region_dict)

        else:
            cos_final = calculate_cos(ps_y)
            cos_year_list.append(cos_final)
    return dict(zip(years, cos_year_list))


def dict_to_df(df: pd.DataFrame, region_group: str, cc: bool = False) -> pd.DataFrame:
    value_col = "cost_of_steelmaking"
    if cc:
        value_col = f"{value_col}_with_cc"
    df_c = pd.DataFrame(df).transpose().copy()
    df_c = df_c.reset_index().rename(mapper={"index": "year"}, axis=1)
    df_c = df_c.melt(id_vars=["year"], var_name=region_group, value_name=value_col)
    return df_c.set_index(["year", region_group]).sort_values(
        by=["year", region_group], axis=0
    )


def create_cost_of_steelmaking_data(
    production_df: pd.DataFrame,
    variable_costs_df: pd.DataFrame,
    capex_ref: dict,
    steel_demand_df: pd.DataFrame,
    capacities_ref: dict,
    demand_scenario: str,
    region_group: str,
) -> pd.DataFrame:

    standard_cos = cost_of_steelmaking(
        production_df,
        variable_costs_df,
        capex_ref,
        steel_demand_df,
        capacities_ref,
        demand_scenario,
        region_group,
        regional=True,
    )
    cc_cos = cost_of_steelmaking(
        production_df,
        variable_costs_df,
        capex_ref,
        steel_demand_df,
        capacities_ref,
        demand_scenario,
        region_group,
        regional=True,
        capital_charges=True,
    )
    cc_cos_d = dict_to_df(cc_cos, region_group, True)
    standard_cos_d = dict_to_df(standard_cos, region_group, False)
    return standard_cos_d.join(cc_cos_d)


def create_df_reference(cols_to_create: list) -> pd.DataFrame:
    steel_plant_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
    )
    country_codes = steel_plant_df["country_code"].unique()
    init_cols = ["year", "country_code", "technology"]
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in year_range:
        for country_code in country_codes:
            for technology in TECH_REFERENCE_LIST:
                entry = dict(zip(init_cols, [year, country_code, technology]))
                df_list.append(entry)
    combined_df = pd.DataFrame(df_list)
    for column in cols_to_create:
        combined_df[column] = ""
    return combined_df


def create_levelised_cost_of_steelmaking(
    variable_costs: pd.DataFrame, capex_ref: dict, include_greenfield=True
) -> pd.DataFrame:
    lev_cost_of_steel = create_df_reference(["levelised_cost_of_steelmaking"])
    tqdma.pandas(desc="Applying Lev. Steel")
    lev_cost_of_steel = lev_cost_of_steel.progress_apply(
        apply_lcos,
        v_costs=variable_costs,
        capex_costs=capex_ref,
        include_greenfield=include_greenfield,
        axis=1,
    )
    return lev_cost_of_steel


@timer_func
def generate_cost_of_steelmaking_results(scenario_dict: dict, serialize: bool = False):
    capacities_dict = create_plant_capacities_dict()
    variable_costs_regional = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "variable_costs_regional", "df"
    )
    capex_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_dict", "df")
    steel_demand_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted", "df"
    )
    production_resource_usage = read_pickle_folder(
        PKL_DATA_FINAL, "production_resource_usage", "df"
    )
    steel_demand_scenario = scenario_dict["steel_demand_scenario"]

    cos_data = create_cost_of_steelmaking_data(
        production_resource_usage,
        variable_costs_regional,
        capex_dict,
        steel_demand_df,
        capacities_dict,
        steel_demand_scenario,
        "region_wsa_region",
    )

    lcos_data = create_levelised_cost_of_steelmaking(
        variable_costs_regional, capex_dict, include_greenfield=True
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(cos_data, PKL_DATA_FINAL, "cost_of_steelmaking")
        serialize_file(lcos_data, PKL_DATA_FINAL, "levelised_cost_of_steelmaking")
    return {"cos_data": cos_data, "lcos_data": lcos_data}
