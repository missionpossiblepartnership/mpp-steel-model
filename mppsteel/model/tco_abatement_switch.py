"""Script to run a full reference dataframe for tco switches and abatement switches"""
import itertools
from functools import lru_cache

import pandas as pd
import numpy as np
import numpy_financial as npf

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.data_loading.data_interface import load_business_cases
from mppsteel.model.tco_calculation_functions import tco_calc, calculate_green_premium
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.config.reference_lists import LOW_CARBON_TECHS, SWITCH_DICT
from mppsteel.config.model_config import (
    DISCOUNT_RATE,
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_INTERMEDIATE,
    PKL_DATA_IMPORTS,
    INVESTMENT_CYCLE_DURATION_YEARS,
)

from mppsteel.utility.location_utility import (
    country_mapping_fixer,
    country_matcher,
    match_country,
    get_region_from_country_code,
)

from mppsteel.utility.log_utility import get_logger

logger = get_logger("TCO & Abatement switches")


def tco_regions_ref_generator(
    electricity_cost_scenario: str, grid_scenario: str, hydrogen_cost_scenario: str
) -> pd.DataFrame:

    carbon_tax_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "carbon_tax_timeseries", "df"
    )
    variable_costs_regional = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "variable_costs_regional", "df"
    )
    power_model = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "power_model_formatted", "df"
    )
    hydrogen_model = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "hydrogen_model_formatted", "df"
    )
    opex_values_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_dict", "df")
    business_cases = load_business_cases()
    calculated_s1_emissivity = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "calculated_s1_emissivity", "df"
    )
    capex_df = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_switching_df", "df")
    country_ref_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    steel_plants = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
    )
    steel_plant_country_codes = list(steel_plants["country_code"].unique())
    technologies = SWITCH_DICT.keys()
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in tqdm(
        year_range, total=len(year_range), desc="All Regions TCO: Year Loop"
    ):
        for country_code in tqdm(
            steel_plant_country_codes,
            total=len(steel_plant_country_codes),
            desc="All Regions TCO: Region Loop",
        ):
            for tech in technologies:
                tco_df = tco_calc(
                    country_code,
                    year,
                    tech,
                    carbon_tax_df,
                    business_cases,
                    variable_costs_regional,
                    power_model,
                    hydrogen_model,
                    opex_values_dict["other_opex"],
                    calculated_s1_emissivity,
                    country_ref_dict,
                    capex_df,
                    INVESTMENT_CYCLE_DURATION_YEARS,
                    electricity_cost_scenario,
                    grid_scenario,
                    hydrogen_cost_scenario,
                )
                df_list.append(tco_df)
    return pd.concat(df_list).reset_index(drop=True)


def create_full_steel_plant_ref(eur_usd_rate: float) -> pd.DataFrame:
    logger.info(
        "Adding Green Premium Values and year and technology index to steel plant data"
    )
    green_premium_timeseries = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "green_premium_timeseries", "df"
    )
    variable_costs_regional = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "variable_costs_regional", "df"
    )
    steel_plants = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
    )
    steel_plant_ref = steel_plants[
        ["plant_id", "plant_name", "country_code", "technology_in_2020"]
    ].copy()
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    df_list = []
    for year in tqdm(
        year_range, total=len(year_range), desc="Steel Plant TCO Switches: Year Loop"
    ):
        sp_c = steel_plant_ref.copy()
        sp_c["year"] = year
        df_list.append(sp_c)
    steel_plant_ref = pd.concat(df_list).reset_index(drop=True)
    steel_plant_ref["discounted_green_premium"] = ""

    def value_mapper(row, enum_dict: dict):
        start_year = row[enum_dict["year"]]
        gp_arr = np.array([])
        year_range = range(start_year, start_year + INVESTMENT_CYCLE_DURATION_YEARS + 1)
        for year in year_range:
            year_loop_val = min(MODEL_YEAR_END, year)
            green_premium_value = calculate_green_premium(
                variable_costs_regional,
                steel_plants,
                green_premium_timeseries,
                row[enum_dict["country_code"]],
                row[enum_dict["plant_name"]],
                row[enum_dict["technology_in_2020"]],
                year_loop_val,
                eur_usd_rate,
            )
            gp_arr = np.append(gp_arr, green_premium_value)
        discounted_gp_arr = npf.npv(DISCOUNT_RATE, gp_arr)
        row[enum_dict["discounted_green_premium"]] = discounted_gp_arr
        return row

    logger.info("Calculating green premium values")
    tqdma.pandas(desc="Apply Green Premium Values")
    enumerated_cols = enumerate_iterable(steel_plant_ref.columns)
    steel_plant_ref = steel_plant_ref.progress_apply(
        value_mapper, enum_dict=enumerated_cols, axis=1, raw=True
    )
    pair_list = []
    for tech in SWITCH_DICT:
        pair_list.append(list(itertools.product([tech], SWITCH_DICT[tech])))
    all_tech_switch_combinations = [item for sublist in pair_list for item in sublist]
    df_list = []
    logger.info("Generating Year range reference")
    for combination in tqdm(
        all_tech_switch_combinations,
        total=len(all_tech_switch_combinations),
        desc="Steel Plant TCO Switches: Technology Loop",
    ):
        sp_c = steel_plant_ref.copy()
        sp_c["base_tech"] = combination[0]
        sp_c["switch_tech"] = combination[1]
        df_list.append(sp_c)
    steel_plant_full_ref = pd.concat(df_list).reset_index(drop=True)
    steel_plant_full_ref.set_index(
        ["year", "country_code", "base_tech", "switch_tech"], inplace=True
    )
    return steel_plant_full_ref


def map_region_tco_to_plants(
    steel_plant_ref: pd.DataFrame, opex_capex_ref: pd.DataFrame
) -> pd.DataFrame:
    logger.info("Mapping Regional emissions dict to plants")

    opex_capex_ref_c = opex_capex_ref.reset_index(drop=True).copy()
    
    opex_capex_ref_c.rename(
        {"start_technology": "base_tech", "end_technology": "switch_tech"},
        axis="columns",
        inplace=True,
    )
    opex_capex_ref_c.set_index(
        ["year", "country_code", "base_tech", "switch_tech"], inplace=True
    )
    logger.info("Joining tco values on steel plant df")
    combined_df = steel_plant_ref.join(opex_capex_ref_c, how="left").reset_index()

    def value_mapper(row):
        opex = float(row["capex_value"] + row["discounted_opex"])
        if row.switch_tech in LOW_CARBON_TECHS:
            opex -= float(row["discounted_green_premium"])
        row["tco"] = float(opex / INVESTMENT_CYCLE_DURATION_YEARS)
        return row

    combined_df["tco"] = 0
    combined_df = combined_df.apply(value_mapper, axis=1)

    new_col_order = [
        "year",
        "plant_id",
        "plant_name",
        "country_code",
        "base_tech",
        "switch_tech",
        "capex_value",
        "discounted_opex",
        "discounted_green_premium",
        "tco",
    ]
    return combined_df[new_col_order]


def get_abatement_difference(
    df: pd.DataFrame,
    year: int,
    country_code: str,
    base_tech: str,
    switch_tech: str,
    emission_type: str,
    emissivity_mapper: dict,
    date_span: int,
) -> float:
    @lru_cache(maxsize=200000)
    def return_abatement_value(base_tech_sum, switch_tech_sum):
        return float(base_tech_sum - switch_tech_sum)

    year_range = range(year, year + date_span)
    emission_val = emissivity_mapper[emission_type]
    base_tech_list = []
    switch_tech_list = []
    for eval_year in year_range:
        year_loop_val = min(MODEL_YEAR_END, eval_year)
        base_tech_val = df.loc[year_loop_val, country_code, base_tech][emission_val]
        switch_tech_val = df.loc[year_loop_val, country_code, switch_tech][emission_val]
        base_tech_list.append(base_tech_val)
        switch_tech_list.append(switch_tech_val)
    return return_abatement_value(sum(base_tech_list), sum(switch_tech_list))


def emissivity_abatement(combined_emissivity: pd.DataFrame, scope: str) -> pd.DataFrame:
    logger.info(
        "Getting all Emissivity Abatement combinations for all technology switches"
    )
    emissivity_mapper = {
        "s1": "s1_emissivity",
        "s2": "s2_emissivity",
        "s3": "s3_emissivity",
        "combined": "combined_emissivity",
    }
    combined_emissivity = (
        combined_emissivity.reset_index(drop=True)
        .set_index(["year", "country_code", "technology"])
        .copy()
    )
    country_codes = combined_emissivity.index.get_level_values(1).unique()
    technologies = combined_emissivity.index.get_level_values(2).unique()
    df_list = []
    year_range = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
    for year in tqdm(
        year_range, total=len(year_range), desc="Emission Abatement: Year Loop"
    ):
        for country_code in country_codes:
            for base_tech in technologies:
                for switch_tech in SWITCH_DICT[base_tech]:
                    value_difference = get_abatement_difference(
                        combined_emissivity[["combined_emissivity"]],
                        year,
                        country_code,
                        base_tech,
                        switch_tech,
                        "combined",
                        emissivity_mapper,
                        INVESTMENT_CYCLE_DURATION_YEARS,
                    )
                    entry = {
                        "year": year,
                        "country_code": country_code,
                        "base_tech": base_tech,
                        "switch_tech": switch_tech,
                        f"abated_{scope}_emissivity": value_difference,
                    }
                    df_list.append(entry)
    return pd.DataFrame(df_list)


@timer_func
def tco_presolver_reference(scenario_dict, serialize: bool = False) -> pd.DataFrame:
    country_ref_dict = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "country_reference_dict", "df"
    )
    electricity_cost_scenario = scenario_dict["electricity_cost_scenario"]
    grid_scenario = scenario_dict["grid_scenario"]
    hydrogen_cost_scenario = scenario_dict["hydrogen_cost_scenario"]
    eur_usd_rate = scenario_dict["eur_usd"]
    opex_capex_reference_data = tco_regions_ref_generator(
        electricity_cost_scenario, grid_scenario, hydrogen_cost_scenario
    )
    tco_summary = opex_capex_reference_data.copy()
    tco_summary["tco"] = tco_summary["discounted_opex"] + tco_summary["capex_value"]
    tco_summary["tco"] = tco_summary["tco"] / INVESTMENT_CYCLE_DURATION_YEARS
    tco_summary['region']= tco_summary['country_code'].apply(lambda x: get_region_from_country_code(
        x, "rmi_region", country_ref_dict
    ))
    steel_plant_ref = create_full_steel_plant_ref(eur_usd_rate)
    tco_reference_data = map_region_tco_to_plants(
        steel_plant_ref, opex_capex_reference_data
    )
    tco_reference_data = add_results_metadata(
        tco_reference_data, scenario_dict, single_line=True
    )
    if serialize:
        logger.info(f"-- Serializing dataframe")
        serialize_file(
            tco_summary, PKL_DATA_INTERMEDIATE, "tco_summary_data"
        )  # This version does not incorporate green premium
        serialize_file(tco_reference_data, PKL_DATA_INTERMEDIATE, "tco_reference_data")
    return tco_reference_data


@timer_func
def abatement_presolver_reference(
    scenario_dict, serialize: bool = False
) -> pd.DataFrame:
    logger.info("Running Abatement Reference Sheet")
    calculated_emissivity_combined = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "calculated_emissivity_combined", "df"
    )
    emissivity_abatement_switches = emissivity_abatement(
        calculated_emissivity_combined, scope="combined"
    )
    emissivity_abatement_switches = add_results_metadata(
        emissivity_abatement_switches, scenario_dict, single_line=True
    )
    if serialize:
        logger.info(f"-- Serializing dataframe")
        serialize_file(
            emissivity_abatement_switches,
            PKL_DATA_INTERMEDIATE,
            "emissivity_abatement_switches",
        )
    return emissivity_abatement_switches
