"""Main solving script for deciding investment decisions."""

import math
import pandas as pd
import numpy as np
import numpy_financial as npf

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger,
    enumerate_columns,
)

from mppsteel.utility.transform_units import (
    mwh_gj
)

from mppsteel.model_config import (
    MODEL_YEAR_START, PKL_DATA_INTERMEDIATE, 
    DISCOUNT_RATE, INVESTMENT_CYCLE_LENGTH,
    STEEL_PLANT_LIFETIME,
    TCO_RANK_1_SCALER, TCO_RANK_2_SCALER,
    ABATEMENT_RANK_2, ABATEMENT_RANK_3,
    COST_SCENARIO_MAPPER, GRID_DECARBONISATION_SCENARIOS
)

from mppsteel.utility.reference_lists import (
    SWITCH_DICT
)

from mppsteel.model.financial_functions import generate_capex_financial_summary

from mppsteel.data_loading.pe_model_formatter import (
    power_data_getter, hydrogen_data_getter, RE_DICT
)

# Create logger
logger = get_logger("TCO")


def carbon_tax_estimate(s3_emissions_ref: dict, scope2_emission_value: float, carbon_tax_df: pd.DataFrame, year: int):
    carbon_tax = carbon_tax_df.set_index('year').loc[year]['value']
    return (scope2_emission_value + s3_emissions_ref.loc[year]) * carbon_tax

def get_steel_making_cost(steel_plant_df: pd.DataFrame, variable_cost_df, plant_name: str, technology: str, eur_usd_rate: float):
    steel_plant_df_c = steel_plant_df.loc[steel_plant_df['plant_name'] == plant_name].copy()
    primary = steel_plant_df_c['primary_capacity_2020'].values[0]
    secondary = steel_plant_df_c['secondary_capacity_2020'].values[0]
    combined = steel_plant_df_c['combined_capacity'].values[0]
    variable_tech_cost = variable_cost_df.loc[technology].values[0]

    if technology == 'EAF':
        variable_cost_value = (variable_tech_cost * primary) + (variable_tech_cost * secondary)
    else:
        variable_cost_value = variable_tech_cost * primary

    return combined * (variable_cost_value / combined) / combined / eur_usd_rate


def calculate_green_premium(variable_costs, steel_plant_capacity, green_premium_timeseries, country_code, plant_name, technology_2020, year, eur_usd_rate: float):
    variable_costs = variable_costs.loc[country_code, year]
    steel_making_cost = get_steel_making_cost(steel_plant_capacity, variable_costs, plant_name, technology_2020, eur_usd_rate)
    green_premium = green_premium_timeseries.loc[green_premium_timeseries['year'] == year]['value']
    return steel_making_cost * green_premium

def get_opex_costs(
    country_code: str,
    year: int,
    variable_costs_df: pd.DataFrame,
    opex_df: pd.DataFrame,
    s3_emissions_ref: dict,
    carbon_tax_timeseries: pd.DataFrame,
    scope2_emission_value: float,
):
    variable_costs = variable_costs_df.loc[country_code, year]
    opex_costs = opex_df.swaplevel().loc[year]

    # Carbon Tax Result
    carbon_tax_result = carbon_tax_estimate(s3_emissions_ref, scope2_emission_value, carbon_tax_timeseries, year)

    variable_costs.rename(mapper={'cost': 'value'},axis=1, inplace=True)
    carbon_tax_result.rename(mapper={'emissions': 'value'},axis=1, inplace=True)

    total_opex = variable_costs + opex_costs + carbon_tax_result
    total_opex.drop(['Charcoal mini furnace', 'Close plant'], inplace=True)

    total_opex.rename(mapper={'value': 'opex'},axis=1, inplace=True)
    
    return total_opex

def capex_getter(capex_df, switch_dict, year, start_tech, end_tech):
    c_df = capex_df.reset_index().set_index(['Year', 'Start Technology', 'New Technology'])
    req_year = year
    if year > 2050:
        req_year = 2050
    if end_tech in switch_dict[start_tech]:
        return c_df.loc[req_year, start_tech, end_tech][0]
    return 0

def calculate_capex(capex_df: pd.DataFrame, start_year: int, base_tech: str):
    df = pd.DataFrame({'start_technology': base_tech, 'end_technology': SWITCH_DICT[base_tech], 'year': start_year, 'capex_value': ''})
    def value_mapper(row, enum_dict):
        row[enum_dict['capex_value']] = capex_getter(capex_df, SWITCH_DICT, start_year, base_tech, row[enum_dict['end_technology']])
        return row
    enumerated_cols = enumerate_columns(df.columns)
    df = df.apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    return df.set_index(['year', 'start_technology'])

def get_s2_emissions(power_model: dict, hydrogen_model: dict, business_cases: pd.DataFrame, year: int, country_code: str, technology: str, electricity_cost_scenario: str, grid_scenario: str, hydrogen_cost_scenario: str):
    electricity_cost_scenario = COST_SCENARIO_MAPPER[electricity_cost_scenario]
    grid_scenario = GRID_DECARBONISATION_SCENARIOS[grid_scenario]
    hydrogen_cost_scenario = COST_SCENARIO_MAPPER[hydrogen_cost_scenario]

    electricity_emissions = power_data_getter(
        power_model,
        'emissions',
        year,
        country_code,
        grid_scenario=grid_scenario,
        cost_scenario=electricity_cost_scenario)

    h2_emissions = hydrogen_data_getter(
        hydrogen_model,
        'emissions',
        year,
        country_code,
        cost_scenario=hydrogen_cost_scenario)

    bcases = business_cases.loc[business_cases["technology"] == technology].copy().reset_index(drop=True)
    hydrogen_consumption = 0
    electricity_consumption = 0
    if 'Hydrogen' in bcases['material_category'].unique():
        hydrogen_consumption =  bcases[bcases['material_category'] == 'Hydrogen']['value'].values[0]
    if 'Electricity' in bcases['material_category'].unique():
        electricity_consumption =  bcases[bcases['material_category'] == 'Electricity']['value'].values[0]

    total_s2_emission = ((h2_emissions / 1000) * hydrogen_consumption) + (mwh_gj(electricity_emissions, 'larger') * electricity_consumption)

    return total_s2_emission

def get_discounted_opex_values(
    country_code: tuple,
    year_start: int,
    carbon_tax_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    variable_cost_summary: pd.DataFrame,
    power_model: dict,
    hydrogen_model: dict,
    other_opex_df: pd.DataFrame,
    s3_emissions_df: pd.DataFrame,
    year_interval: int,
    int_rate: float,
    electricity_cost_scenario: str,
    grid_scenario: str,
    hydrogen_cost_scenario: str,
    base_tech: str,
    ):

    year_range = range(year_start, year_start+year_interval+1)
    pd.DataFrame()
    df_list = []
    for year in year_range:
        year_ref = year
        if year > 2050:
            year_ref = 2050
        s2_value = get_s2_emissions(
            power_model, hydrogen_model, business_cases, year_ref,
            country_code, base_tech, electricity_cost_scenario,
            grid_scenario, hydrogen_cost_scenario)
        df = get_opex_costs(
            country_code,
            year_ref,
            variable_cost_summary,
            other_opex_df,
            s3_emissions_df,
            carbon_tax_df,
            scope2_emission_value=s2_value,
        )
        df['year'] = year_ref
        df_list.append(df)
    df_combined = pd.concat(df_list)
    new_df = pd.DataFrame(index=SWITCH_DICT[base_tech], columns=['discounted_opex'])
    for technology in new_df.index.values:
        new_df.loc[technology, 'discounted_opex'] = npf.npv(int_rate, df_combined.loc[technology]['opex'].values)
    return new_df

def tco_calc(
    country_code, start_year: int, base_tech: str, carbon_tax_df: pd.DataFrame,
    business_cases: pd.DataFrame, variable_cost_summary: pd.DataFrame,
    power_model: dict, hydrogen_model: dict,
    other_opex_df: pd.DataFrame, s3_emissions_df: pd.DataFrame, capex_df: pd.DataFrame,
    investment_cycle: int, electricity_cost_scenario: str,
    grid_scenario: str, hydrogen_cost_scenario: str,
    ):
    opex_values = get_discounted_opex_values(
        country_code, start_year, carbon_tax_df, business_cases,
        variable_cost_summary, power_model,
        hydrogen_model, other_opex_df, s3_emissions_df,
        year_interval=investment_cycle, int_rate=DISCOUNT_RATE,
        electricity_cost_scenario=electricity_cost_scenario,
        grid_scenario=grid_scenario, hydrogen_cost_scenario=hydrogen_cost_scenario,
        base_tech=base_tech,
        )
    capex_values = calculate_capex(capex_df, start_year, base_tech).swaplevel().loc[base_tech].groupby('end_technology').sum()
    opex_values.index.rename('end_technology', inplace=True)
    capex_opex_values = capex_values.join(opex_values, on='end_technology')
    capex_opex_values['year'] = start_year
    capex_opex_values['country_code'] = country_code
    capex_opex_values['start_technology'] = base_tech
    column_order = ['country_code', 'year', 'start_technology', 'end_technology', 'capex_value', 'discounted_opex']
    capex_opex_values.reset_index(inplace=True)
    return capex_opex_values[column_order]

def capex_values_for_levelised_steelmaking(capex_df: pd.DataFrame, int_rate: float, year: int, payments: int):
    df_temp = capex_df.swaplevel().loc[year]
    value_list = []
    def value_mapper(row, enum_dict):
        capex_value = - generate_capex_financial_summary(row[enum_dict['value']], int_rate, payments)['total_interest']
        value_list.append(capex_value)
    enumerated_cols = enumerate_columns(df_temp.columns)
    df_temp.apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    df_temp.drop(['value'], axis=1, inplace=True)
    df_temp['value'] = value_list
    return df_temp

def levelised_steelmaking_cost(year: pd.DataFrame, include_greenfield: bool = False):
    # Levelised of cost of steelmaking = OtherOpex + VariableOpex + RenovationCapex w/ 7% over 20 years (+ GreenfieldCapex w/ 7% over 40 years)
    df_list = []
    all_plant_variable_costs_summary = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'all_plant_variable_costs_summary', 'df')
    opex_values_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'capex_dict', 'df')
    for plant_country_ref in all_plant_variable_costs_summary.index.get_level_values(0).unique():
        variable_costs = all_plant_variable_costs_summary.loc[plant_country_ref, year]
        other_opex = opex_values_dict['other_opex'].swaplevel().loc[year]
        brownfield_capex = capex_values_for_levelised_steelmaking(opex_values_dict['brownfield'], DISCOUNT_RATE, year, INVESTMENT_CYCLE_LENGTH)
        variable_costs.rename(mapper={'cost': 'value'}, axis=1, inplace=True)
        variable_costs.rename(mapper={'technology': 'Technology'}, axis=0, inplace=True)
        combined_df = variable_costs + other_opex + brownfield_capex
        if include_greenfield:
            greenfield_capex = capex_values_for_levelised_steelmaking(opex_values_dict['greenfield'], DISCOUNT_RATE, year, STEEL_PLANT_LIFETIME)
            combined_df = variable_costs + other_opex + greenfield_capex + brownfield_capex
        combined_df['plant_country_ref'] = plant_country_ref
        df_list.append(combined_df)
    df = pd.concat(df_list)
    return df.reset_index().rename(mapper={'index': 'technology'},axis=1).set_index(['plant_country_ref', 'technology'])

def normalise_data(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))

def scale_data(df: pd.DataFrame, cols = list):
    df_c = df.copy()[cols]
    df_c[cols] = 1 - normalise_data(df_c[cols].values)
    return df_c

def tco_ranker_logic(x: float, min_value: float):
    if min_value is None: # check for this
        # print('NoneType value')
        return 1
    if x > min_value * TCO_RANK_2_SCALER:
        return 3
    elif x > min_value * TCO_RANK_1_SCALER:
        return 2
    else:
        return 1

def abatement_ranker_logic(x: float):
    if x < ABATEMENT_RANK_3:
        return 3
    elif x < ABATEMENT_RANK_2:
        return 2
    else:
        return 1

def tco_min_ranker(df: pd.DataFrame, value_col: list, rank_only: bool = False):
    df_c = df.copy().sort_values(value_col, ascending=False).copy()
    min_value = df_c.min().values[0]
    if rank_only:
        df_c['rank_score'] = df_c[value_col[0]].apply(lambda x: tco_ranker_logic(x, min_value))
        df_c.drop(value_col, axis=1, inplace=True)
        return df_c
    return df_c

def abatement_min_ranker(df: pd.DataFrame, start_tech: str, year: int, cols: list, rank_only: bool = False):
    df_c = df.copy()
    df_subset = df_c.loc[year, start_tech][cols].sort_values(cols, ascending=False).copy()
    if rank_only:
        df_subset['rank_score'] = df_subset[cols[0]].apply(lambda x: abatement_ranker_logic(x)) # get fix for list subset
        df_subset.drop(cols, axis=1, inplace=True)
        return df_subset
    return df_subset
