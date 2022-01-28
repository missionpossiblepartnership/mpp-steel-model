"""Main solving script for deciding investment decisions."""

from functools import lru_cache

import pandas as pd
import numpy_financial as npf

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.model.emissions_reference_tables import get_s2_emissions
from mppsteel.model.financial_functions import generate_capex_financial_summary

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger,
    enumerate_columns,
)
from mppsteel.model_config import (
    MODEL_YEAR_END, PKL_DATA_INTERMEDIATE, DISCOUNT_RATE,
    INVESTMENT_CYCLE_LENGTH, STEEL_PLANT_LIFETIME
)
from mppsteel.utility.reference_lists import (
    SWITCH_DICT
)

# Create logger
logger = get_logger("TCO")

def carbon_tax_estimate(s1_emissions_value: dict, scope2_emission_value: float, carbon_tax_value: pd.DataFrame):
    return (scope2_emission_value + s1_emissions_value) * carbon_tax_value

@lru_cache(maxsize=200000)
def get_steel_making_cost(variable_tech_cost: float, primary_capacity: float, secondary_capacity: float, technology: str, eur_usd_rate: float):
    combined_capacity = primary_capacity + secondary_capacity
    if technology == 'EAF':
        variable_cost_value = (variable_tech_cost * primary_capacity) + (variable_tech_cost * secondary_capacity)
    else:
        variable_cost_value = variable_tech_cost * primary_capacity
    return combined_capacity * (variable_cost_value / combined_capacity) / combined_capacity / eur_usd_rate


def calculate_green_premium(variable_costs, steel_plant_df, green_premium_timeseries, country_code, plant_name, technology_2020, year, eur_usd_rate: float):
    variable_tech_cost = variable_costs.loc[country_code, year, technology_2020].values[0]
    steel_plant_df_c = steel_plant_df.loc[steel_plant_df['plant_name'] == plant_name].copy()
    primary_capacity = steel_plant_df_c['primary_capacity_2020'].values[0]
    secondary_capacity = steel_plant_df_c['secondary_capacity_2020'].values[0]
    green_premium = green_premium_timeseries.loc[green_premium_timeseries['year'] == year]['value']
    steel_making_cost = get_steel_making_cost(variable_tech_cost, primary_capacity, secondary_capacity, technology_2020, eur_usd_rate)
    return steel_making_cost * green_premium

def get_opex_costs(
    country_code: str,
    year: int,
    variable_costs_df: pd.DataFrame,
    opex_df: pd.DataFrame,
    s1_emissions_ref: dict,
    carbon_tax_timeseries: pd.DataFrame,
    scope2_emission_value: float,
):
    variable_costs = variable_costs_df.loc[country_code, year]
    opex_costs = opex_df.swaplevel().loc[year]
    carbon_tax_value = carbon_tax_timeseries.set_index('year').loc[year]['value']
    s1_emissions_value = s1_emissions_ref.loc[year]

    # Carbon Tax Result
    carbon_tax_result = carbon_tax_estimate(s1_emissions_value, scope2_emission_value, carbon_tax_value)
    variable_costs.rename(mapper={'cost': 'value'},axis=1, inplace=True)
    carbon_tax_result.rename(mapper={'emissions': 'value'},axis=1, inplace=True)
    total_opex = variable_costs + opex_costs + carbon_tax_result
    total_opex.drop(['Charcoal mini furnace', 'Close plant'], inplace=True)
    total_opex.rename(mapper={'value': 'opex'},axis=1, inplace=True)
    return total_opex

def capex_getter(capex_df, switch_dict, year, start_tech, end_tech):
    year = min(MODEL_YEAR_END, year)
    if end_tech in switch_dict[start_tech]:
        return capex_df.loc[year, start_tech, end_tech][0]
    return 0

def calculate_capex(capex_df: pd.DataFrame, start_year: int, base_tech: str):
    df = pd.DataFrame({'start_technology': base_tech, 'end_technology': SWITCH_DICT[base_tech], 'year': start_year, 'capex_value': ''})
    c_df = capex_df.reset_index().set_index(['Year', 'Start Technology', 'New Technology']).copy()
    def value_mapper(row, enum_dict):
        row[enum_dict['capex_value']] = capex_getter(c_df, SWITCH_DICT, start_year, base_tech, row[enum_dict['end_technology']])
        return row
    enumerated_cols = enumerate_columns(df.columns)
    df = df.apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    return df.set_index(['year', 'start_technology'])

def get_discounted_opex_values(
    country_code: tuple,
    year_start: int,
    carbon_tax_df: pd.DataFrame,
    business_cases: pd.DataFrame,
    variable_cost_summary: pd.DataFrame,
    power_model: dict,
    hydrogen_model: dict,
    other_opex_df: pd.DataFrame,
    s1_emissions_df: pd.DataFrame,
    country_ref_dict: pd.DataFrame,
    year_interval: int,
    int_rate: float,
    electricity_cost_scenario: str,
    grid_scenario: str,
    hydrogen_cost_scenario: str,
    base_tech: str,
    ):

    year_range = range(year_start, year_start+year_interval+1)
    df_list = []
    for year in year_range:
        year = min(MODEL_YEAR_END, year)
        s2_value = get_s2_emissions(
            power_model, hydrogen_model, business_cases, country_ref_dict, year,
            country_code, base_tech, electricity_cost_scenario,
            grid_scenario, hydrogen_cost_scenario)
        df = get_opex_costs(
            country_code,
            year,
            variable_cost_summary,
            other_opex_df,
            s1_emissions_df,
            carbon_tax_df,
            scope2_emission_value=s2_value,
        )
        df['year'] = year
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
    other_opex_df: pd.DataFrame, s1_emissions_df: pd.DataFrame, country_ref_dict: pd.DataFrame, capex_df: pd.DataFrame,
    investment_cycle: int, electricity_cost_scenario: str,
    grid_scenario: str, hydrogen_cost_scenario: str,
    ):
    opex_values = get_discounted_opex_values(
        country_code, start_year, carbon_tax_df, business_cases,
        variable_cost_summary, power_model,
        hydrogen_model, other_opex_df, s1_emissions_df,
        country_ref_dict=country_ref_dict,
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
    tqdma.pandas(desc="Capex Values for Levelised Steel")
    enumerated_cols = enumerate_columns(df_temp.columns)
    df_temp.progress_apply(value_mapper, enum_dict=enumerated_cols, axis=1, raw=True)
    df_temp.drop(['value'], axis=1, inplace=True)
    df_temp['value'] = value_list
    return df_temp

def levelised_steelmaking_cost(year: pd.DataFrame, include_greenfield: bool = False):
    # Levelised of cost of steelmaking = OtherOpex + VariableOpex + RenovationCapex w/ 7% over 20 years (+ GreenfieldCapex w/ 7% over 40 years)
    df_list = []
    variable_costs_regional = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'variable_costs_regional', 'df')
    opex_values_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'capex_dict', 'df')
    for plant_country_ref in variable_costs_regional.index.get_level_values(0).unique():
        variable_costs = variable_costs_regional.loc[plant_country_ref, year]
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
