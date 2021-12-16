"""Main solving script for deciding investment decisions."""

import pandas as pd
import numpy as np

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger
)

from mppsteel.model_config import (
    PKL_FOLDER, SWITCH_DICT, DISCOUNT_RATE,
    TCO_RANK_1_SCALER, TCO_RANK_2_SCALER,
    ABATEMENT_RANK_2, ABATEMENT_RANK_3,
    EUR_USD_CONVERSION
)

from mppsteel.model.tco_and_emissions import (
    calculate_present_values, compare_capex,
    generate_capex_financial_summary,
)

# Create logger
logger = get_logger("Solver")

def carbon_tax_estimate(emission_dict_ref: dict, carbon_tax_df: pd.DataFrame, year: int):
    year_ref = year
    if year > 2050:
        year_ref = 2050
    carbon_tax = carbon_tax_df.set_index('year').loc[year_ref]['value']
    return (emission_dict_ref['s2'].loc[year_ref] + emission_dict_ref['s3'].loc[year_ref]) * carbon_tax

def get_steel_making_costs(steel_plant_df: pd.DataFrame, variable_cost_df, plant_name: str, technology: str):
    conversion = EUR_USD_CONVERSION
    if technology == 'Not operating':
        return 0
    else:
        primary = steel_plant_df[steel_plant_df['plant_name'] == plant_name]['primary_capacity_2020'].values[0]
        secondary = steel_plant_df[steel_plant_df['plant_name'] == plant_name]['secondary_capacity_2020'].values[0]
        variable_tech_cost = variable_cost_df.loc[technology].values[0]
        if technology == 'EAF':
            variable_cost_value = (variable_tech_cost * primary) + (variable_tech_cost * secondary)
        else:
            variable_cost_value = variable_tech_cost * primary

        return (primary + secondary) * (variable_cost_value / (primary+secondary)) / (primary + secondary) / conversion

def get_opex_costs(
    plant: tuple,
    year: int,
    variable_costs_df: pd.DataFrame,
    opex_df: pd.DataFrame,
    emissions_dict_ref: dict,
    carbon_tax_timeseries: pd.DataFrame,
    green_premium_timeseries: pd.DataFrame,
    steel_plant_capacity: pd.DataFrame,
    include_carbon_tax: bool = False,
    include_green_premium: bool = False,
):
    combined_row = str(plant.abundant_res) + str(plant.ccs_available) + str(plant.cheap_natural_gas)
    variable_costs = variable_costs_df.loc[combined_row, year]
    opex_costs = opex_df.swaplevel().loc[year]

    # single results
    carbon_tax_result = 0
    if include_carbon_tax:
        carbon_tax_result = carbon_tax_estimate(emissions_dict_ref, carbon_tax_timeseries, year)
    green_premium_value = 0
    if include_green_premium:
        steel_making_cost = get_steel_making_costs(steel_plant_capacity, variable_costs, plant.plant_name, plant.technology_in_2020)
        green_premium = green_premium_timeseries[green_premium_timeseries['year'] == year]['value'].values[0]
        green_premium_value = (steel_making_cost * green_premium)

    variable_costs.rename(mapper={'cost': 'value'},axis=1, inplace=True)
    carbon_tax_result.rename(mapper={'emissions': 'value'},axis=1, inplace=True)
    total_opex = variable_costs + opex_costs + carbon_tax_result - green_premium_value
    total_opex.rename(mapper={'value': 'opex'},axis=1, inplace=True)
    total_opex.loc['Close plant', 'opex'] = 0
    total_opex.drop(['Charcoal mini furnace', 'Close plant'],inplace=True)
    return total_opex

def calculate_capex(start_year):
    df_list = []
    for base_tech in SWITCH_DICT.keys():
        for switch_tech in SWITCH_DICT[base_tech]:
            if switch_tech == 'Close plant':
                pass
            else:
                df = compare_capex(base_tech, switch_tech, start_year)
                df_list.append(df)
    full_df = pd.concat(df_list)
    return full_df.set_index(['year', 'start_technology'])

def get_capex_year(capex_df: pd.DataFrame, start_tech: str, end_tech: str, switch_year: int):
    df_c = None
    if switch_year > 2050:
        df_c = capex_df.loc[2050, start_tech].copy()
    elif 2020 <= switch_year <= 2050:
        df_c = capex_df.loc[switch_year, start_tech].copy()

    raw_switch_value = df_c.loc[df_c['new_technology'] == end_tech]['value'].values[0]

    financial_summary = generate_capex_financial_summary(
        principal=raw_switch_value,
        interest_rate=DISCOUNT_RATE,
        years=20,
        downpayment=0,
        compounding_type='annual',
        rounding=2
    )

    return financial_summary

def create_emissions_dict():
    calculated_s1_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s1_emissions', 'df')
    calculated_s2_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s2_emissions', 'df')
    calculated_s3_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s3_emissions', 'df')
    return {'s1': calculated_s1_emissions, 's2': calculated_s2_emissions, 's3': calculated_s3_emissions}


def get_discounted_opex_values(
    plant: tuple, year_start: int, carbon_tax_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    variable_cost_summary: pd.DataFrame,
    green_premium_timeseries: pd.DataFrame,
    other_opex_df: pd.DataFrame,
    year_interval: int = 20, int_rate: float = 0.07):
    year_range = range(year_start, year_start+year_interval+1)
    df_list = []
    emissions_dict = create_emissions_dict()
    for year in year_range:
        year_ref = year
        if year > 2050:
            year_ref = 2050
        df = get_opex_costs(
            plant,
            year_ref,
            variable_cost_summary,
            other_opex_df,
            emissions_dict,
            carbon_tax_df,
            green_premium_timeseries,
            steel_plant_df,
            include_carbon_tax=True,
            include_green_premium=True
        )
        df['year'] = year_ref
        df_list.append(df)
    df_combined = pd.concat(df_list)
    technologies = df.index
    new_df = pd.DataFrame(index=technologies, columns=['value'])
    for technology in technologies:
        values = df_combined.loc[technology]['opex'].values
        discounted_values = calculate_present_values(values, int_rate)
        new_df.loc[technology, 'value'] = sum(discounted_values)
    return new_df

def tco_calc(
    plant, start_year: int, plant_tech: str, carbon_tax_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame, variable_cost_summary: pd.DataFrame, 
    green_premium_timeseries: pd.DataFrame, other_opex_df: pd.DataFrame, 
    investment_cycle: int = 20
    ):
    opex_values = get_discounted_opex_values(
        plant, start_year, carbon_tax_df, steel_plant_df, variable_cost_summary,
        green_premium_timeseries, other_opex_df, int_rate=DISCOUNT_RATE
        )
    capex_values = calculate_capex(start_year).swaplevel().loc[plant_tech].groupby('end_technology').sum()
    return capex_values + opex_values / investment_cycle

def capex_values_for_levilised_steelmaking(capex_df: pd.DataFrame, int_rate: float, year: int, payments: int):
    df_temp = capex_df.swaplevel().loc[year]
    value_list = []
    for tech_row in df_temp.itertuples():
        capex_value = - generate_capex_financial_summary(tech_row.value, int_rate, payments)['total_interest']
        value_list.append(capex_value)
    df_temp.drop(['value'], axis=1, inplace=True)
    df_temp['value'] = value_list
    return df_temp


def levelised_steelmaking_cost(year: pd.DataFrame, include_greenfield: bool = False):
    # Levelised of cost of steelmaking = OtherOpex + VariableOpex + RenovationCapex w/ 7% over 20 years (+ GreenfieldCapex w/ 7% over 40 years)
    df_list = []
    all_plant_variable_costs_summary = read_pickle_folder(PKL_FOLDER, 'all_plant_variable_costs_summary', 'df')
    opex_values_dict = read_pickle_folder(PKL_FOLDER, 'capex_dict', 'df')
    for iteration in all_plant_variable_costs_summary.index.get_level_values(0).unique():
        variable_costs = all_plant_variable_costs_summary.loc[iteration, year]
        other_opex = opex_values_dict['other_opex'].swaplevel().loc[year]
        brownfield_capex = capex_values_for_levilised_steelmaking(opex_values_dict['brownfield'], DISCOUNT_RATE, year, 20)
        variable_costs.rename(mapper={'cost': 'value'}, axis=1, inplace=True)
        variable_costs.rename(mapper={'technology': 'Technology'}, axis=0, inplace=True)
        combined_df = variable_costs + other_opex + brownfield_capex
        if include_greenfield:
            greenfield_capex = capex_values_for_levilised_steelmaking(opex_values_dict['greenfield'], DISCOUNT_RATE, year, 40)
            combined_df = variable_costs + other_opex + greenfield_capex + brownfield_capex
        combined_df['iteration'] = iteration
        df_list.append(combined_df)
    df = pd.concat(df_list)
    return df.reset_index().rename(mapper={'index': 'technology'},axis=1).set_index(['iteration', 'technology'])

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
