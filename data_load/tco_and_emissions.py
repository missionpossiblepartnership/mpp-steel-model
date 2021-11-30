"""Script to generate Switching tables for TCO and emission abatement"""

import pandas as pd
import numpy as np
import numpy_financial as npf

# For logger and units dict
from utils import (
    get_logger, read_pickle_folder, serialize_df)

from model_config import PKL_FOLDER, DISCOUNT_RATE, SWITCH_DICT

# Create logger
logger = get_logger('TCO & Emissions')

def calculate_present_values(values: list, int_rate: float, rounding: int = 2):
    def present_value(value: float, factor: float):
        discount_factor = 1 / ((1 + int_rate)**factor)
        return value * discount_factor
    zipped_values = zip(values, list(range(len(values))))
    present_values = [round(present_value(val, factor), rounding) for val, factor in zipped_values]
    return present_values

def discounted_opex(
    opex_value_dict: dict,
    technology: str,
    interest_rate: float,
    start_year: int = 2020,
    date_span: int = 20,
):
    # 20 year discouted opex
    tech_values = opex_value_dict.loc[technology].copy()
    max_value = tech_values.loc[2050].value
    year_range = range(start_year, start_year+date_span)

    value_list = []
    for year in year_range:
        if year <= 2050:
            value_list.append(tech_values.loc[year].value)

        if year > 2050:
            value_list.append(max_value)

    return calculate_present_values(value_list, interest_rate)

def get_capex_year(
    capex_df: pd.DataFrame, start_tech: str, 
    end_tech: str, switch_year: int,
    interest_rate: float,
    ):
    df_c = None
    if switch_year > 2050:
        df_c = capex_df.loc[2050, start_tech].copy()
    elif 2020 <= switch_year <= 2050:
        df_c = capex_df.loc[switch_year, start_tech].copy()

    raw_switch_value = df_c.loc[df_c['new_technology'] == end_tech]['value'].values[0]

    financial_summary = generate_capex_financial_summary(
        principal=raw_switch_value,
        interest_rate=interest_rate,
        years=20,
        downpayment=0,
        compounding_type='annual',
        rounding=2
    )

    return financial_summary

def generate_capex_financial_summary(
    principal: float,
    interest_rate: float,
    years: int = 20,
    downpayment: float = None,
    compounding_type: str = 'annual',
    rounding: int = 2,
):

    rate = interest_rate
    nper = years

    if compounding_type == 'monthly':
        rate = interest_rate/12
        nper = years*12

    if compounding_type == 'semi-annual':
        rate = interest_rate/2
        nper = years*2

    if downpayment:
        pmt = -1 * downpayment
    else:
        pmt = -(principal*rate)

    fv_calc = npf.fv(
        rate=rate, # annual interest rate
        nper=nper, # total payments (years)
        pmt=pmt, # downpayment
        pv=principal, # value borrowed today
        when='end' # when the initial payment is made
    )

    pmt_calc = npf.pmt(
        rate=rate,
        nper=nper,
        pv=principal,
        when='end'
    )

    ipmt = npf.ipmt(
        rate=rate,
        per=np.arange(nper) + 1,
        nper=nper,
        pv=principal,
        when='end'
    )

    ppmt = npf.ppmt(
        rate=rate,
        per=np.arange(nper) + 1,
        nper=nper,
        pv=principal,
        when='end'
    )

    return {
        'future_value': round(fv_calc, rounding),
        'interest_payments': round(pmt_calc, rounding),
        'total_interest': round(ipmt.sum(), rounding),
        'principal_schedule': ppmt.round(rounding),
        'interest_schedule': ipmt.round(rounding)
    }

def compare_capex(base_tech: str, switch_tech: str, interest_rate: float, start_year: int = 2020, date_span: int = 20):
    other_opex_values = discounted_opex(opex_values_dict, switch_tech, interest_rate, start_year)
    variable_opex_values = discounted_opex(variable_costs, switch_tech, interest_rate, start_year)
    annual_capex_value = get_capex_year(capex_c, base_tech, switch_tech, start_year, interest_rate)['interest_payments']
    annual_capex_array = np.full(20, annual_capex_value) * -1
    discounted_annual_capex_array = calculate_present_values(annual_capex_array, interest_rate)
    years = list(range(start_year, start_year+date_span))
    df = pd.DataFrame(data={
        'start_technology': base_tech,
        'end_technology': switch_tech,
        'years': years,
        'other_opex': other_opex_values,
        'variable_opex': variable_opex_values,
        'annual_capex': discounted_annual_capex_array})
    df['tco'] = df['other_opex'] + df['variable_opex'] + df['annual_capex']
    return df

def calculate_tco(start_year: int, interest_rate: float, output_type: str = 'full'):
    df_list = []
    for base_tech in SWITCH_DICT.keys():
        for switch_tech in SWITCH_DICT[base_tech]:
            if switch_tech == 'Close plant':
                pass
            else:
                df = compare_capex(base_tech, switch_tech, interest_rate, start_year)
                df_list.append(df)
    full_df = pd.concat(df_list)
    full_summary_df = full_df[['start_technology', 'end_technology', 'other_opex', 'variable_opex', 'annual_capex', 'tco']].groupby(by=['start_technology', 'end_technology']).sum()
    if output_type == 'summary':
        return full_summary_df
    if output_type == 'full':
        return full_df

def get_emissions_by_year(df: pd.DataFrame, tech: str, start_year: int = 2020, date_span: int = 20):
    df_c = df.copy()
    df_c = df_c.reorder_levels(['technology', 'year'])
    df_c = df_c.loc[tech]
    max_value = df_c.loc[2050]['emissions']
    year_range = range(start_year, start_year+date_span)
    value_list = []
    for year in year_range:
        if year <= 2050:
            value_list.append(df_c.loc[year]['emissions'])
        if year > 2050:
            value_list.append(max_value)

    return dict(zip(year_range, value_list))

def compare_emissions(df: pd.DataFrame, base_tech: str, comp_tech: str, start_year: int = 2020, date_span: int = 20):
    base_tech_dict = get_emissions_by_year(df, base_tech, start_year, date_span)
    comp_tech_dict = get_emissions_by_year(df, comp_tech, start_year, date_span)
    years = list(base_tech_dict.keys())
    df = pd.DataFrame(data={
        'year': years,
        'start_technology': base_tech,
        'end_technology': comp_tech,
        'start_tech_values': base_tech_dict.values(),
        'comp_tech_values': comp_tech_dict.values(),
    })
    df['abated_emissions'] = df['start_tech_values'] - df['comp_tech_values']
    return df

#### Mega Function for calculating tco per ton steel
def calculate_emissions(start_year, emission_type: str = 's1', output_type: str = 'full'):
    df_list = []
    for base_tech in SWITCH_DICT.keys():
        for switch_tech in SWITCH_DICT[base_tech]:
            if switch_tech in ['Close plant']:
                pass
            else:
                if emission_type == 's1':
                    df = compare_emissions(s1_emissions, base_tech, switch_tech, start_year)
                elif emission_type == 's23':
                    df = compare_emissions(s23_emissions, base_tech, switch_tech, start_year)
                df_list.append(df)
    full_df = pd.concat(df_list)
    full_summary_df = full_df[['start_technology', 'end_technology', 'abated_emissions']].groupby(by=['start_technology', 'end_technology']).sum()
    if output_type == 'summary':
        return full_summary_df
    if output_type == 'full':
        return full_df

variable_costs = read_pickle_folder(PKL_FOLDER, 'calculated_variable_costs', 'df')
variable_costs = variable_costs.reorder_levels(['technology', 'year'])
variable_costs.rename(columns={'cost':'value'}, inplace=True)

opex_values_dict = read_pickle_folder(PKL_FOLDER, 'capex_dict', 'df')['other_opex']

capex = read_pickle_folder(PKL_FOLDER, 'capex_switching_df', 'df')
capex_c = capex.copy()
capex_c.reset_index(inplace=True)
capex_c.columns = [col.lower().replace(' ', '_') for col in capex_c.columns]
capex_c = capex_c.set_index(['year', 'start_technology']).sort_index()

s1_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s1_emissions', 'df')
s23_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s23_emissions', 'df')

print(calculate_tco(2050, DISCOUNT_RATE, 'summary'))

print(calculate_emissions(2050, 's23', 'summary'))
