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
    # logger.info('--- Calculating present values')
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
    # logger.info('-- Calculating 20-year discouted opex')
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

def get_capital_schedules(
    capex_df: pd.DataFrame, start_tech: str,
    end_tech: str, switch_year: int,
    interest_rate: float,
    ):
    # logger.info(f'-- Calculating capital schedule for {start_tech} to {end_tech}')
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

def compare_capex(
    base_tech: str, switch_tech: str, interest_rate: float, 
    start_year: int = 2020, date_span: int = 20):

    # logger.info(f'- Comparing capex values for {base_tech} and {switch_tech}')

    other_opex_values = discounted_opex(opex_values_dict, switch_tech, interest_rate, start_year)
    variable_opex_values = discounted_opex(variable_costs, switch_tech, interest_rate, start_year)
    annual_capex_value = get_capital_schedules(capex_c, base_tech, switch_tech, start_year, interest_rate)['interest_payments']
    annual_capex_array = np.full(20, annual_capex_value) * -1
    discounted_annual_capex_array = calculate_present_values(annual_capex_array, interest_rate)
    years = list(range(start_year, start_year+date_span))
    df = pd.DataFrame(data={
        'start_technology': base_tech,
        'end_technology': switch_tech,
        'start_year': start_year,
        'years': years,
        'other_opex': other_opex_values,
        'variable_opex': variable_opex_values,
        'annual_capex': discounted_annual_capex_array})
    df['tco'] = df['other_opex'] + df['variable_opex'] + df['annual_capex']
    return df

def calculate_tco(
    interest_rate: float, 
    year_end: int = 2050, 
    output_type: str = 'full', 
    serialize_only: bool = False
    ):

    logger.info(f'- Calculating TCO tables for all technologies from 2020 up to {year_end}')

    df_list = []
    year_range = range(2020, year_end+1)
    for year in year_range:
        logger.info(f'Calculating technology TCO for {year}')
        for base_tech in SWITCH_DICT.keys():
            for switch_tech in SWITCH_DICT[base_tech]:
                if switch_tech == 'Close plant':
                    pass
                else:
                    df = compare_capex(base_tech, switch_tech, interest_rate, year)
                    df_list.append(df)
    full_df = pd.concat(df_list)
    full_summary_df = full_df[
        ['start_technology', 'end_technology', 'start_year',
        'other_opex', 'variable_opex', 'annual_capex', 'tco']
        ].groupby(by=['start_year', 'start_technology', 'end_technology']).sum()
    
    if serialize_only:
        serialize_df(full_summary_df, PKL_FOLDER, 'tco_switching_df_summary')
        serialize_df(full_df, PKL_FOLDER, 'tco_switching_df_full')
        return

    if output_type == 'summary':
        return full_summary_df
    return full_df

def get_emissions_by_year(
    df: pd.DataFrame, tech: str, start_year: int = 2020, date_span: int = 20):

    # logger.info(f'--- Getting emissions for {tech} for each year across the relevant range, starting at {start_year}')

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

def compare_emissions(
    df: pd.DataFrame, base_tech: str, comp_tech: str,
    emission_type: str, start_year: int = 2020, date_span: int = 20):

    # logger.info(f'--- Comparing emissions for {base_tech} and {comp_tech}')

    base_tech_dict = get_emissions_by_year(df, base_tech, start_year, date_span)
    comp_tech_dict = get_emissions_by_year(df, comp_tech, start_year, date_span)
    years = list(base_tech_dict.keys())
    df = pd.DataFrame(data={
        'start_year': start_year,
        'year': years,
        'start_technology': base_tech,
        'end_technology': comp_tech,
        'start_tech_values': base_tech_dict.values(),
        'comp_tech_values': comp_tech_dict.values(),
    })
    df[f'abated_{emission_type}_emissions'] = df['start_tech_values'] - df['comp_tech_values']
    return df

def calculate_emissions(
    year_end: int = 2050, output_type: str = 'full', serialize_only: bool = False):

    logger.info(f'Calculating emissions for all technologies from 2020 up to {year_end}')

    df_list = []
    df_base_cols = ['start_year', 'year', 'start_technology', 'end_technology',
        'start_tech_values', 'comp_tech_values']
    year_range = range(2020, year_end+1)
    for year in year_range:
        logger.info(f'Calculating technology emissions for {year}')
        for base_tech in SWITCH_DICT.keys():
            for switch_tech in SWITCH_DICT[base_tech]:
                if switch_tech in ['Close plant']:
                    pass
                else:
                    df_cols = {}
                    emission_dict = {'s1': s1_emissions, 's2': s2_emissions, 's3': s3_emissions}
                    for item in emission_dict.items():
                        emission_type = item[0]
                        emission_df = item[1]
                        emission_type_col = f'abated_{emission_type}_emissions'
                        df = compare_emissions(emission_df, base_tech, switch_tech, emission_type, year)
                        df_cols[emission_type] = df[emission_type_col]
                    df_base = df[df_base_cols]
                    for key in df_cols.keys():
                        df_base[f'abated_{key}_emissions'] = df_cols[key]
                    df_list.append(df_base)
    full_df = pd.concat(df_list)
    col_list = [
        'start_year', 'start_technology', 'end_technology', 'abated_s1_emissions',
        'abated_s2_emissions', 'abated_s3_emissions'
        ]
    full_summary_df = full_df[col_list].groupby(by=['start_year', 'start_technology', 'end_technology']).sum()

    if serialize_only:
        serialize_df(full_summary_df, PKL_FOLDER, 'emissions_switching_df_summary')
        serialize_df(full_df, PKL_FOLDER, 'emissions_switching_df_full')
        return

    if output_type == 'summary':
        return full_summary_df
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
s2_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s2_emissions', 'df')
s3_emissions = read_pickle_folder(PKL_FOLDER, 'calculated_s3_emissions', 'df')
