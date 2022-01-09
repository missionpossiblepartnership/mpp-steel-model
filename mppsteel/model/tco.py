"""Main solving script for deciding investment decisions."""

import pandas as pd
import numpy as np
import numpy_financial as npf

from mppsteel.utility.utils import (
    read_pickle_folder, get_logger
)

from mppsteel.model_config import (
    MODEL_YEAR_START, PKL_DATA_INTERMEDIATE, 
    DISCOUNT_RATE, INVESTMENT_CYCLE_LENGTH,
    STEEL_PLANT_LIFETIME,
    TCO_RANK_1_SCALER, TCO_RANK_2_SCALER,
    ABATEMENT_RANK_2, ABATEMENT_RANK_3,
    EUR_USD_CONVERSION
)

from mppsteel.utility.reference_lists import (
    SWITCH_DICT
)

# Create logger
logger = get_logger("TCO")


def calculate_present_values(values: list, int_rate: float, rounding: int = 2) -> list:
    """Converts a list of values into a present values discounted to today using a discouont rate.

    Args:
        values (list): A list of values to convert to present values.
        int_rate (float): A discount rate to convert the list of values using a present_value function.
        rounding (int, optional): The number of decimal places to round the final list of values to. Defaults to 2.

    Returns:
        list: A list of future opex converted to present values.
    """
    # logger.info('--- Calculating present values')
    def present_value(value: float, factor: float):
        discount_factor = 1 / ((1 + int_rate) ** factor)
        return value * discount_factor

    zipped_values = zip(values, list(range(len(values))))
    present_values = [
        round(present_value(val, factor), rounding) for val, factor in zipped_values
    ]
    return present_values


def discounted_opex(
    opex_value_dict: dict,
    technology: str,
    interest_rate: float,
    start_year: int = MODEL_YEAR_START,
    date_span: int = 20,
) -> list:
    """Produces a list of Present values based on predicted opex numbers.

    Args:
        opex_value_dict (dict): The opex value dict containing values in a timeseries.
        technology (str): The technology that subsets the opex_value_dict
        interest_rate (float): The interest rate to be applied to produce the discounted opex values.
        start_year (int, optional): The start date of the opex. Defaults to 2020.
        date_span (int, optional): The investment length. Defaults to 20.

    Returns:
        list: A list of future opex converted to present values.
    """
    # logger.info('-- Calculating 20-year discouted opex')
    tech_values = opex_value_dict.loc[technology].copy()
    max_value = tech_values.loc[2050].value
    year_range = range(start_year, start_year + date_span)

    value_list = []
    for year in year_range:
        if year <= 2050:
            value_list.append(tech_values.loc[year].value)

        if year > 2050:
            value_list.append(max_value)

    return calculate_present_values(value_list, interest_rate)


def get_capital_schedules(
    capex_df: pd.DataFrame,
    start_tech: str,
    end_tech: str,
    switch_year: int,
    interest_rate: float,
) -> dict:
    """Gets the capex schedule of capital charges for a given capex amount.

    Args:
        capex_df (pd.DataFrame): A DataFrame containing all the capex amounts.
        start_tech (str): The technology that starts the process.
        end_tech (str): The technology that will be switched to.
        switch_year (int): The year the switch is planned to take place.
        interest_rate (float): The interest rate used to calculate interest payments on the capex loan.

    Returns:
        dict: A dictionary containing the capex schedule for the switching technology.
    """
    # logger.info(f'-- Calculating capital schedule for {start_tech} to {end_tech}')
    df_c = None
    if switch_year > 2050:
        df_c = capex_df.loc[2050, start_tech].copy()
    elif 2020 <= switch_year <= 2050:
        df_c = capex_df.loc[switch_year, start_tech].copy()

    raw_switch_value = df_c.loc[df_c["new_technology"] == end_tech]["value"].values[0]

    financial_summary = generate_capex_financial_summary(
        principal=raw_switch_value,
        interest_rate=interest_rate,
        years=20,
        downpayment=0,
        compounding_type="annual",
        rounding=2,
    )

    return financial_summary


def generate_capex_financial_summary(
    principal: float,
    interest_rate: float,
    years: int = 20,
    downpayment: float = None,
    compounding_type: str = "annual",
    rounding: int = 2,
) -> dict:
    """Generates a number of capex schedules based on inputs.

    Args:
        principal (float): The capex (loan) amount.
        interest_rate (float): The rate of interest to be applied to the loan.
        years (int, optional): The number of years the loan will be active for. Defaults to 20.
        downpayment (float, optional): Any amouunt paid down on the loan in the inital period. Defaults to None.
        compounding_type (str, optional): Whether the loan amount is to be compounded annually, semi-annually or monthly. Defaults to 'annual'.
        rounding (int, optional): The number of decimal places to round the final list of values to. Defaults to 2.

    Returns:
        dict: A dictionary containing the capex schedule for the switching technology
        (future_value, interest_payments, total_interest, principal_schedule, interest_schedule).
    """

    rate = interest_rate
    nper = years

    if compounding_type == "monthly":
        rate = interest_rate / 12
        nper = years * 12

    if compounding_type == "semi-annual":
        rate = interest_rate / 2
        nper = years * 2

    if downpayment:
        pmt = -1 * downpayment
    else:
        pmt = -(principal * rate)

    fv_calc = npf.fv(
        rate=rate,  # annual interest rate
        nper=nper,  # total payments (years)
        pmt=pmt,  # downpayment
        pv=principal,  # value borrowed today
        when="end",  # when the initial payment is made
    )

    pmt_calc = npf.pmt(rate=rate, nper=nper, pv=principal, when="end")

    ipmt = npf.ipmt(
        rate=rate, per=np.arange(nper) + 1, nper=nper, pv=principal, when="end"
    )

    ppmt = npf.ppmt(
        rate=rate, per=np.arange(nper) + 1, nper=nper, pv=principal, when="end"
    )

    return {
        "future_value": fv_calc.round(rounding),
        "interest_payments": pmt_calc.round(rounding),
        "total_interest": round(ipmt.sum(), rounding),
        "principal_schedule": ppmt.round(rounding),
        "interest_schedule": ipmt.round(rounding),
    }


def compare_capex(
    base_tech: str,
    switch_tech: str,
    interest_rate: float,
    start_year: int = MODEL_YEAR_START,
    date_span: int = INVESTMENT_CYCLE_LENGTH,
) -> pd.DataFrame:
    """[summary]

    Args:
        base_tech (str): The technology to start from.
        switch_tech (str): The technology to be switched to.
        interest_rate (float): The rate of interest to be applied to the capex.
        start_year (int, optional): The start date of the opex and capex calculations. Defaults to MODEL_YEAR_START.
        date_span (int, optional): The years that comprise the investment. Defaults to 20.

    Returns:
        pd.DataFrame: A DataFrame that stacks the opex and capex and tco values togather.
    """

    # logger.info(f'- Comparing capex values for {base_tech} and {switch_tech}')
    capex = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_switching_df", "df")
    capex_c = capex.copy()
    capex_c.reset_index(inplace=True)
    capex_c.columns = [col.lower().replace(" ", "_") for col in capex_c.columns]
    capex_c = capex_c.set_index(["year", "start_technology"]).sort_index()
    annual_capex_value = get_capital_schedules(
        capex_c, base_tech, switch_tech, start_year, interest_rate
    )["interest_payments"]
    annual_capex_array = np.full(date_span, annual_capex_value) * -1
    discounted_annual_capex_array = calculate_present_values(
        annual_capex_array, interest_rate
    )
    years = list(range(start_year, start_year + date_span))
    df = pd.DataFrame(
        data={
            "start_technology": base_tech,
            "end_technology": switch_tech,
            "start_year": start_year,
            "years": years,
            "annual_capex": discounted_annual_capex_array,
        }
    )
    return df

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
    include_carbon_tax: bool,
    include_green_premium: bool,
):
    # combined_row = str(plant.abundant_res) + str(plant.ccs_available) + str(plant.cheap_natural_gas)
    # variable_costs = variable_costs_df.loc[combined_row, year]
    variable_costs = variable_costs_df.loc[plant.region, year]
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


def create_emissions_dict():
    calculated_s1_emissions = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'calculated_s1_emissions', 'df')
    calculated_s2_emissions = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'calculated_s2_emissions', 'df')
    calculated_s3_emissions = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'calculated_s3_emissions', 'df')
    return {'s1': calculated_s1_emissions, 's2': calculated_s2_emissions, 's3': calculated_s3_emissions}


def get_discounted_opex_values(
    plant: tuple, year_start: int, carbon_tax_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    variable_cost_summary: pd.DataFrame,
    green_premium_timeseries: pd.DataFrame,
    other_opex_df: pd.DataFrame,
    include_carbon_tax: bool,
    include_green_premium: bool,
    year_interval: int, int_rate: float):

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
            include_carbon_tax=include_carbon_tax,
            include_green_premium=include_green_premium
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
    include_carbon_tax: bool, include_green_premium: bool,
    investment_cycle: int
    ):
    opex_values = get_discounted_opex_values(
        plant, start_year, carbon_tax_df, steel_plant_df, variable_cost_summary,
        green_premium_timeseries, other_opex_df,
        include_carbon_tax=include_carbon_tax, include_green_premium=include_green_premium,
        year_interval=investment_cycle, int_rate=DISCOUNT_RATE,
        )
    capex_values = calculate_capex(start_year).swaplevel().loc[plant_tech].groupby('end_technology').sum()
    return capex_values + opex_values / investment_cycle

def capex_values_for_levelised_steelmaking(capex_df: pd.DataFrame, int_rate: float, year: int, payments: int):
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
