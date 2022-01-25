import pandas as pd
import numpy as np
import numpy_financial as npf

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