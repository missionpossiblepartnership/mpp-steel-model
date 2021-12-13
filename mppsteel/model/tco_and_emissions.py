"""Script to generate Switching tables for TCO and emission abatement"""

import pandas as pd
import numpy as np
import numpy_financial as npf

# For logger and units dict
from mppSteel.utility.utils import get_logger, read_pickle_folder, serialize_df

from mppSteel.model_config import (
    PKL_FOLDER,
    DISCOUNT_RATE,
    SWITCH_DICT,
    INVESTMENT_CYCLE_LENGTH,
)

# Create logger
logger = get_logger("TCO & Emissions")


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
    start_year: int = 2020,
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
        "future_value": round(fv_calc, rounding),
        "interest_payments": round(pmt_calc, rounding),
        "total_interest": round(ipmt.sum(), rounding),
        "principal_schedule": ppmt.round(rounding),
        "interest_schedule": ipmt.round(rounding),
    }


def compare_capex(
    base_tech: str,
    switch_tech: str,
    interest_rate: float,
    start_year: int = 2020,
    date_span: int = 20,
) -> pd.DataFrame:
    """[summary]

    Args:
        base_tech (str): The technology to start from.
        switch_tech (str): The technology to be switched to.
        interest_rate (float): The rate of interest to be applied to the capex.
        start_year (int, optional): The start date of the opex and capex calculations. Defaults to 2020.
        date_span (int, optional): The years that comprise the investment. Defaults to 20.

    Returns:
        pd.DataFrame: A DataFrame that stacks the opex and capex and tco values togather.
    """

    # logger.info(f'- Comparing capex values for {base_tech} and {switch_tech}')
    # Added by Luis:
    opex_values_dict = read_pickle_folder(PKL_FOLDER, "capex_dict", "df")["other_opex"]
    variable_costs = read_pickle_folder(PKL_FOLDER, "calculated_variable_costs", "df")
    variable_costs = variable_costs.reorder_levels(["technology", "year"])
    variable_costs.rename(columns={"cost": "value"}, inplace=True)
    capex = read_pickle_folder(PKL_FOLDER, "capex_switching_df", "df")
    capex_c = capex.copy()
    capex_c.reset_index(inplace=True)
    capex_c.columns = [col.lower().replace(" ", "_") for col in capex_c.columns]
    capex_c = capex_c.set_index(["year", "start_technology"]).sort_index()
    other_opex_values = discounted_opex(
        opex_values_dict, switch_tech, interest_rate, start_year
    )
    variable_opex_values = discounted_opex(
        variable_costs, switch_tech, interest_rate, start_year
    )
    annual_capex_value = get_capital_schedules(
        capex_c, base_tech, switch_tech, start_year, interest_rate
    )["interest_payments"]
    annual_capex_array = np.full(20, annual_capex_value) * -1
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
            "other_opex": other_opex_values,
            "variable_opex": variable_opex_values,
            "annual_capex": discounted_annual_capex_array,
        }
    )
    df["tco"] = df["other_opex"] + df["variable_opex"] + df["annual_capex"]
    return df


def calculate_tco(
    year_end: int = 2050, output_type: str = "full", serialize_only: bool = False
) -> pd.DataFrame:
    """Calculates the complete array of technology switch matches to years.

    Args:
        interest_rate (float): The rate of interest to be applied to the capex.
        year_end (int, optional): The year that the table should stop calculating. Defaults to 2050.
        output_type (str, optional): Determines whether to return the full DataFrame or a summary. Defaults to 'full'.
        serialize_only (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with the complete TCO iterations of years and technology switches available.
    """

    logger.info(
        f"- Calculating TCO tables for all technologies from 2020 up to {year_end}"
    )

    df_list = []
    year_range = range(2020, year_end + 1)
    for year in year_range:
        logger.info(f"Calculating technology TCO for {year}")
        for base_tech in SWITCH_DICT.keys():
            for switch_tech in SWITCH_DICT[base_tech]:
                if switch_tech == "Close plant":
                    pass
                else:
                    df = compare_capex(
                        base_tech,
                        switch_tech,
                        DISCOUNT_RATE,
                        year,
                        INVESTMENT_CYCLE_LENGTH,
                    )
                    df_list.append(df)
    full_df = pd.concat(df_list)
    full_summary_df = (
        full_df[
            [
                "start_technology",
                "end_technology",
                "start_year",
                "other_opex",
                "variable_opex",
                "annual_capex",
                "tco",
            ]
        ]
        .groupby(by=["start_year", "start_technology", "end_technology"])
        .sum()
    )

    if serialize_only:
        serialize_df(full_summary_df, PKL_FOLDER, "tco_switching_df_summary")
        serialize_df(full_df, PKL_FOLDER, "tco_switching_df_full")
        return

    if output_type == "summary":
        return full_summary_df
    return full_df


def get_emissions_by_year(
    df: pd.DataFrame, tech: str, start_year: int = 2020, date_span: int = 20
) -> dict:
    """Generates a dictionary of years as keys, and emissions as values.

    Args:
        df (pd.DataFrame): A DataFrame containing emissions.
        tech (str): The technology to subset the DataFrame.
        start_year (int, optional): The start year for the technology. Defaults to 2020.
        date_span (int, optional): The years that comprise the investment. Defaults to 20.

    Returns:
        dict: A dictionary with the with the years and emissions value for the technology.
    """
    # logger.info(f'--- Getting emissions for {tech} for each year across the relevant range, starting at {start_year}')

    df_c = df.copy()
    df_c = df_c.reorder_levels(["technology", "year"])
    df_c = df_c.loc[tech]
    max_value = df_c.loc[2050]["emissions"]
    year_range = range(start_year, start_year + date_span)

    value_list = []
    for year in year_range:
        if year <= 2050:
            value_list.append(df_c.loc[year]["emissions"])

        if year > 2050:
            value_list.append(max_value)

    return dict(zip(year_range, value_list))


def compare_emissions(
    df: pd.DataFrame,
    base_tech: str,
    comp_tech: str,
    emission_type: str,
    start_year: int = 2020,
    date_span: int = 20,
):

    # logger.info(f'--- Comparing emissions for {base_tech} and {comp_tech}')

    base_tech_dict = get_emissions_by_year(df, base_tech, start_year, date_span)
    comp_tech_dict = get_emissions_by_year(df, comp_tech, start_year, date_span)
    years = list(base_tech_dict.keys())
    df = pd.DataFrame(
        data={
            "start_year": start_year,
            "year": years,
            "start_technology": base_tech,
            "end_technology": comp_tech,
            "start_tech_values": base_tech_dict.values(),
            "comp_tech_values": comp_tech_dict.values(),
        }
    )
    df[f"abated_{emission_type}_emissions"] = (
        df["start_tech_values"] - df["comp_tech_values"]
    )
    return df


def calculate_emissions(
    year_end: int = 2050, output_type: str = "full", serialize_only: bool = False
) -> pd.DataFrame:
    """Calculates the complete array of technology switch matches to years.

    Args:
        year_end (int, optional): The year that the table should stop calculating. Defaults to 2050.
        output_type (str, optional): Determines whether to return the full DataFrame or a summary. Defaults to 'full'.
        serialize_only (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with the complete iterations of years and technology switches available.
    """

    logger.info(
        f"Calculating emissions for all technologies from 2020 up to {year_end}"
    )

    df_list = []
    df_base_cols = [
        "start_year",
        "year",
        "start_technology",
        "end_technology",
        "start_tech_values",
        "comp_tech_values",
    ]
    s1_emissions = read_pickle_folder(PKL_FOLDER, "calculated_s1_emissions", "df")
    s2_emissions = read_pickle_folder(PKL_FOLDER, "calculated_s2_emissions", "df")
    s3_emissions = read_pickle_folder(PKL_FOLDER, "calculated_s3_emissions", "df")
    year_range = range(2020, year_end + 1)
    for year in year_range:
        logger.info(f"Calculating technology emissions for {year}")
        for base_tech in SWITCH_DICT.keys():
            for switch_tech in SWITCH_DICT[base_tech]:
                if switch_tech in ["Close plant"]:
                    pass
                else:
                    df_cols = {}
                    emission_dict = {
                        "s1": s1_emissions,
                        "s2": s2_emissions,
                        "s3": s3_emissions,
                    }
                    for item in emission_dict.items():
                        emission_type = item[0]
                        emission_df = item[1]
                        emission_type_col = f"abated_{emission_type}_emissions"
                        df = compare_emissions(
                            emission_df,
                            base_tech,
                            switch_tech,
                            emission_type,
                            year,
                            INVESTMENT_CYCLE_LENGTH,
                        )
                        df_cols[emission_type] = df[emission_type_col]
                    df_base = df[df_base_cols]
                    for key in df_cols.keys():
                        df_base[f"abated_{key}_emissions"] = df_cols[key]
                    df_list.append(df_base)
    full_df = pd.concat(df_list)
    col_list = [
        "start_year",
        "start_technology",
        "end_technology",
        "abated_s1_emissions",
        "abated_s2_emissions",
        "abated_s3_emissions",
    ]
    full_summary_df = (
        full_df[col_list]
        .groupby(by=["start_year", "start_technology", "end_technology"])
        .sum()
    )

    if serialize_only:
        serialize_df(full_summary_df, PKL_FOLDER, "emissions_switching_df_summary")
        serialize_df(full_df, PKL_FOLDER, "emissions_switching_df_full")
        return

    if output_type == "summary":
        return full_summary_df
    return full_df
