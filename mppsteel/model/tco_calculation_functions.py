"""TCO Calculations used to derive the Total Cost of Ownership"""

from functools import lru_cache

import pandas as pd
import numpy_financial as npf

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

from mppsteel.model.emissions_reference_tables import get_s2_emissions

from mppsteel.utility.utils import enumerate_iterable

from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    DISCOUNT_RATE,
)
from mppsteel.config.reference_lists import SWITCH_DICT
from mppsteel.utility.log_utility import get_logger

logger = get_logger("TCO Calculation Functions")


def carbon_tax_estimate(
    s1_emissions_value: float, s2_emissions_value: float, carbon_tax_value: float
) -> float:
    """Creates a carbon tax based on the scope 1 & 2 emissivity as a standardised unit and a technology and a carbon tax value per ton of steel.

    Args:
        s1_emissions_value (float): Scope 1 emissivity as a standarised unit.
        s2_emissions_value (float): Scope 2 emissivity as a standarised unit.
        carbon_tax_value (float): A carbon tax value per standardised unit.

    Returns:
        float: A  carbon tax estimate based on S1 & S2 emissions and a carbon tax per unit value.
    """
    return (s1_emissions_value + s2_emissions_value) * carbon_tax_value


@lru_cache(maxsize=200000)
def green_premium_capacity_calculation(
    variable_tech_cost: float,
    plant_capacity: float,
    technology: str,
    eur_to_usd_rate: float,
) -> float:
    """Calculates a green premium capacity amount to be multiplied by the green premium value. 

    Args:
        variable_tech_cost (float): The variable cost of a technology.
        plant_capacity (float): The plant_capacity of a plant.
        technology (str): The technology that the green premium is being calculated for.
        eur_to_usd_rate (float): A conversion rate from euros to usd.

    Returns:
        float: A green premium capacity value based on the inputted values.
    """
    variable_cost_value = variable_tech_cost * plant_capacity
    return (
        (variable_cost_value / plant_capacity) / eur_to_usd_rate
    )


def calculate_green_premium(
    variable_costs: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    green_premium_timeseries: pd.DataFrame,
    country_code: str,
    plant_name: str,
    technology_2020: str,
    year: int,
    eur_to_usd_rate: float,
) -> float:
    """Calculates a green premium amout based on the product of the green premium capacity and the green premium timeseries value.

    Args:
        variable_costs (pd.DataFrame): DataFrame containing the variable costs data split by technology and region.
        steel_plant_df (pd.DataFrame): DataFrame containing the list of steel plants.
        green_premium_timeseries (pd.DataFrame): The green premium timeseries with the subsidy amounts on a yearly basis.
        country_code (str): The country code that the plant is based in.
        plant_name (str): The name of the plant you want to calculate the green premium value for.
        technology (str): The technology that the green premium is being calculated for.
        year (int): The year to get the green premium timeseries value for.
        eur_to_usd_rate (float): A conversion rate from euros to usd.

    Returns:
        float: A green premium value product.
    """

    variable_tech_cost = variable_costs.loc[country_code, year, technology_2020].values[
        0
    ]
    steel_plant_df_c = steel_plant_df.loc[
        steel_plant_df["plant_name"] == plant_name
    ].copy()
    plant_capacity = steel_plant_df_c["plant_capacity"].values[0]
    green_premium = green_premium_timeseries.loc[
        green_premium_timeseries["year"] == year
    ]["value"]
    steel_making_cost = green_premium_capacity_calculation(
        variable_tech_cost,
        plant_capacity,
        technology_2020,
        eur_to_usd_rate,
    )
    return steel_making_cost * green_premium


def get_opex_costs(
    country_code: str,
    year: int,
    variable_costs_df: pd.DataFrame,
    opex_df: pd.DataFrame,
    s1_emissions_ref: dict,
    carbon_tax_timeseries: pd.DataFrame,
    s2_emissions_value: float,
) -> pd.DataFrame:
    """Returns the combined Opex costs for each technology in each region.

    Args:
        country_code (str): The country code of the plant you want to get opex costs for,
        year (int): The year you want to request opex values for.
        variable_costs_df (pd.DataFrame): DataFrame containing the variable costs data split by technology and region.
        opex_df (pd.DataFrame): The Fixed Opex DataFrame containing opex costs split by technology.
        s1_emissions_ref (dict): The DataFrame for scope 1 emissions.
        carbon_tax_timeseries (pd.DataFrame): The carbon tax timeseries with the carbon tax amounts on a yearly basis.
        s2_emissions_value (float): The value for Scope 2 emissions.

    Returns:
        pd.DataFrame: A DataFrame containing the opex costs for each technology for a given year.
    """

    variable_costs = variable_costs_df.loc[country_code, year]
    opex_costs = opex_df.loc[year]
    opex_costs.drop(["Charcoal mini furnace", "Close plant"], inplace=True)
    carbon_tax_value = carbon_tax_timeseries.set_index("year").loc[year]["value"]
    s1_emissions_value = s1_emissions_ref.loc[year]
    carbon_tax_result = carbon_tax_estimate(
        s1_emissions_value, s2_emissions_value, carbon_tax_value
    )
    carbon_tax_result.rename(mapper={"emissions": "value"}, axis=1, inplace=True)
    total_opex = variable_costs + opex_costs + carbon_tax_result
    return total_opex.rename(mapper={"value": "opex"}, axis=1)


def capex_getter(
    capex_df: pd.DataFrame, switch_dict: dict, year: int, start_tech: str, end_tech: str
) -> float:
    """Returns the capex value for a given year and valid techology from a capex switching dictionary.

    Args:
        capex_df (pd.DataFrame): A capex switching dictionary with relevant brownfield and greenfield values.
        switch_dict (dict): A dictionary mapping each technology to a list of possible switches.
        year (int): The year that you want to get the capex switch value for.
        start_tech (str): The start technology.
        end_tech (str): The end technology.

    Returns:
        float: The capex value.
    """
    year = min(MODEL_YEAR_END, year)
    if end_tech in switch_dict[start_tech]:
        return capex_df.loc[year, start_tech, end_tech][0]
    raise ValueError(f'Invalid technology switch from {start_tech} to {end_tech}')


def calculate_capex(
    capex_df: pd.DataFrame, start_year: int, base_tech: str) -> pd.DataFrame:
    """Creates a capex DataFrame for a given base tech along with capex values for potential switches. 

    Args:
        capex_df (pd.DataFrame): A capex DataFrame containing all switch capex values.
        start_year (int): The year you want to start generating capex values for.
        base_tech (str): The technology you are starting from.

    Returns:
        pd.DataFrame: A DataFrame with the Capex values with a multiindex as year and start_technology.
    """
    df_c = capex_df.set_index(['start_technology', 'year']).loc[base_tech, start_year].copy()
    df_c = df_c[df_c['end_technology'].isin(SWITCH_DICT[base_tech])]
    return df_c.loc[base_tech].groupby("end_technology").sum()


def get_discounted_opex_values(
    country_code: tuple,
    year_start: int,
    carbon_tax_df: pd.DataFrame,
    variable_cost_summary: pd.DataFrame,
    other_opex_df: pd.DataFrame,
    s1_emissions_df: pd.DataFrame,
    s2_emissions_df: pd.DataFrame,
    year_interval: int,
    int_rate: float,
    base_tech: str,
) -> pd.DataFrame:
    """Calculates discounted opex reference DataFrame from given inputs.

    Args:
        country_code (tuple): The country code of the steel plant you want to get discounted opex values for.
        year_start (int): The year in which the model starts.
        carbon_tax_df (pd.DataFrame): The carbon tax timeseries with the carbon tax amounts on a yearly basis.
        variable_costs_df (pd.DataFrame): DataFrame containing the variable costs data split by technology and region.
        power_df (pd.DataFrame, optional): The shared MPP Power assumptions model. Defaults to None.
        hydrogen_df (pd.DataFrame, optional): The shared MPP Hydrogen assumptions model. Defaults to None.
        other_opex_df (pd.DataFrame): A fixed opex DataFrame to be used to calculate total opex costs.
        s1_emissions_df (pd.DataFrame): An S1 emissions DataFrame to be used to calculate opex costs. 
        year_interval (int): The year interval for the discounting window.
        int_rate (float): The interest rate that you want to discount values according to.
        base_tech (str): The technology you are starting from.

    Returns:
        pd.DataFrame: A DataFrame with discounted opex values.
    """

    year_range = range(year_start, year_start + year_interval + 1)
    df_list = []
    for year in year_range:
        year = min(MODEL_YEAR_END, year)
        s2_value = s2_emissions_df.loc[year, country_code, base_tech]
        df = get_opex_costs(
            country_code,
            year,
            variable_cost_summary,
            other_opex_df,
            s1_emissions_df,
            carbon_tax_df,
            s2_value,
        )
        df["year"] = year
        df_list.append(df)
    df_combined = pd.concat(df_list)
    new_df = pd.DataFrame({'technology': SWITCH_DICT[base_tech]})
    new_df['discounted_opex'] = new_df['technology'].apply(
        lambda technology: npf.npv(int_rate, df_combined.loc[technology]["opex"].values))
    return new_df.set_index(['technology'])


def tco_calc(
    country_code,
    start_year: int,
    base_tech: str,
    carbon_tax_df: pd.DataFrame,
    variable_cost_summary: pd.DataFrame,
    other_opex_df: pd.DataFrame,
    s1_emissions_df: pd.DataFrame,
    s2_emissions_df: pd.DataFrame,
    capex_df: pd.DataFrame,
    investment_cycle: int,
) -> pd.DataFrame:
    """Creates a DataFrame with the full tco calculations for each technology. 

    Args:
        country_code (tuple): The country code of the steel plant you want to get discounted opex values for.
        year_start (int): The year in which the model starts.
        base_tech (str): The technology you are starting from.
        carbon_tax_df (pd.DataFrame): The carbon tax timeseries with the carbon tax amounts on a yearly basis.
        variable_cost_summary (pd.DataFrame): DataFrame containing the variable costs data split by technology and region.
        other_opex_df (pd.DataFrame): A fixed opex DataFrame to be used to calculate total opex costs.
        s1_emissions_df (pd.DataFrame): An S1 emissions DataFrame to be used to calculate opex costs.
        s2_emissions_df (pd.DataFrame): An S2 emissions DataFrame to be used to calculate opex costs.
        capex_df (pd.DataFrame): A capex DataFrame containing all switch capex values.
        investment_cycle (int): The year interval for the discounting window.

    Returns:
        pd.DataFrame: A DataFrame with full tco calculations and component calculation columns.
    """
    other_opex_df = other_opex_df.swaplevel()
    variable_cost_summary.rename(mapper={"cost": "value"}, axis=1, inplace=True)
    s2_emissions_df_c = s2_emissions_df.set_index(['year', 'country_code', 'technology']).copy()
    opex_values = get_discounted_opex_values(
        country_code,
        start_year,
        carbon_tax_df,
        variable_cost_summary,
        other_opex_df,
        s1_emissions_df,
        s2_emissions_df_c,
        year_interval=investment_cycle,
        int_rate=DISCOUNT_RATE,
        base_tech=base_tech
    )
    capex_values = calculate_capex(capex_df, start_year, base_tech)
    opex_values.index.rename("end_technology", inplace=True)
    capex_opex_values = capex_values.join(opex_values, on="end_technology")
    capex_opex_values["year"] = start_year
    capex_opex_values["country_code"] = country_code
    capex_opex_values["start_technology"] = base_tech
    column_order = [
        "country_code",
        "year",
        "start_technology",
        "end_technology",
        "capex_value",
        "discounted_opex",
    ]
    return capex_opex_values.reset_index()[column_order]