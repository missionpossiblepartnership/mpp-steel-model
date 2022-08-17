"""TCO Calculations used to derive the Total Cost of Ownership"""

import pandas as pd
import numpy_financial as npf
from tqdm import tqdm

from mppsteel.config.model_config import (
    DISCOUNT_RATE,
    INVESTMENT_CYCLE_DURATION_YEARS,
    MEGATON_TO_TON,
    MODEL_YEAR_END,
)
from mppsteel.config.reference_lists import SWITCH_DICT
from mppsteel.utility.log_utility import get_logger
from mppsteel.model_tests.df_tests import test_negative_df_values

logger = get_logger(__name__)


def calculate_green_premium(
    variable_cost_ref: pd.DataFrame,
    capacity_ref: dict,
    green_premium_timeseries: pd.DataFrame,
    country_code: str,
    plant_name: str,
    year: int,
    usd_eur_rate: float,
) -> dict:
    """Calculates a green premium amout based on the product of the green premium capacity and the green premium timeseries value.

    Args:
        variable_cost_ref (pd.DataFrame): DataFrame containing the variable costs data split by technology and region.
        steel_plant_df (pd.DataFrame): DataFrame containing the list of steel plants.
        green_premium_timeseries (pd.DataFrame): The green premium timeseries with the subsidy amounts on a yearly basis.
        country_code (str): The country code that the plant is based in.
        plant_name (str): The name of the plant you want to calculate the green premium value for.
        year (int): The year to get the green premium timeseries value for.
        eur_to_usd_rate (float): A conversion rate from euros to usd.

    Returns:
        dict: A dictionary of technology key values and green premium values as an array.
    """

    def green_premium_calc(loop_year: int):
        variable_tech_costs = variable_cost_ref.loc[country_code, loop_year]  # ts
        plant_capacity = capacity_ref[plant_name]  # float
        green_premium = green_premium_timeseries.loc[loop_year]["value"]  # float
        return (variable_tech_costs * green_premium * usd_eur_rate) / (
            plant_capacity * MEGATON_TO_TON
        )  # ts

    year_range = range(year, year + INVESTMENT_CYCLE_DURATION_YEARS + 1)
    year_range = [
        year if (year <= MODEL_YEAR_END) else min(MODEL_YEAR_END, year)
        for year in year_range
    ]
    df_list = [green_premium_calc(loop_year) for loop_year in year_range]
    df_combined = pd.concat(df_list)
    technologies = df_combined.index.unique()
    return {
        technology: npf.npv(DISCOUNT_RATE, df_combined.loc[technology]["cost"].values)
        for technology in technologies
    }


def calculate_capex(
    capex_df: pd.DataFrame, start_year: int, base_tech: str
) -> pd.DataFrame:
    """Creates a capex DataFrame for a given base tech along with capex values for potential switches.

    Args:
        capex_df (pd.DataFrame): A capex DataFrame containing all switch capex values.
        start_year (int): The year you want to start generating capex values for.
        base_tech (str): The technology you are starting from.

    Returns:
        pd.DataFrame: A DataFrame with the Capex values with a multiindex as year and start_technology.
    """
    df_c = capex_df.loc[base_tech, start_year].copy()
    df_c = df_c[df_c["end_technology"].isin(SWITCH_DICT[base_tech])]
    return df_c.loc[base_tech].set_index("end_technology")


def get_discounted_opex_values(
    country_code: tuple,
    year_start: int,
    opex_cost_ref: dict,
    year_interval: int,
    int_rate: float,
) -> dict:
    """Calculates discounted opex reference DataFrame from given inputs.

    Args:
        country_code (tuple): The country code of the steel plant you want to get discounted opex values for.
        year_start (int): The year in which the model starts.
        opex_cost_ref (dict): A dict of opex values to be used to calculate total opex costs.
        year_interval (int): The year interval for the discounting window.
        int_rate (float): The interest rate that you want to discount values according to.

    Returns:
        dict: A dictionary of technology key values and opex values as an array.
    """
    year_range = range(year_start, year_start + year_interval + 1)
    loop_year_range = [
        year if (year <= MODEL_YEAR_END) else min(MODEL_YEAR_END, year)
        for year in year_range
    ]
    df_list = [opex_cost_ref[(year, country_code)] for year in loop_year_range]
    df_combined = pd.concat(df_list)
    test_negative_df_values(df_combined)
    technologies = df_combined.index.unique()
    return {
        technology: npf.npv(int_rate, df_combined.loc[technology]["opex"].values)
        for technology in technologies
    }
