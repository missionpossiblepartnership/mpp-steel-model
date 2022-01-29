"""Calculation Functions used to derive various forms of Cost of Steelmaking."""

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

logger = get_logger("Cost of Steelmaking")

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

def standard_cost_of_steelmaking(results_df: pd.DataFrame):
    
    pass


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
