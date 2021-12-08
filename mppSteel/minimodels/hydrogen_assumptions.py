"""Assumptions for the Hydrogen Minimodel"""
# For system level operations
from collections import namedtuple

import numpy as np

from mppSteel.model_config import (
    HYDROGEN_PRICE_START_YEAR,
    HYDROGEN_PRICE_END_YEAR,
    DISCOUNT_RATE,
)

HydrogenAssumptions = namedtuple(
    "Hydrogen_Assumptions", ["metric", "unit", "year", "value"]
)

VRE_PRICE_FAVORABLE_START = HydrogenAssumptions(
    metric="vre_price_favorable_start",
    unit="USD/MWh",
    year=HYDROGEN_PRICE_START_YEAR,
    value=22,
)

VRE_PRICE_FAVORABLE_END = HydrogenAssumptions(
    metric="vre_price_favorable_end",
    unit="USD/MWh",
    year=HYDROGEN_PRICE_END_YEAR,
    value=10,
)

VRE_PRICE_AVERAGE_START = HydrogenAssumptions(
    metric="vre_price_average_start",
    unit="USD/MWh",
    year=HYDROGEN_PRICE_START_YEAR,
    value=39,
)

VRE_PRICE_AVERAGE_END = HydrogenAssumptions(
    metric="vre_price_average_end",
    unit="USD/MWh",
    year=HYDROGEN_PRICE_END_YEAR,
    value=17,
)

ELECTROLYZER_CAPEX_START = HydrogenAssumptions(
    metric="electrolyzer_capex_start",
    unit="USD/kW",
    year=HYDROGEN_PRICE_START_YEAR,
    value=1400,
)

ELECTROLYZER_CAPEX_END = HydrogenAssumptions(
    metric="electrolyzer_capex_end",
    unit="USD/kW",
    year=HYDROGEN_PRICE_END_YEAR,
    value=200,
)

ELECTROLYZER_CAPACITY = HydrogenAssumptions(
    metric="electrolyzer_capacity", unit="MW", year="constant", value=20
)

ELECTROLYZER_LIFETIME = HydrogenAssumptions(
    metric="electrolyzer_lifetime", unit="years", year="constant", value=30
)

STACK_LIFETIME_START = HydrogenAssumptions(
    metric="stack_lifetime_start",
    unit="hours",
    year=HYDROGEN_PRICE_START_YEAR,
    value=np.average([50000, 80000]),
)

STACK_LIFETIME_END = HydrogenAssumptions(
    metric="stack_lifetime_end",
    unit="hours",
    year=HYDROGEN_PRICE_END_YEAR,
    value=np.average([100000, 120000]),
)

STACK_CAPEX_START = HydrogenAssumptions(
    metric="stack_capex_start", unit="USD/kW", year=HYDROGEN_PRICE_START_YEAR, value=400
)

STACK_CAPEX_END = HydrogenAssumptions(
    metric="stack_capex_end", unit="USD/kW", year=HYDROGEN_PRICE_END_YEAR, value=100
)

ENERGY_CONSUMPTION_START = HydrogenAssumptions(
    metric="energy_consumption_start",
    unit="MWh / t H2",
    year=HYDROGEN_PRICE_START_YEAR,
    value=54,
)

ENERGY_CONSUMPTION_END = HydrogenAssumptions(
    metric="energy_consumption_end",
    unit="MWh / t H2",
    year=HYDROGEN_PRICE_START_YEAR,
    value=45,
)

CAPACITY_UTILIZATION_FACTOR = HydrogenAssumptions(
    metric="capacity_utilization_factor", unit="percentage", year="constant", value=0.33
)

LEVELIZED_H2_STORAGE_COST_FAVORABLE = HydrogenAssumptions(
    metric="levelized_h2_storage_cost_favorable",
    unit="USD / kg",
    year="constant",
    value=0.12,
)

LEVELIZED_H2_STORAGE_COST_AVERAGE = HydrogenAssumptions(
    metric="levelized_h2_storage_cost_average",
    unit="USD / kg",
    year="constant",
    value=0.35,
)

FIXED_OPEX = HydrogenAssumptions(
    metric="fixed_opex", unit="percentage", year="constant", value=0.03
)

HYDROGEN_LHV = HydrogenAssumptions(
    metric="hydrogen_lhv", unit="GJ / t H2", year="constant", value=120
)

CAPITAL_RECOVERY_FACTOR = (
    DISCOUNT_RATE * (1 + DISCOUNT_RATE) ** ELECTROLYZER_LIFETIME.value
) / (((1 + DISCOUNT_RATE) ** ELECTROLYZER_LIFETIME.value) - 1)
