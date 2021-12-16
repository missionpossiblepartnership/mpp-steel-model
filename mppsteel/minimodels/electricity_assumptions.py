"""Assumptions for the Hydrogen Minimodel"""
# For system level operations
from collections import namedtuple

from mppsteel.model_config import (
    ELECTRICITY_PRICE_START_YEAR,
    ELECTRICITY_PRICE_MID_YEAR,
    ELECTRICITY_PRICE_END_YEAR,
)

# Power Grid Assumptions & Functions
PowerGridTuple = namedtuple(
    "Power_Grid_Assumptions", ["metric", "unit", "year", "value"]
)

GRID_ELECTRICITY_PRICE_FAVORABLE_MID = PowerGridTuple(
    metric="grid_electricity_price_favorable",
    unit="USD/MWh",
    year=ELECTRICITY_PRICE_MID_YEAR,
    value=29,
)

GRID_ELECTRICITY_PRICE_AVG_MID = PowerGridTuple(
    metric="grid_electricity_price_avg",
    unit="USD/MWh",
    year=ELECTRICITY_PRICE_MID_YEAR,
    value=57,
)

DEEPLY_DECARBONISED_POWER_SYSTEM_PRICE_AVG = PowerGridTuple(
    metric="deeply_decarbonised_power_system_price_avg",
    unit="percentage",
    year=ELECTRICITY_PRICE_END_YEAR,
    value=0.19,
)

DEEPLY_DECARBONISED_POWER_SYSTEM_PRICE_INCREASE = PowerGridTuple(
    metric="deeply_decarbonised_power_system_price_increase",
    unit="USD/MWh",
    year=ELECTRICITY_PRICE_END_YEAR,
    value=15,
)

GRID_ELECTRICITY_PRICE_FAVORABLE_START = PowerGridTuple(
    metric="grid_electricity_price_favorable",
    unit="USD/MWh",
    year=ELECTRICITY_PRICE_START_YEAR,
    value=29,
)
