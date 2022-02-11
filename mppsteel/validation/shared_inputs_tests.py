"""Data Import Pandera Checks"""
import pandas as pd
import pandera as pa

from pandera import DataFrameSchema, Column, Index, MultiIndex, Check
from pandera.typing import DataFrame, Series

from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.config.model_config import PKL_DATA_IMPORTS, PE_MODEL_SHEETNAME_DICT

RE_YEAR_COL_TEST = "^[12][0-9]{3}$"
YEAR_VALUE_TEST = Column(int, Check.greater_than_or_equal_to(2020))
COUNTRY_CODE_CHECK = Column(str, Check.str_length(3), required=True, nullable=True)
NULLABLE_INT_CHECK = Column(int, nullable=True)
NULLABLE_STR_CHECK = Column(str, nullable=True)
UNIT_COL_TEST = Column(str, Check.str_contains("/"))
VALUE_COL_TEST = Column(float, nullable=False)
COERCE_TO_STRING = Column(str, nullable=True, coerce=True)

POWER_RES_PRICE_CHECK = DataFrameSchema(
    {
        "Tab": Column(str),
        "Captive power source": Column(str),
        "Region": Column(int),
        "Customer": Column(str),
        "Grid scenario": Column(str),
        "Cost scenario ": Column(str),
        "Unit": Column(str, Check.str_contains("/")),
        RE_YEAR_COL_TEST: Column(float, regex=True),
    }
)

POWER_MODEL_SCHEMA = DataFrameSchema(
    {
        "Tab": Column(str),
        "Captive power source": Column(str, required=False),
        "Region": Column(str),
        "Customer": Column(str),
        "Grid scenario": Column(str),
        "Cost scenario ": Column(str),
        "Unit": Column(str, Check.str_contains("/")),
        RE_YEAR_COL_TEST: Column(float, regex=True, required=True),
    }
)

HYDROGEN_MODEL_SCHEMA = DataFrameSchema(
    {
        "Tab": Column(str),
        "Variable": Column(str),
        "Region": Column(str),
        "Production scenario": Column(str),
        "Cost scenario": Column(str),
        "Unit ": Column(str, Check.str_contains("/")),
        RE_YEAR_COL_TEST: Column(float, regex=True, required=True),
    }
)

BIO_PRICE_MODEL_SCHEMA = DataFrameSchema(
    {
        "Tab": Column(str),
        "Region": Column(str),
        "Price scenario": Column(str),
        "Feedstock type": Column(str),
        "Unit": Column(str, Check.str_contains("/")),
        RE_YEAR_COL_TEST: Column(float, regex=True, required=True),
    }
)

BIO_CONSTRAINT_MODEL_SCHEMA = DataFrameSchema(
    {
        "Tab": Column(str),
        "Scenario": Column(str),
        "Sector": Column(str),
        "Unit": Column(str, Check.str_contains("EJ")),
        RE_YEAR_COL_TEST: Column(float, regex=True, required=True),
    }
)

CCUS_TRANSPORT_SCHEMA = DataFrameSchema(
    {
        "Tab": Column(str),
        "Reference": Column(str),
        "Cost Tier": Column(int),
        "Region": Column(str),
        "Region factor": Column(float),
        "Transport Type": Column(
            str, Check.isin(["Onshore Pipeline", "Offshore Pipeline", "Shipping"])
        ),
        "Cost Estimate": Column(str, Check.isin(["BaseCase", "Low"])),
        "Unit_ Capacity": Column(str, Check.str_contains("/")),
        "Capacity": Column(float),
        "Unit_Transport costs": Column(str, Check.str_contains("/")),
        "Transport costs _Node 1": Column(float),
        "Transport costs _Node 2": Column(float),
        "Transport costs _Node 3": Column(float),
    }
)

CCUS_STORAGE_FACTOR = DataFrameSchema(
    {
        "Tab": Column(str),
        "Reference": Column(str),
        "Region": Column(str),
        "Cost Tier": Column(int),
        "Regional factor": Column(float),
        "Storage location": Column(str, Check.isin(["Offshore", "Onshore"])),
        "Storage type": Column(
            str, Check.isin(["Depleted O&G field", "Saline aquifers"])
        ),
        "Reusable legacy wells": Column(str, Check.isin(["Yes", "No"])),
        "Value": Column(str, Check.isin(["Low", "Medium", "High"])),
        "Unit": Column(str, Check.str_contains("/")),
        "Costs -  capacity 5": Column(float),
    }
)


def shared_inputs_tests():
    """Example data tests
    """
    power_model = read_pickle_folder(PKL_DATA_IMPORTS, "power_model", "dict")
    hydrogen_model = read_pickle_folder(PKL_DATA_IMPORTS, "hydrogen_model", "dict")
    bio_model = read_pickle_folder(PKL_DATA_IMPORTS, "bio_model", "dict")
    ccus_model = read_pickle_folder(PKL_DATA_IMPORTS, "ccus_model", "dict")

    for tab in PE_MODEL_SHEETNAME_DICT["power"]:
        POWER_MODEL_SCHEMA.validate(power_model[tab])

    for tab in PE_MODEL_SHEETNAME_DICT["hydrogen"]:
        HYDROGEN_MODEL_SCHEMA.validate(hydrogen_model[tab])

    BIO_PRICE_MODEL_SCHEMA.validate(bio_model["Feedstock_Prices"])
    BIO_CONSTRAINT_MODEL_SCHEMA.validate(bio_model["Biomass_constraint"])

    CCUS_TRANSPORT_SCHEMA.validate(ccus_model["Transport"])
    CCUS_STORAGE_FACTOR.validate(ccus_model["Storage"])
