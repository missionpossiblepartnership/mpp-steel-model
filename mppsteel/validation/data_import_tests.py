"""Data Import Pandera Checks"""
import pandas as pd
import pandera as pa

from pandera import DataFrameSchema, Column, Index, MultiIndex, Check
from pandera.typing import DataFrame, Series

from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.config.model_config import PKL_DATA_IMPORTS
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST

# Strict w/ filter
# Transforming Schema: add_columns() , remove_columns(), update_columns(), rename_columns(), set_index(), and reset_index()
# You can create dummy data from a schema schema.example(size=x)
# pa.schema_inference.infer_schema(steel_plants))

TECH_REFERENCE_LIST_ADJ = TECH_REFERENCE_LIST.copy()
TECH_REFERENCE_LIST_ADJ += ["Charcoal mini furnace", "Close plant", "New capacity"]

RE_YEAR_COL_TEST = "^[12][0-9]{3}$"
YEAR_VALUE_TEST = Column(int, Check.greater_than_or_equal_to(2020))
COUNTRY_CODE_CHECK = Column(str, Check.str_length(3), required=True, nullable=True)
NULLABLE_INT_CHECK = Column(int, nullable=True)
NULLABLE_STR_CHECK = Column(str, nullable=True)
UNIT_COL_TEST = Column(str, Check.str_contains("/"))
VALUE_COL_TEST = Column(float, nullable=False)
COERCE_TO_STRING = Column(str, nullable=True, coerce=True)

COUNTRY_REF_SCHEMA = DataFrameSchema(
    {
        "Country": Column(str),
        "ISO-alpha3 code": Column(
            str, Check.str_length(3), required=True, nullable=True
        ),
        "M49 Code": Column(int),
        "Region 1": Column(str),
        "Continent": Column(str),
        "WSA Group Region": Column(str),
        "RMI Model Region": Column(str),
    }
)

FEEDSTOCK_INPUT_SCHEMA = DataFrameSchema(
    {
        "Year": Column(int, Check.greater_than_or_equal_to(2020)),
        "Category": Column(str),
        "Unit": UNIT_COL_TEST,
        "Value": VALUE_COL_TEST,
        "Source": Column(str, nullable=True, coerce=True),
    }
)

STEEL_PLANT_DATA_SCHEMA = DataFrameSchema(
    columns={
        "Plant ID": Column(str, Check.str_length(8)),
        "Plant name (English)": Column(str),
        "Parent": Column(str),
        "Country": Column(str),
        "Region - analysis": Column(str),
        "Coordinates": Column(str),
        "Status": Column(str),
        "Start of operation": Column(
            str, nullable=True, coerce=True
        ),  # WARNING! handle this case later
        "Fill in data BF-BOF": Column(float, coerce=True),
        "Fill in data EAF": Column(float, coerce=True),
        "Fill in data DRI": Column(float, coerce=True),
        "Estimated BF-BOF capacity (kt steel/y)": Column(float, coerce=True),
        "Estimated EAF capacity (kt steel/y)": Column(
            str, coerce=True
        ),  # WARNING! handle this case later
        "Estimated DRI capacity (kt sponge iron/y)": Column(float, coerce=True),
        "Estimated DRI-EAF capacity (kt steel/y)": Column(float, coerce=True),
        "Final estimated BF-BOF capacity (kt steel/y)": Column(float, coerce=True),
        "Final estimated EAF capacity (kt steel/y)": Column(
            str, coerce=True
        ),  # WARNING! handle this case later
        "Final estimated DRI capacity (kt sponge iron/y)": Column(float, coerce=True),
        "Final estimated DRI-EAF capacity (kt steel/y)": Column(float, coerce=True),
        "Abundant RES?": Column(int, Check.isin([0, 1])),
        "CCS available?": Column(int, Check.isin([0, 1])),
        "Cheap natural gas?": Column(int, Check.isin([0, 1])),
        "Industrial cluster?": Column(int, Check.isin([0, 1])),
        "Plant Technology in 2020": Column(str),
        "Source": Column(str),
    }
)

ETHANOL_PLASTIC_CHARCOAL_SCHEMA = DataFrameSchema(
    columns={
        "Classification": Column(str, Check.str_length(2)),
        "Year": Column(int, Check.greater_than_or_equal_to(2020)),
        "Period": Column(int, Check.greater_than_or_equal_to(2020)),
        "Period Desc.": Column(int, Check.greater_than_or_equal_to(2020)),
        "Aggregate Level": Column(int, Check.less_than(10)),
        "Is Leaf Code": Column(int, Check.less_than(2)),
        "Trade Flow Code": Column(int, Check.less_than(5)),
        "Trade Flow": Column(
            str, Check.isin(["Import", "Export", "Re-Export", "Re-Import"])
        ),
        "Reporter Code": Column(int),
        "Reporter": Column(str),
        "Reporter ISO": COUNTRY_CODE_CHECK,
        "Partner Code": Column(int),
        "Partner": Column(str, Check.str_matches("World")),
        "Partner ISO": Column(str, Check.str_matches("WLD")),
        "2nd Partner Code": Column(str, nullable=True, coerce=True),
        "2nd Partner": Column(str, nullable=True, coerce=True),
        "2nd Partner ISO": Column(str, nullable=True, coerce=True),
        "Customs Proc. Code": Column(str, nullable=True, coerce=True),
        "Customs": Column(str, nullable=True, coerce=True),
        "Mode of Transport Code": Column(str, nullable=True, coerce=True),
        "Mode of Transport": Column(str, nullable=True, coerce=True),
        "Commodity Code": Column(int, nullable=True),
        "Commodity": Column(str, nullable=True),
        "Qty Unit Code": Column(int, nullable=True),
        "Qty Unit": Column(str, nullable=True),
        "Qty": Column(int, nullable=True),
        "Alt Qty Unit Code": Column(str, nullable=True, coerce=True),
        "Alt Qty Unit": Column(str, nullable=True, coerce=True),
        "Alt Qty": Column(float, nullable=True),
        "Netweight (kg)": Column(float, nullable=True, coerce=True),
        "Gross weight (kg)": Column(float, nullable=True, coerce=True),
        "Trade Value (US$)": Column(float, nullable=True, coerce=True),
        "CIF Trade Value (US$)": Column(float, nullable=True, coerce=True),
        "FOB Trade Value (US$)": Column(float, nullable=True, coerce=True),
        "Flag": Column(int, nullable=True),
    }
)

CAPEX_OPEX_PER_TECH_SCHEMA = DataFrameSchema(
    {
        "Technology": Column(str, Check.isin(TECH_REFERENCE_LIST_ADJ)),
        RE_YEAR_COL_TEST: Column(float, regex=True),
    }
)

REGIONAL_STEEL_DEMAND_SCHEMA = DataFrameSchema(
    {
        "Metric": Column(str),
        "Region": Column(str),
        "Scenario": Column(str, Check.isin(["BAU", "High Circ"])),
        RE_YEAR_COL_TEST: Column(float, regex=True),
    }
)

SCOPE3_EF_SCHEMA_1 = DataFrameSchema(
    {
        "Category": Column(str),
        "Fuel": Column(str),
        "Unit": UNIT_COL_TEST,
        RE_YEAR_COL_TEST: Column(float, regex=True, nullable=True),
    }
)

SCOPE3_EF_SCHEMA_2 = DataFrameSchema(
    {
        "Year": Column(str, nullable=True),
        RE_YEAR_COL_TEST: Column(float, regex=True, nullable=True),
    }
)

SCOPE1_EF_SCHEMA = DataFrameSchema(
    {
        "Year": Column(int, Check.greater_than_or_equal_to(2020)),
        "Category": Column(str),
        "Metric": Column(str),
        "Unit": UNIT_COL_TEST,
        "Value": VALUE_COL_TEST,
        "Source": Column(str, nullable=True, coerce=True),
    }
)

ENERGY_PRICES_STATIC_SCHEMA = DataFrameSchema(
    {
        "Year": Column(int, Check.greater_than_or_equal_to(2020)),
        "Category": Column(str),
        "Metric": Column(str),
        "Value": VALUE_COL_TEST,
        "Source": Column(str),
    }
)

TECH_AVAILABILIY_SCHEMA = DataFrameSchema(
    {
        "Technology": Column(str, Check.isin(TECH_REFERENCE_LIST_ADJ)),
        "Main Technology Type": Column(str),
        "Technology Phase": Column(str),
        "Year available from": Column(int, Check.greater_than_or_equal_to(2020)),
        "Year available until": Column(int, Check.greater_than_or_equal_to(2020)),
        "Source": Column(str, nullable=True, coerce=True),
    }
)

CO2_CCS_SCHEMA = DataFrameSchema(
    {
        "Metric": Column(str),
        "Year": Column(int, Check.greater_than_or_equal_to(2020)),
        "Units": Column(str),
        "Value": VALUE_COL_TEST,
        "Source": Column(str),
    }
)

STEEL_BUSINESS_CASES_SCHEMA = DataFrameSchema(
    {
        "Section": Column(str),
        "Process": Column(str),
        "Process Detail": Column(str),
        "Step": Column(str),
        "Material Category": Column(str),
        "Unit": Column(str),
    }
)


def import_data_tests():
    """Example data tests.
    """
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref")
    COUNTRY_REF_SCHEMA.validate(country_ref)

    feedstock_prices = read_pickle_folder(PKL_DATA_IMPORTS, "feedstock_prices")
    FEEDSTOCK_INPUT_SCHEMA.validate(feedstock_prices)

    steel_plants = read_pickle_folder(PKL_DATA_IMPORTS, "steel_plants")
    STEEL_PLANT_DATA_SCHEMA.validate(
        steel_plants
    )  # will fail due to 'anticipated and NaNs'

    greenfield_capex = read_pickle_folder(PKL_DATA_IMPORTS, "greenfield_capex")
    brownfield_capex = read_pickle_folder(PKL_DATA_IMPORTS, "brownfield_capex")
    other_opex = read_pickle_folder(PKL_DATA_IMPORTS, "other_opex")

    for capex_df in [greenfield_capex, brownfield_capex, other_opex]:
        CAPEX_OPEX_PER_TECH_SCHEMA.validate(capex_df)

    s3_emissions_factors_1 = read_pickle_folder(
        PKL_DATA_IMPORTS, "s3_emissions_factors_1"
    )
    SCOPE3_EF_SCHEMA_1.validate(s3_emissions_factors_1)

    s3_emissions_factors_2 = read_pickle_folder(
        PKL_DATA_IMPORTS, "s3_emissions_factors_2"
    )
    SCOPE3_EF_SCHEMA_2.validate(s3_emissions_factors_2)

    ethanol_plastic_charcoal = read_pickle_folder(
        PKL_DATA_IMPORTS, "ethanol_plastic_charcoal"
    )
    ETHANOL_PLASTIC_CHARCOAL_SCHEMA.validate(ethanol_plastic_charcoal)

    s1_emissions_factors = read_pickle_folder(PKL_DATA_IMPORTS, "s1_emissions_factors")
    SCOPE1_EF_SCHEMA.validate(s1_emissions_factors)

    static_energy_prices = read_pickle_folder(PKL_DATA_IMPORTS, "static_energy_prices")
    ENERGY_PRICES_STATIC_SCHEMA.validate(static_energy_prices)

    tech_availability = read_pickle_folder(PKL_DATA_IMPORTS, "tech_availability")
    TECH_AVAILABILIY_SCHEMA.validate(tech_availability)

    regional_steel_demand = read_pickle_folder(
        PKL_DATA_IMPORTS, "regional_steel_demand"
    )
    REGIONAL_STEEL_DEMAND_SCHEMA.validate(regional_steel_demand)

    co2_ccs = read_pickle_folder(PKL_DATA_IMPORTS, "ccs_co2")
    CO2_CCS_SCHEMA.validate(co2_ccs)

    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    STEEL_BUSINESS_CASES_SCHEMA.validate(business_cases)
