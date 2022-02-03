'''Pandera recipies for data validation'''
import pandas as pd
import pandera as pa
from pandera import (
    DataFrameSchema, Column, Index, MultiIndex, Check
)
from pandera.typing import Index, DataFrame, Series

from mppsteel.utility.file_handling_utility import read_pickle_folder
from mppsteel.model_config import PKL_DATA_IMPORTS

# Strict w/ filter
# Transforming Schema: add_columns() , remove_columns(), update_columns(), rename_columns(), set_index(), and reset_index()

RE_YEAR_COL_TEST = '^[12][0-9]{3}$'
YEAR_VALUE_TEST = Column(int, Check.greater_than_or_equal_to(2020))
COUNTRY_CODE_CHECK = Column(str, Check.str_length(8), required=True)
NULLABLE_INT_CHECK = Column(int, nullable=True)
NULLABLE_STR_CHECK = Column(str, nullable=True)
UNIT_COL_TEST = Column(str, Check.str_contains('/'))

# OBJECT METHOD
COUNTRY_REF_SCHEMA = DataFrameSchema({
    'Country': Column(str),
    'ISO-alpha3 code': COUNTRY_CODE_CHECK,
    'Region 1': Column(str),
    'M49 Code': Column(int),
    'Continent': Column(str),
    'WSA Group Region': Column(str),
    'RMI Model Region': Column(str),
})

# CLASS METHOD
class FeedstockInputSchema(pa.SchemaModel):
    Year: Series[int] = pa.Field(ge=2020, coerce=True)
    Category: Series[str] = pa.Field()
    Value: Series[float] = pa.Field()
    Unit: Series[str] = pa.Field()
    Source: Series[str] = pa.Field()


STEEL_PLANT_DATA_SCHEMA = DataFrameSchema(
    columns={
        'Plant ID': Column(str, Check.str_length(8)),
        'Plant name (English)': Column(str),
        'Parent': Column(str),
        'Country': Column(str),
        'Region - analysis': Column(str),
        'Coordinates': Column(str),
        'Status': Column(str),
        'Start of operation': Column(int, coerce=True),
        'Fill in data BF-BOF': Column(float),
        'Fill in data EAF': Column(float),
        'Fill in data DRI': Column(float),
        'Estimated BF-BOF capacity (kt steel/y)': Column(float),
        'Estimated EAF capacity (kt steel/y)': Column(float),
        'Estimated DRI capacity (kt sponge iron/y)': Column(float),
        'Estimated DRI-EAF capacity (kt steel/y)': Column(float),
        'Final estimated BF-BOF capacity (kt steel/y)': Column(float),
        'Final estimated EAF capacity (kt steel/y)': Column(float),
        'Final estimated DRI capacity (kt sponge iron/y)': Column(float),
        'Final estimated DRI-EAF capacity (kt steel/y)': Column(float),
        'Abundant RES?': Column(int),
        'CCS available?': Column(int),
        'Cheap natural gas?': Column(int),
        'Industrial cluster?': Column(int),
        'Plant Technology in 2020': Column(str),
        'Source': Column(str),
    })

ETHANOL_PLASTIC_CHARCOAL_SCHEMA = DataFrameSchema(
    columns={
        'Classification': Column(str, Check.str_length(2)),
        'Year': YEAR_VALUE_TEST,
        'Period': YEAR_VALUE_TEST,
        'Period Desc.': YEAR_VALUE_TEST,
        'Aggregate Level': Column(int, Check.less_than(10)),
        'Is Leaf Code': Column(int, Check.less_than(2)),
        'Trade Flow Code': Column(int, Check.less_than(4)),
        'Trade Flow': Column(str, Check.isin(['Import', 'Export'])),
        'Reporter Code': Column(int),
        'Reporter': Column(str),
        'Reporter ISO': COUNTRY_CODE_CHECK,
        'Partner Code': Column(int),
        'Partner': Column(str, Check.str_matches('World')),
        'Partner ISO': Column(str, Check.str_matches('WLD')),
        '2nd Partner Code': Column(str, nullable=True),
        '2nd Partner': Column(str, nullable=True),
        '2nd Partner ISO': Column(str, nullable=True),
        'Customs Proc. Code': Column(str, nullable=True),
        'Customs': Column(str, nullable=True),
        'Mode of Transport Code': NULLABLE_STR_CHECK,
        'Mode of Transport': NULLABLE_STR_CHECK,
        'Commodity Code': NULLABLE_STR_CHECK,
        'Commodity': NULLABLE_INT_CHECK,
        'Qty Unit Code': NULLABLE_INT_CHECK,
        'Qty Unit': NULLABLE_INT_CHECK,
        'Qty':  NULLABLE_INT_CHECK,
        'Alt Qty Unit Code': NULLABLE_INT_CHECK,
        'Alt Qty Unit': NULLABLE_INT_CHECK,
        'Alt Qty': NULLABLE_INT_CHECK,
        'Netweight (kg)': NULLABLE_INT_CHECK,
        'Gross weight (kg)': NULLABLE_INT_CHECK,
        'Trade Value (US$)': NULLABLE_INT_CHECK,
        'CIF Trade Value (US$)': NULLABLE_INT_CHECK,
        'FOB Trade Value (US$)': NULLABLE_INT_CHECK,
        'Flag': NULLABLE_INT_CHECK,
    }
)

CAPEX_OPEX_PER_TECH_SCHEMA = DataFrameSchema({
    'Technology': Column(str),
    RE_YEAR_COL_TEST: Column(float, regex=True),
})

REGIONAL_STEEL_DEMAND_SCHEMA = DataFrameSchema({
    'Metric': Column(str),
    'Region': Column(str),
    'Scenario': Column(str, Check.isin(['BAU', 'High Circ'])),
    RE_YEAR_COL_TEST: Column(float, regex=True),
})

SCOPE3_EF_SCHEMA_1 = DataFrameSchema({
    'Category': Column(str),
    'Fuel': Column(str),
    'Unit': Column(str, Check.str_contains('/')),
    RE_YEAR_COL_TEST: Column(float, regex=True),
})

SCOPE3_EF_SCHEMA_2 = DataFrameSchema({
    'Year': Column(str, nullable=True),
    RE_YEAR_COL_TEST: Column(float, regex=True, nullable=True),
})

SCOPE1_EF_SCHEMA = DataFrameSchema({
    'Year': YEAR_VALUE_TEST,
    'Category': Column(str),
    'Metric': Column(str),
    'Unit': UNIT_COL_TEST,
    'Value': Column(float),
    'Source': Column(str)
})

ENERGY_PRICES_STATIC = DataFrameSchema({
    'Year': YEAR_VALUE_TEST,
    'Category': Column(str),
    'Metric': Column(str),
    'Value': Column(float),
    'Source': Column(str)
})

TECH_AVAILABILIY = DataFrameSchema({
    'Year': YEAR_VALUE_TEST,
    'Category': Column(str),
    'Metric': Column(str),
    'Value': Column(float),
    'Source': Column(str)
})

def import_data_tests():
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref")
    COUNTRY_REF_SCHEMA.validate(country_ref)

    feedstock_prices = read_pickle_folder(PKL_DATA_IMPORTS, "feedstock_prices")
    FeedstockInputSchema.validate(feedstock_prices)

    steel_plants = read_pickle_folder(PKL_DATA_IMPORTS, "steel_plants")
    # STEEL_PLANT_DATA_SCHEMA.validate(steel_plants) # will fail due to 'anticipated and NaNs'

    greenfield_capex = read_pickle_folder(PKL_DATA_IMPORTS, "greenfield_capex")
    brownfield_capex = read_pickle_folder(PKL_DATA_IMPORTS, "brownfield_capex")
    other_opex = read_pickle_folder(PKL_DATA_IMPORTS, "other_opex")

    for capex_df in [greenfield_capex, brownfield_capex, other_opex]:
        CAPEX_OPEX_PER_TECH_SCHEMA.validate(capex_df)


    s3_emissions_factors_1 = read_pickle_folder(PKL_DATA_IMPORTS, "s3_emissions_factors_1")
    SCOPE3_EF_SCHEMA_1.validate(s3_emissions_factors_1)

    s3_emissions_factors_2 = read_pickle_folder(PKL_DATA_IMPORTS, "s3_emissions_factors_2")
    SCOPE3_EF_SCHEMA_2.validate(s3_emissions_factors_2)

    ethanol_plastic_charcoal = read_pickle_folder(PKL_DATA_IMPORTS, "ethanol_plastic_charcoal")
    ETHANOL_PLASTIC_CHARCOAL_SCHEMA.validate(ethanol_plastic_charcoal)

    s1_emissions_factors = read_pickle_folder(PKL_DATA_IMPORTS, "s1_emissions_factors")
    SCOPE1_EF_SCHEMA.validate(s1_emissions_factors)

    static_energy_prices = read_pickle_folder(PKL_DATA_IMPORTS, "static_energy_prices")
    ENERGY_PRICES_STATIC.validate(static_energy_prices)

    tech_availability = read_pickle_folder(PKL_DATA_IMPORTS, "tech_availability")

    # (pa.schema_inference.infer_schema(steel_plants))
