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

COUNTRY_REF_SCHEMA = DataFrameSchema({
        'Country': Column(str),
        'ISO-alpha3 code': Column(str, required=True),
        'Region 1': Column(str),
        'M49 Code': Column(int),
        'Continent': Column(str),
        'WSA Group Region': Column(str),
        'RMI Model Region': Column(str),
    })

class FeedstockInputSchema(pa.SchemaModel):
    Year: Series[int] = pa.Field(eq=200, coerce=True)
    Category: Series[str] = pa.Field()
    Value: Series[float] = pa.Field()
    Unit: Series[str] = pa.Field()
    Source: Series[str] = pa.Field


def import_data_tests():
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref")
    COUNTRY_REF_SCHEMA.validate(country_ref)

    feedstock_prices = read_pickle_folder(PKL_DATA_IMPORTS, "feedstock_prices")
    FeedstockInputSchema.validate(feedstock_prices)
