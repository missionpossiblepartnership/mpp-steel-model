"""Tests for negative values"""

import pandas as pd


def test_negative_df_values(df_combined: pd.DataFrame):
    assert (df_combined.values < 0).any() == False


def test_negative_list_values(a_list: list):
    assert all(x > 0 for x in a_list)
