"""Formats Regional Steel Demand and defines getter function"""

import itertools

import pandas as pd

from mppsteel.model_config import (
    PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE, MODEL_YEAR_END
)

from mppsteel.model_scenarios import STEEL_DEMAND_SCENARIO_MAPPER

from mppsteel.utility.utils import (
    timer_func,
    read_pickle_folder,
    serialize_file,
    match_country,
)
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Regional Steel Demand Formatter")

RMI_MATCHER = {
    'Japan, South Korea, and Taiwan': ['JPN', 'KOR', 'TWN'],
    'World': ['GBL']
}

def get_unique_countries(country_arrays):
    b_set = set(tuple(x) for x in country_arrays)
    b_list = [list(x) for x in b_set if x]
    return list(itertools.chain(*b_list))

def get_countries_from_group(country_ref: pd.DataFrame, grouping: str, group: str, exc_list: list = None):
    df_c = country_ref[['ISO-alpha3 code', grouping]].copy()
    code_list = df_c.set_index([grouping, 'ISO-alpha3 code']).sort_index().loc[group].index.unique().to_list()
    if exc_list:
        exc_codes = [match_country(country) for country in exc_list]
        return list(set(code_list).difference(exc_codes))
    return code_list

def steel_demand_region_assignor(region: str, country_ref: pd.DataFrame, rmi_matcher: dict):
    if region in rmi_matcher.keys():
        return rmi_matcher[region]
    return get_countries_from_group(country_ref, 'RMI Model Region', region)


def steel_demand_creator(rmi_matcher: dict):
    logger.info('Formatting the Regional Steel Demand Data')
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, 'country_ref', 'df')
    steel_demand = read_pickle_folder(PKL_DATA_IMPORTS, 'regional_steel_demand', 'df')
    steel_demand['country_code'] = steel_demand['Region'].apply(lambda x: steel_demand_region_assignor(x, country_ref, rmi_matcher))
    steel_demand = steel_demand.melt(id_vars = ['Metric', 'Region', 'Scenario', 'country_code'], var_name=['year'])
    steel_demand.set_index(['year', 'Scenario', 'Metric'], inplace=True)
    return steel_demand

@timer_func
def get_steel_demand(serialize_only: bool = False):
    steel_demand_f = steel_demand_creator(RMI_MATCHER) # pickle this
    if serialize_only:
        serialize_file(steel_demand_f, PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted")
    return steel_demand_f

def steel_demand_getter(
    df: pd.DataFrame, year: int, scenario: str, metric: str, country_code: str, default_country: str = 'GBL'):
    df_c = df.copy()
    # define country list based on the data_type
    country_list = get_unique_countries(df_c['country_code'].values)
    metric_mapper = {
        'crude': 'Crude steel demand',
        'scrap': 'Scrap availability',
    }

    # Apply country check and use default
    if country_code in country_list:
        df_c = df_c[df_c['country_code'].str.contains(country_code, regex=False)]
    else:
        df_c = df_c[df_c['country_code'].str.contains(default_country, regex=False)]
    # Cap year at 2050
    year = min(MODEL_YEAR_END, year)
    # Apply subsets
    # Scenario: BAU, High Circ, average
    # Metric: crude, scrap
    scenario_entry = STEEL_DEMAND_SCENARIO_MAPPER[scenario]
    if scenario_entry == 'average':
        df1_val = df_c.xs((str(year), 'BAU', metric_mapper[metric]), level=['year', 'Scenario', 'Metric']).value.values[0]
        df2_val = df_c.xs((str(year), 'High Circ', metric_mapper[metric]), level=['year', 'Scenario', 'Metric']).value.values[0]
        return (df1_val + df2_val) / 2
    else:
        df_c = df_c.xs((str(year), scenario_entry, metric_mapper[metric]), level=['year', 'Scenario', 'Metric'])
        df_c.reset_index(drop=True, inplace=True)
        # Return the value figure
        return df_c.value.values[0]
