"""Script to determine the variable plant cost types dependent on regions."""

import pandas as pd
from tqdm import tqdm

from mppsteel.model_config import (
    PKL_FOLDER,
)

from mppsteel.model.solver import (
    load_business_cases,
)

from mppsteel.utility.utils import (
    serialize_file, get_logger, read_pickle_folder
)

from mppsteel.data_loading.data_interface import (
    commodity_data_getter, static_energy_prices_getter,
)

from mppsteel.model.prices_and_emissions_tables import (
    dynamic_energy_price_getter
)

# Create logger
logger = get_logger("Variable Plant Cost Archetypes")

def plant_variable_costs():
    df_list = []

    options = [[0,0,0],[1,0,0],[1,1,0],[1,1,1],[0,1,1],[0,0,1],[0,1,0],[1,0,1]]
    plant_iterations = {''.join([str(num) for num in option]): option for option in options}

    electricity_minimodel_timeseries = read_pickle_folder(PKL_FOLDER, 'electricity_minimodel_timeseries', 'df')
    hydrogen_minimodel_timeseries = read_pickle_folder(PKL_FOLDER, 'hydrogen_minimodel_timeseries', 'df')

    static_energy_prices = read_pickle_folder(PKL_FOLDER, 'static_energy_prices', 'df')[['Metric', 'Year', 'Value']]
    feedstock_dict = generate_feedstock_dict()

    business_cases = load_business_cases()

    for plant_iteration in tqdm(plant_iterations, total=len(plant_iterations), desc='Plant variables'):
        df = generate_variable_costs(
            business_cases_df=business_cases,
            plant_iteration=plant_iteration,
            year_end=2050,
            feedstock_dict=feedstock_dict,
            static_energy_df=static_energy_prices,
            electricity_df=electricity_minimodel_timeseries,
            hydrogen_df=hydrogen_minimodel_timeseries
        )
        df['plant_iteration'] = plant_iteration
        df_list.append(df)

    return pd.concat(df_list).reset_index(drop=True)


def generate_feedstock_dict():
    commodities_df = read_pickle_folder(PKL_FOLDER, 'commodities_df', 'df')
    feedstock_prices = read_pickle_folder(PKL_FOLDER, 'feedstock_prices', 'df')
    commodities_dict = commodity_data_getter(commodities_df)
    commodity_dictname_mapper = {'plastic': 'Plastic waste', 'ethanol': 'Ethanol', 'charcoal': 'Charcoal'}
    for key in commodity_dictname_mapper.keys():
        commodities_dict[commodity_dictname_mapper[key]] = commodities_dict.pop(key)
    return {**commodities_dict, **dict(zip(feedstock_prices['Metric'], feedstock_prices['Value']))}


def generate_variable_costs(
    business_cases_df: pd.DataFrame,
    plant_iteration: list = None,
    year_end: int = None,
    feedstock_dict: dict = None,
    static_energy_df: pd.DataFrame = None,
    electricity_df: pd.DataFrame = None,
    hydrogen_df: pd.DataFrame = None
) -> pd.DataFrame:

    df_list = []

    plant_iteration_dict = {
        'abundant_res': plant_iteration[0],
        'ccs_available': plant_iteration[1],
        'cheap_natural_gas': plant_iteration[2],
    }

    # Create resources reference list
    static_energy_list = static_energy_df['Metric'].unique().tolist()
    feedstock_list = list(feedstock_dict.keys())

    # Create a year range
    year_range = range(2020, tuple({year_end+1 or 2021})[0])

    for year in tqdm(year_range, desc='Variable costs'):
        df_c = business_cases_df.copy()

        df_c['Static'] = ''
        df_c['Feedstock'] = ''
        df_c['Electricity'] = ''
        df_c['Hydrogen'] = ''
        df_c['Natural gas'] = ''

        static_year = year
        if year > 2026:
            static_year = 2026
        dynamic_year = year
        if year > 2050:
            dynamic_year = 2050

        low_elec_price = dynamic_energy_price_getter(electricity_df, 'favorable', dynamic_year)
        high_elec_price = dynamic_energy_price_getter(electricity_df, 'average', dynamic_year)
        low_hyd_price = dynamic_energy_price_getter(hydrogen_df, 'favorable', dynamic_year)
        high_hyd_price = dynamic_energy_price_getter(hydrogen_df, 'average', dynamic_year)
        natural_gas_high = static_energy_prices_getter(static_energy_df, 'Natural gas - high', static_year)
        natural_gas_low = static_energy_prices_getter(static_energy_df, 'Natural gas - low', static_year)

        for row in df_c.itertuples():
            resource = row.material_category
            resource_consumed = row.value

            if resource in static_energy_list:
                price_unit_value = static_energy_prices_getter(static_energy_df, resource, static_year)
                df_c.loc[row.Index, 'Static'] = resource_consumed * price_unit_value

            if resource in feedstock_list:
                price_unit_value = feedstock_dict[resource]
                df_c.loc[row.Index, 'Feedstock'] = resource_consumed * price_unit_value

            if resource == 'Natural gas':
                if plant_iteration_dict['cheap_natural_gas'] == 1:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * natural_gas_low
                elif plant_iteration_dict['cheap_natural_gas'] == 0:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * natural_gas_high

            if resource == 'Electricity':
                if plant_iteration_dict['abundant_res'] == 1 or plant_iteration_dict['ccs_available'] == 1:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * low_elec_price
                elif plant_iteration_dict['abundant_res'] == 0 and plant_iteration_dict['ccs_available'] == 0:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * high_elec_price

            if resource == 'Hydrogen':
                if plant_iteration_dict['abundant_res'] == 1 or plant_iteration_dict['ccs_available'] == 1:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * low_hyd_price
                elif plant_iteration_dict['abundant_res'] == 0 and plant_iteration_dict['ccs_available'] == 0:
                    df_c.loc[row.Index, 'Natural gas'] = resource_consumed * high_hyd_price

        df_c['year'] = year
        df_list.append(df_c)

    combined_df = pd.concat(df_list)
    return combined_df

def format_variable_costs(variable_cost_df: pd.DataFrame):

    df_c = variable_cost_df.copy()
    df_c.drop(labels=['value'], axis=1, inplace=True)
    df_c = df_c.melt(id_vars=['plant_iteration', 'technology', 'year', 'material_category', 'unit'],var_name=['cost_type'], value_name='cost')
    df_c['cost'] = df_c['cost'].replace('', 0)
    return df_c.groupby(by=['plant_iteration', 'year', 'technology']).sum().sort_values(by=['plant_iteration', 'year', 'technology'])

def generate_variable_plant_summary(serialize_only: bool = False):
    all_plant_variable_costs = plant_variable_costs()
    all_plant_variable_costs_summary = format_variable_costs(all_plant_variable_costs)

    if serialize_only:
        logger.info(f'-- Serializing dataframes')
        serialize_file(all_plant_variable_costs_summary, PKL_FOLDER, "all_plant_variable_costs_summary")
    return all_plant_variable_costs_summary
