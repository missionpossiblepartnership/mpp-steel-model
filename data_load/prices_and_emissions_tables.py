"""Script that creates the price and emissions tables."""

# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from utils import (
    get_logger, read_pickle_folder, CountryMetadata, serialize_df)

from model_config import PKL_FOLDER, TECH_REFERENCE_LIST
from data_interface import (
    commodity_data_getter, static_energy_prices_getter, scope1_emissions_getter,
    grid_emissivity_getter, scope3_ef_getter, carbon_tax_getter
)

# Create logger
logger = get_logger('Prices and Emissions')

def dynamic_energy_price_getter(df: pd.DataFrame, scenario: str, year: str) -> float:
    df_c = df.copy()
    df_c.set_index(['scenario', 'year'], inplace=True)
    value = df_c.loc[scenario, year]['value']
    return value

def apply_emissions(
    df: pd.DataFrame,
    single_year: int = None,
    year_end: int = 2021,
    s1_emissions_df: pd.DataFrame = None,
    s2_emissions_df: pd.DataFrame = None,
    s3_emissions_df: pd.DataFrame = None,
    carbon_tax_df: pd.DataFrame = None,
    non_standard_dict: dict = None,
    scope: str = '1'
) -> pd.DataFrame:

    # Create resources reference list
    s1_emissions_resources = s1_emissions_df['Metric'].unique().tolist()
    s2_emissions_resources = ['Electricity']
    s3_emissions_resources = s3_emissions_df['Fuel'].unique().tolist()

    # Create a year range
    year_range = range(2020, tuple({year_end+1 or 2021})[0])
    if single_year:
        year_range = [single_year]

    df_list = []

    for year in year_range:
        print(f'calculating year {year}')
        df_c = df.copy()
        df_c['year'] = year
        df_c['S1'] = ''
        df_c['S2'] = ''
        df_c['S3'] = ''
        df_c['carbon_cost'] = ''

        for row in df_c.itertuples():
            resource = row.material_category
            resource_consumed = row.value

            if resource in s1_emissions_resources:
                emission_unit_value = scope1_emissions_getter(s1_emissions_df, resource)
                # S1 emissions without process emissions or CCS/CCU
                df_c.loc[row.Index, 'S1'] = (resource_consumed * emission_unit_value / 1000)
            else:
                df_c.loc[row.Index, 'S1'] = 0

            if resource in s2_emissions_resources:
                emission_unit_value = grid_emissivity_getter(s2_emissions_df, year)
                df_c.loc[row.Index, 'S2'] = resource_consumed * emission_unit_value / 1000
            else:
                df_c.loc[row.Index, 'S2'] = 0

            if resource in s3_emissions_resources:
                emission_unit_value = scope3_ef_getter(s3_emissions_df, resource, year)
                df_c.loc[row.Index, 'S3'] = resource_consumed * emission_unit_value
            else:
                df_c.loc[row.Index, 'S3'] = 0

            if carbon_tax_df is not None:
                carbon_tax_unit = carbon_tax_getter(carbon_tax_df, year)
            else:
                carbon_tax_unit = 0

            if scope == '1':
                S1_value = df_c.at[row.Index, 'S1']
                df_c.loc[row.Index, 'carbon_cost'] = S1_value * carbon_tax_unit
            elif scope == '2&3':
                S2_value = df_c.at[row.Index, 'S2']
                S3_value = df_c.at[row.Index, 'S3']
                df_c.loc[row.Index, 'carbon_cost'] = (S2_value + S3_value) * carbon_tax_unit

        df_list.append(df_c)

    combined_df = pd.concat(df_list)
    combined_df.drop(labels=['value'], axis=1, inplace=True)
    combined_df = combined_df.melt(id_vars=['technology', 'year', 'material_category', 'unit'],var_name=['scope'], value_name='emissions')

    if carbon_tax_df:
        combined_df = combined_df.loc[combined_df['scope'] != 'carbon_cost'].copy()

    carbon_df = combined_df.loc[combined_df['scope'] == 'carbon_cost'].reset_index(drop=True).copy()
    carbon_df.rename(mapper={'emissions': 'value'}, axis=1, inplace=True)
    emissions_df = combined_df.loc[combined_df['scope'] != 'carbon_cost'].reset_index(drop=True).copy()

    return emissions_df, carbon_df


def create_emissions_ref_dict(df: pd.DataFrame, tech_ref_list: list):
    value_ref_dict = {}
    resource_list = ['Process emissions', 'Captured CO2', 'Used CO2']
    for technology in tech_ref_list:
        resource_dict = {}
        for resource in resource_list:
            try:
                val = df[(df['technology'] == technology) & (df['material_category'] == resource)]['value'].values[0]
            except:
                val = 0
            resource_dict[resource] = val
        value_ref_dict[technology] = resource_dict
    return value_ref_dict

def full_emissions(df: pd.DataFrame, emissions_exceptions_dict: dict, tech_list: list):
    df_c = df.copy()
    for year in df_c.index.get_level_values(0).unique().values:
        for technology in tech_list:
            val = df_c.loc[year, technology]['emissions']
            em_exc_dict = emissions_exceptions_dict[technology]
            process_emission = em_exc_dict['Process emissions']
            combined_ccs_ccu_emissions = em_exc_dict['Used CO2'] + em_exc_dict['Captured CO2']
            df_c.loc[year, technology]['emissions'] = val + process_emission - combined_ccs_ccu_emissions
    return df_c

def generate_variable_costs(
    df: pd.DataFrame,
    single_year: int = None,
    year_end: int = 2021,
    region: str = None,
    feedstock_dict: dict = None,
    static_energy_df: pd.DataFrame = None,
    electricity_df: pd.DataFrame = None,
    hydrogen_df: pd.DataFrame = None,
    natural_gas_ref: pd.DataFrame = None,
    solar_ref: pd.DataFrame = None,
    wind_ref: pd.DataFrame = None
) -> pd.DataFrame:

    # Create resources reference list
    static_energy_list = static_energy_df['Metric'].unique().tolist()
    feedstock_list = list(feedstock_dict.keys())

    # Create a year range
    year_range = range(2020, tuple({year_end+1 or 2021})[0])
    if single_year:
        year_range = [single_year]

    df_list = []

    # Set default region
    selected_region = list({region or 'USA'})[0]

    for year in year_range:
        print(f'calculating year {year}')
        df_c = df.copy()
        df_c['Static'] = ''
        df_c['Feedstock'] = ''
        df_c['Electricity'] = ''
        df_c['Hydrogen'] = ''
        df_c['Natural gas'] = ''

        if year > 2026:
            static_year = 2026
        else:
            static_year = year

        low_elec_price = dynamic_energy_price_getter(electricity_df, 'favorable', year)
        high_elec_price = dynamic_energy_price_getter(electricity_df, 'average', year)
        low_hyd_price = dynamic_energy_price_getter(hydrogen_df, 'favorable', year)
        high_hyd_price = dynamic_energy_price_getter(hydrogen_df, 'average', year)
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
                scalar_calc = natural_gas_ref.loc[selected_region, 2020]['value'].max()
                if scalar_calc == 1:
                    price_unit_value = natural_gas_low
                else:
                    price_unit_value = natural_gas_low + ((natural_gas_high - natural_gas_low) * (1 - scalar_calc))
                df_c.loc[row.Index, 'Natural gas'] = resource_consumed * price_unit_value

            if resource == 'Electricity':
                scalar_calc = solar_ref.loc[selected_region, 'practical_potential'].value
                if scalar_calc == 1:
                    price_unit_value = low_elec_price
                else:
                    price_unit_value = low_elec_price + ((high_elec_price - low_elec_price) * (1 - scalar_calc))
                df_c.loc[row.Index, 'Electricity'] = resource_consumed * price_unit_value

            if resource == 'Hydrogen':
                scalar_calc = solar_ref.loc[selected_region, 'practical_potential'].value
                if scalar_calc == 1:
                    price_unit_value = low_hyd_price
                else:
                    price_unit_value = low_hyd_price + ((high_hyd_price - low_hyd_price) * (1 - scalar_calc))
                df_c.loc[row.Index, 'Hydrogen'] = resource_consumed * price_unit_value

        df_c['year'] = year
        df_list.append(df_c)

    combined_df = pd.concat(df_list)
    return combined_df

def create_total_opex(df: pd.DataFrame) -> pd.DataFrame:
    capex_dict = read_pickle_folder(PKL_FOLDER, 'capex_dict', 'df')
    opex_df = capex_dict['other_opex'].reorder_levels(['Year', 'Technology'])
    df_c = df.copy()
    for row in df_c.itertuples():
        year = row.Index[0]
        tech = row.Index[1]
        df_c.loc[year, tech]['cost'] = row.cost + opex_df.loc[year, tech]['value']
    df_c.rename({'cost': 'total_opex'}, axis=1, inplace=True)
    return df_c

def generate_emissions_dataframe(df: pd.DataFrame, year_end: int):

    # S1 emissions covers the Green House Gas (GHG) emissions that a company makes directly
    s1_emissions = read_pickle_folder(PKL_FOLDER, 's1_emissions_factors', 'df')

    # Scope 2 Emissions: These are the emissions it makes indirectly
    # like when the electricity or energy it buys for heating and cooling buildings
    grid_emissivity = read_pickle_folder(PKL_FOLDER, 'grid_emissivity', 'df')

    # S3 emissions: all the emissions associated, not with the company itself,
    # but that the organisation is indirectly responsible for, up and down its value chain.
    final_scope3_ef_df = read_pickle_folder(PKL_FOLDER, 'final_scope3_ef_df', 'df')

    # Carbon Taxes
    carbon_tax = read_pickle_folder(PKL_FOLDER, 'carbon_tax', 'df')

    non_standard_dict_ref = create_emissions_ref_dict(df, TECH_REFERENCE_LIST)

    emissions, carbon = apply_emissions(
        df=df.copy(),
        year_end=year_end,
        s1_emissions_df=s1_emissions,
        s2_emissions_df=grid_emissivity,
        s3_emissions_df=final_scope3_ef_df,
        # carbon_tax_df=carbon_tax,
        non_standard_dict=non_standard_dict_ref,
        scope='1'
    )

    return emissions, carbon

def generate_prices_dataframe(df: pd.DataFrame, year_end: int):

    solar_ref = read_pickle_folder(PKL_FOLDER, 'solar_processed', 'df')
    wind_ref = read_pickle_folder(PKL_FOLDER, 'wind_processed', 'df')
    natural_gas_ref = read_pickle_folder(PKL_FOLDER, 'natural_gas_processed', 'df')

    # Static Energy Prices: 
    static_energy_prices = read_pickle_folder(PKL_FOLDER, 'static_energy_prices', 'df')[['Metric', 'Year', 'Value']]

    # Feedstock prices: Everything else
    feedstock_prices = read_pickle_folder(PKL_FOLDER, 'feedstock_prices', 'df')

    # Commodities data: Ethanol, Charcoal, Plastic Waste
    commodities_df = read_pickle_folder(PKL_FOLDER, 'commodities_df', 'df')

    commodities_dict = commodity_data_getter(commodities_df)
    commodity_dictname_mapper = {'plastic': 'Plastic waste', 'ethanol': 'Ethanol', 'charcoal': 'Charcoal'}
    for key in commodity_dictname_mapper.keys():
        commodities_dict[commodity_dictname_mapper[key]] = commodities_dict.pop(key)

    # Electricity prices
    electricity_minimodel_timeseries = read_pickle_folder(PKL_FOLDER, 'electricity_minimodel_timeseries', 'df')

    # Hydrogen prices
    hydrogen_minimodel_timeseries = read_pickle_folder(PKL_FOLDER, 'hydrogen_minimodel_timeseries', 'df')

    feedstock_dict = {**commodities_dict, **dict(zip(feedstock_prices['Metric'], feedstock_prices['Value']))}

    variable_cost_df = generate_variable_costs(
        df=df.copy(),
        year_end=year_end,
        static_energy_df=static_energy_prices,
        feedstock_dict=feedstock_dict,
        electricity_df=electricity_minimodel_timeseries,
        hydrogen_df=hydrogen_minimodel_timeseries,
        natural_gas_ref=natural_gas_ref,
        solar_ref=solar_ref,
        wind_ref=wind_ref

    )
    variable_cost_df.drop(labels=['value'], axis=1, inplace=True)
    variable_cost_df = variable_cost_df.melt(id_vars=['technology', 'year', 'material_category', 'unit'],var_name=['cost_type'], value_name='cost')
    variable_cost_df['cost'] = variable_cost_df['cost'].replace('', 0)

    return variable_cost_df

def price_and_emissions_flow(serialize_only: bool = False):
    year_end = 2050
    business_cases_summary = read_pickle_folder(PKL_FOLDER, 'standardised_business_cases', 'df')
    business_cases_summary_c = business_cases_summary.loc[business_cases_summary['material_category'] != 0].copy().reset_index(drop=True)
    emissions_df = business_cases_summary_c.copy()
    emissions, carbon = generate_emissions_dataframe(business_cases_summary_c, year_end)
    emissions_s1_summary = emissions[emissions['scope'] == 'S1']
    s1_summary_df = emissions_s1_summary[['technology', 'year', 'emissions']].groupby(by=['year', 'technology']).sum()
    em_exc_ref_dict = create_emissions_ref_dict(emissions_df, TECH_REFERENCE_LIST)
    s1_summary_df = full_emissions(s1_summary_df, em_exc_ref_dict, TECH_REFERENCE_LIST)
    emissions_s2_summary = emissions[emissions['scope'] == 'S2'][['technology', 'year', 'emissions']].groupby(by=['year', 'technology']).sum()
    emissions_s3_summary = emissions[emissions['scope'] == 'S3'][['technology', 'year', 'emissions']].groupby(by=['year', 'technology']).sum()
    variable_costs = generate_prices_dataframe(business_cases_summary_c, year_end)
    cost_tech_summary = variable_costs.groupby(by=['year', 'technology']).sum().sort_values(by=['year'])
    opex_sheet = create_total_opex(cost_tech_summary)

    if serialize_only:
        serialize_df(s1_summary_df, PKL_FOLDER, 'calculated_s1_emissions')
        serialize_df(emissions_s2_summary, PKL_FOLDER, 'calculated_s2_emissions')
        serialize_df(emissions_s3_summary, PKL_FOLDER, 'calculated_s3_emissions')
        serialize_df(cost_tech_summary, PKL_FOLDER, 'calculated_variable_costs')
        serialize_df(opex_sheet, PKL_FOLDER, 'calculated_total_opex')
        return
    return {
        's1_calculations': s1_summary_df,
        's2_calculations': emissions_s2_summary,
        's3_calculations': emissions_s3_summary,
        'variable_costs': cost_tech_summary,
        'opex_calculations': opex_sheet
    }
price_and_emissions_flow(serialize_only=True)