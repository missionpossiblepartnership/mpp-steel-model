"""Formats Price & Emissions Model Data and defines getter functions"""

import pandas as pd

from mppsteel.model_config import PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE

from mppsteel.utility.utils import (
    get_logger,
    timer_func,
    read_pickle_folder,
    serialize_file,
    match_country
)

from mppsteel.utility.reference_lists import EU_COUNTRIES

from mppsteel.data_loading.reg_steel_demand_formatter import get_countries_from_group, get_unique_countries

# Create logger
logger = get_logger("Price & Emissions Formatter")

LATEST_FILES = {
    'power': 'Power model.xlsx',
    'ccus': 'CCUS Model.xlsx',
    'hydrogen': 'H2 Model.xlsx',
}

OUTPUT_SHEETS = {
    'power': ['GridPrice', 'RESPrice', 'GridEmissions'],
    'ccus': ['Transport', 'Storage'],
    'hydrogen': ['Prices', 'Emissions']
}

INDEX_DICT = {
    'power': ['Region', 'year', 'Customer'],
    'hydrogen': ['Region', 'year'],
    'bio': ['Region', 'year'],
    'ccus': ['Region']   
}

RE_DICT = {
    'solar': 'Price of onsite solar',
    'wind': 'Price of onsite wind',
    'wind_and_solar': 'Price of onsite wind + solar',
    'gas': 'Price of onsite gas + ccs'
}

def get_ccus_country_groups(country_ref: pd.DataFrame):
    
    other_eurasia_ref = [
        'Afghanistan',
        'Armenia',
        'Azerbaijan',
        'Belarus',
        'Georgia',
        'Kazakhstan',
        'Kyrgyzstan',
        'Mongolia'
    ]

    dynamic_asia_ref = [
        'Taiwan' ,
        'Hong Kong',
        'Indonesia',
        'Malaysia',
        'Philippines',
        'Singapore',
        'Thailand'
    ]

    middle_east = get_countries_from_group(country_ref, 'RMI Model Region', 'Middle East')
    africa = get_countries_from_group(country_ref, 'Continent', 'Africa')
    other_latin_america = get_countries_from_group(country_ref, 'WSA Group Region', 'Central and South America', ['Mexico', 'Brazil'])
    cis = get_countries_from_group(country_ref, 'WSA Group Region', 'CIS')
    other_eurasia = [match_country(country) for country in other_eurasia_ref]
    dynamic_asia = [match_country(country) for country in dynamic_asia_ref]
    big_asia = [match_country(country) for country in ['China', 'Japan', 'Korea', 'Indonesia', 'India']]
    not_other_asia = dynamic_asia + big_asia + middle_east + cis
    other_asia = list(set(get_countries_from_group(country_ref, 'Continent', 'Asia')).difference(set(not_other_asia)))
    central_and_south_americas = get_countries_from_group(country_ref, 'WSA Group Region', 'Central and South America')
    europe = get_countries_from_group(country_ref, 'Continent', 'Europe')

    group_dict = {
        'Middle East': middle_east,
        'Africa': africa,
        'Other Latin America': other_latin_america,
        'Other Eurasia ': other_eurasia,
        'Dynamic Asia': dynamic_asia,
        'Other East Asia': other_asia,
        'South and Central Americas': central_and_south_americas,
        'Europe': europe,
        'EU': EU_COUNTRIES,
    }

    return group_dict

def country_match_ref(df: pd.DataFrame, country_group_dict: dict, default_country: str = None):
    df_c = df.copy()
    countries = df_c['Region'].unique()
    country_dict = {}
    for country in countries:
        if country in country_group_dict.keys():
            country_dict[country] = country_group_dict[country]
        elif country == 'Global':
            country_dict[country] = ['GBL']
        elif pd.isna(country):
            country_dict[country] = ''
        else:
            country_match = match_country(country)
            if country_match is None:
                print(f'Country not found {country}')
                if default_country:
                    print(f'Reassigning to {default_country}')
                    country_dict[country] = [default_country]
                else:
                    print(f'Leaving blank')
                    country_dict[country] = ['']
            else:
                country_dict[country] = [country_match]
    df_c['country_code'] = df_c['Region'].apply(lambda x: country_dict[x])
    return df_c

def format_model_data(model_name: str, data_dict: dict, sheet_dict: dict, index_dict: dict, country_ref: pd.DataFrame):
    logger.info(f'Formatting the {model_name} model')
    dict_obj = {}
    country_group_dict = get_ccus_country_groups(country_ref)
    for sheet in sheet_dict[model_name]:
        temp_df = data_dict[sheet].copy()
        temp_df.columns = [col.strip() if isinstance(col, str) else col for col in temp_df.columns ]
        temp_df = country_match_ref(temp_df, country_group_dict)
        if model_name in ['power', 'hydrogen', 'bio']:
            years = [year_col for year_col in temp_df.columns if isinstance(year_col, int)]
            temp_df = temp_df.melt(id_vars=set(temp_df.columns).difference(set(years)), var_name='year')
        temp_df.set_index(index_dict[model_name], inplace=True)
        dict_obj[sheet] = temp_df
    return dict_obj

def full_model_getter_flow(model_name: str, country_ref: pd.DataFrame):
    logger.info(f'Creating the formatted model for {model_name}')
    model_pickle_file = f'{model_name}_model'
    data_dict = read_pickle_folder(PKL_DATA_IMPORTS, model_pickle_file, 'df')
    return format_model_data(model_name, data_dict, OUTPUT_SHEETS, INDEX_DICT, country_ref)

@timer_func
def format_pe_data(serialize_only: bool = False):
    logger.info(f'Initiating fulll format flow for all models')
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, 'country_ref', 'df')
    power = full_model_getter_flow('power', country_ref)
    hydrogen = full_model_getter_flow('hydrogen', country_ref)
    ccus = full_model_getter_flow('ccus', country_ref)

    data_dict = {
        'power': power,
        'hydrogen': hydrogen,
        'ccus': ccus
    }
    if serialize_only:
        serialize_file(power, PKL_DATA_INTERMEDIATE, "power_model_formatted")
        serialize_file(hydrogen, PKL_DATA_INTERMEDIATE, "hydrogen_model_formatted")
        serialize_file(ccus, PKL_DATA_INTERMEDIATE, "ccus_model_formatted")

    return data_dict

def power_data_getter(
    df_dict: dict, data_type: str, year: int, country_code: str,
    re_dict: dict = {}, re_type: str = '',
    default_country: str = 'USA', grid_scenario: str = 'Central',
    cost_scenario: str = 'Baseline', customer: str = 'Commercial'
    ):
    # map data_type to df_dict keys
    data_type_mapper = dict(zip(['grid', 'renewable', 'emissions'], OUTPUT_SHEETS['power']))

    # subset the dict_object
    df_c = df_dict[data_type_mapper[data_type]].copy()

    # define country list based on the data_type
    country_list = get_unique_countries(df_c['country_code'].values)

    # Cap year at 2050
    if year > 2050:
        year = 2050

    # Apply subsets
    df_c = df_c.xs((year, customer), level=['year', 'Customer'])
    df_c.reset_index(drop=True, inplace=True)
    # Grid scenarios: Central, Accelerated, All
    # Cost scenarios: Baseline, Min, Max, All
    df_c = df_c[(df_c['Grid scenario'] == grid_scenario) & (df_c['Cost scenario'] == cost_scenario)]

    if data_type == 'renewable':
        df_c = df_c[df_c['Captive power source'] == re_dict[re_type]]

    # Apply country check and use default
    if country_code in country_list:
        df_c = df_c[df_c['country_code'].str.contains(country_code, regex=False)]
    else:
        df_c = df_c[df_c['country_code'].str.contains(default_country, regex=False)]
    # Return the value figure
    return df_c.value.values[0]

def hydrogen_data_getter(
    df_dict: dict, data_type: str, year: int, country_code: str,
    default_country: str = 'USA', variable: str = None, 
    cost_scenario: str = 'Baseline', prod_scenario: str = 'Utility plant, grid'
    ):
    # map data_type to df_dict keys
    data_type_mapper = {
        'prices': 'Prices',
        'emissions': 'Emissions'
    }

    # subset the dict_object
    df_c = df_dict[data_type_mapper[data_type]].copy()

    # define country list based on the data_type
    country_list = get_unique_countries(df_c['country_code'].values)

    # Cap year at 2050
    if year > 2050:
        year = 2050

    # Apply subsets
    df_c = df_c.xs(year, level='year')
    df_c.reset_index(drop=True, inplace=True)
    # Variables: 'H2 price', 'Electrolyser-related H2 cost component', 'Cost of energy ', 'Total Other costs', 'Total price premium '
    # Production: 'Utility plant, dedicated VREs', 'On-site, dedicated VREs', 'On-site, grid', 'Utility plant, grid'
    # Cost scenarios: Baseline, Min, Max, All
    df_c = df_c[
        (df_c['Cost scenario'] == cost_scenario) & (df_c['Production scenario'] == prod_scenario)]
    
    if (data_type=='prices') and variable:
        df_c = df_c[(df_c['Variable'] == variable)]
    elif (data_type=='prices') and not variable:
        df_c = df_c[(df_c['Variable'] == 'Total price premium ')]
        
    # Apply country check and use default
    if country_code in country_list:
        df_c = df_c[df_c['country_code'].str.contains(country_code, regex=False)]
    else:
        df_c = df_c[df_c['country_code'].str.contains(default_country, regex=False)]
    # Return the value figure
    return df_c.value.values[0]

def ccus_data_getter(
    df_dict: dict, data_type: str, country_code: str,
    default_country: str = 'GBL', 
    transport_type: str = 'Onshore Pipeline', 
    cost_scenario: str = 'BaseCase',
    storage_location: str = 'Onshore',
    storage_type: str = 'Saline aquifers',
    reusable_lw = 'No'
    ):
    # map data_type to df_dict keys
    data_type_mapper = {
        'transport': 'Transport',
        'storage': 'Storage',
    }

    # subset the dict_object
    df_c = df_dict[data_type_mapper[data_type]].copy()

    # define country list based on the data_type
    country_list = get_unique_countries(df_c['country_code'].values)

    value_col = 'Costs -  capacity 5'
    # Apply country check and use default
    if country_code in country_list:
        df_c = df_c[df_c['country_code'].str.contains(country_code, regex=False)]
    else:
        df_c = df_c[df_c['country_code'].str.contains(default_country, regex=False)]

    if data_type == 'capacity':
        return df_c['Capacity'].values[0]

    if data_type == 'transport':
        # Apply subsets
        df_c.reset_index(drop=True, inplace=True)
        # Transport Type: 'Onshore Pipeline', 'Offshore Pipeline', 'Shipping'
        # Cost scenarios: 'BaseCase', 'Low'
        df_c = df_c[
            (df_c['Cost Estimate'] == cost_scenario) & (df_c['Transport Type'] == transport_type)]

        # Return the transport node costs
        return tuple(df_c[['Transport costs _Node 1','Transport costs _Node 2', 'Transport costs _Node 3']].values[0])

    if data_type == 'storage':
        # Apply subsets
        df_c.reset_index(drop=True, inplace=True)
        # Storage Location: 'Onshore' 'Offshore'
        # Storage type: 'Depleted O&G field', 'Saline aquifers'
        # reusable_lw: Yes or No
        df_c = df_c[(df_c['Storage location'] == storage_location) & (df_c['Storage type'] == storage_type) & (df_c['Reusable legacy wells'] == reusable_lw)]

        return df_c[value_col].values[0]
