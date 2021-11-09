"""Manages data imports"""
# For Data Manipulation
import pickle
import pandas as pd

# For logger and units dict
from utils import get_logger, read_pickle_folder

# Create logger
logger = get_logger('Data Import')

def extract_data(data_path: str, filename: str, ext: str, sheet: int=0) -> pd.DataFrame:
    """Extracts data from excel or csv files based on input parameters

    Args:
        data_path (str): path where data files are stored
        filename (str): name of file to extract (without extension)
        ext (str): extension of the file to extract
        sheet (int, optional): Number of the sheet to extract. For xlsx (workbook) files only. - . Defaults to 0.

    Returns:
        DataFrame: A dataframe of the data file
    """
    # Full path of the file
    full_filename = fr'{data_path}/{filename}.{ext}'
    # If else logic that determines which pandas function to call based on the extension
    logger.info(f'|| Extracting file {filename}.{ext}')
    if ext == 'xlsx':
        return pd.read_excel(full_filename, sheet_name=sheet)
    elif ext == 'csv':
        return pd.read_csv(full_filename)


def replace_rows(df: pd.DataFrame, header_row: int) -> pd.DataFrame:
    """For WSA trade data, this function replaces the column names with the appropriate row.

    Args:
        df (DataFrame): The unformatted DataFrame
        header_row (int): The row that the DataFrame should start from.

    Returns:
        DataFrame: A reformatted DataFrame
    """
    df_c = df.copy()
    new_header = df_c.iloc[header_row] # grab the first row for the header
    df_c = df[header_row+1:] # take the data less the header row
    df_c.columns = new_header # set the header row as the df header
    return df_c

def df_serializer(data_path: str, data_dict: dict):
    """Iterate through each df and store the file as pickle or feather. Does not return any object.

    Args:
        data_ref (dict): A data dictionary where the DataFrames are stored
        data_path (str): The path where the pickle files will be stored
    """
    logger.info(f'||| Serializing each df to a pickle file {data_path}')
    for df_name in data_dict.keys():
        with open(f'{data_path}/{df_name}.pickle', 'wb') as f:
            # Pickle the 'data' dictionary using the highest protocol available.
            logger.info(f'* Saving df {df_name} to pickle')
            pickle.dump(data_dict[df_name], f, pickle.HIGHEST_PROTOCOL)

# Define Data Path
IMPORT_DATA_PATH = "./import_data"
PKL_FOLDER = "./pkl_data"


# Import capex numbers
greenfield_capex = extract_data(IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 0)
brownfield_capex = extract_data(IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 1)
other_opex = extract_data(IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 2)

# Import ccs co2 capacity numbers
ccs_co2 = extract_data(IMPORT_DATA_PATH, "CO2 CCS Capacity", "csv")

# Import country reference
country_ref = extract_data(IMPORT_DATA_PATH, "Country Reference", "xlsx").fillna("")

# Import emissions factors
emissions_factors = extract_data(IMPORT_DATA_PATH, "Emissions Factors", "xlsx")

# Import static energy prices
static_energy_prices = extract_data(IMPORT_DATA_PATH, "Energy Prices - Static", "xlsx")

# Import feedstock prices
feedstock_prices = extract_data(IMPORT_DATA_PATH, "Feedstock Prices", "xlsx")

# Import grid emissivity
grid_emissivity = extract_data(IMPORT_DATA_PATH, "Grid Emissivity", "xlsx")

# Import steel demand
steel_demand = extract_data(IMPORT_DATA_PATH, "Steel Demand", "csv")

# Import steel plant data
steel_plants = extract_data(IMPORT_DATA_PATH, "Steel Plant Data", "csv")

# Import technology availability
tech_availability = extract_data(IMPORT_DATA_PATH, "Technology Availability", "csv")

# Import WSA data
crude_regional_shares = extract_data(IMPORT_DATA_PATH, "WSA World Steel In Figures 2021", "xlsx", 0)
crude_regional_real = extract_data(IMPORT_DATA_PATH, "WSA World Steel In Figures 2021", "xlsx", 1)
iron_ore_pig_iron = extract_data(IMPORT_DATA_PATH, "WSA World Steel In Figures 2021", "xlsx", 2)

crude_trade = replace_rows(
    extract_data(IMPORT_DATA_PATH, "WSA World Steel In Figures 2021", "xlsx", 3), 1).fillna(0)
iron_ore_trade = replace_rows(
    extract_data(IMPORT_DATA_PATH, "WSA World Steel In Figures 2021", "xlsx", 4), 1).fillna(0)
scrap_trade = replace_rows(
    extract_data(IMPORT_DATA_PATH, "WSA World Steel In Figures 2021", "xlsx", 5), 1).fillna(0)

# Define a data dictionary
df_dict = {
    "greenfield_capex" : greenfield_capex, 
    "brownfield_capex" : brownfield_capex,
    "other_opex" : other_opex,
    "ccs_co2" : ccs_co2,
    "country_ref" : country_ref,
    "emissions_factors" : emissions_factors,
    "static_energy_prices" : static_energy_prices,
    "feedstock_prices" : feedstock_prices,
    "grid_emissivity" : grid_emissivity,
    "steel_demand" : steel_demand,
    "steel_plants" : steel_plants,
    "tech_availability" : tech_availability,
    "crude_regional_real" : crude_regional_real,
    "crude_regional_shares" : crude_regional_shares,
    "iron_ore_pig_iron" : iron_ore_pig_iron,
    "crude_trade" : crude_trade,
    "iron_ore_trade" : iron_ore_trade,
    "scrap_trade" : scrap_trade
}

# Turn dataframes into pickle files
df_serializer(PKL_FOLDER, df_dict)

# Unload dataframes from pickle files
pkl_dict = read_pickle_folder(PKL_FOLDER)