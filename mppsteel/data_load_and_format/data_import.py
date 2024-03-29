"""Manages data imports"""
# For Data Manipulation
import pandas as pd

# For logger and units dict
from mppsteel.utility.file_handling_utility import extract_data, serialize_df_dict
from mppsteel.utility.function_timer_utility import timer_func

# Get model parameters
from mppsteel.config.model_config import IMPORT_DATA_PATH, PKL_DATA_IMPORTS
from mppsteel.utility.log_utility import get_logger


logger = get_logger(__name__)

# POWER & ENERGY FILE & TAB NAMES
PE_MODEL_FILENAME_DICT = {
    "power": "Power Model.xlsx",
    "hydrogen": "H2 Model.xlsx",
    "bio": "Bio Model.xlsx",
    "ccs": "CCS Model.xlsx",
}

PE_MODEL_SHEETNAME_DICT = {
    "power": ["GridPrice", "GridEmissions", "RESPrice"],
    "hydrogen": ["Prices", "Emissions"],
    "bio": ["Feedstock_Prices", "Biomass_constraint"],
    "ccs": ["Transport", "Storage", "Capture", "Constraint"],
}


def replace_rows(df: pd.DataFrame, header_row: int) -> pd.DataFrame:
    """For WSA trade data, this function replaces the column names with the appropriate row.

    Args:
        df (DataFrame): The unformatted DataFrame
        header_row (int): The row that the DataFrame should start from.

    Returns:
        DataFrame: A reformatted DataFrame
    """
    df_c = df.copy()
    # grab the first row for the header
    new_header = df_c.iloc[header_row]
    # take the data less the header row
    df_c = df[header_row + 1 :]
    # set the header row as the df header
    df_c.columns = new_header
    return df_c


def get_pe_model_data(model_name: str) -> pd.DataFrame:
    """Extracts individual sheets from the Price & Emissions shared assumptions models.

    Args:
        model_name (str): The name of the excel model to be extracted.

    Returns:
        pd.DataFrame: A DataFrame of the specific sheet you have extracted.
    """

    def get_path(model_name: str, filenames_dict: dict) -> str:
        return f"{IMPORT_DATA_PATH}/{filenames_dict[model_name]}"

    datapath = get_path(model_name, PE_MODEL_FILENAME_DICT)
    return pd.read_excel(datapath, sheet_name=PE_MODEL_SHEETNAME_DICT[model_name])


@timer_func
def load_import_data(serialize: bool = False) -> dict:
    """Loads all the data you specify when the function is called.

    Args:
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary with all the data from the imported files.
    """
    # Import capex numbers
    greenfield_capex = extract_data(
        IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 0
    )
    brownfield_capex = extract_data(
        IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 1
    )
    other_opex = extract_data(IMPORT_DATA_PATH, "CAPEX OPEX Per Technology", "xlsx", 2)
    # Import ccs co2 capacity numbers
    ccs_co2 = extract_data(IMPORT_DATA_PATH, "CO2 CCU Capacity", "csv")
    # Import country reference
    country_ref = extract_data(IMPORT_DATA_PATH, "Country Reference", "xlsx").fillna("")
    # Import emissions factors
    s1_emissions_factors = extract_data(
        IMPORT_DATA_PATH, "Scope 1 Emissions Factors", "xlsx"
    )
    # Import scope 3 EF data
    s3_emissions_factors_1 = extract_data(
        IMPORT_DATA_PATH, "Scope 3 Emissions Factors", "xlsx", 0
    )
    s3_emissions_factors_2 = pd.read_excel(
        f"{IMPORT_DATA_PATH}/Scope 3 Emissions Factors.xlsx",
        sheet_name=1,
        skiprows=1,
    )
    # Import static energy prices
    static_energy_prices = extract_data(
        IMPORT_DATA_PATH, "Energy Prices - Static", "xlsx"
    )
    # Import feedstock prices
    feedstock_prices = extract_data(IMPORT_DATA_PATH, "Feedstock Prices", "xlsx")
    # Import steel plant data
    steel_plants = extract_data(IMPORT_DATA_PATH, "Steel Plant Data Anon", "xlsx")
    # Import technology availability
    tech_availability = extract_data(IMPORT_DATA_PATH, "Technology Availability", "csv")
    # Import Commodities Data
    plastic_prices = extract_data(IMPORT_DATA_PATH, "Plastic Prices", "csv")
    # Import Regional Steel Demand Data
    regional_steel_demand = extract_data(
        IMPORT_DATA_PATH, "Regional Steel Demand", "csv"
    )
    # WSA data
    wsa_production = extract_data(IMPORT_DATA_PATH, "WSA Production 2020", "csv")
    # Fossil Fuel Data
    fossil_fuel_model = extract_data(IMPORT_DATA_PATH, "Fossil Fuel Model", "csv")
    # Import Price and Emissions Models
    power_model = get_pe_model_data("power")
    hydrogen_model = get_pe_model_data("hydrogen")
    bio_model = get_pe_model_data("bio")
    ccs_model = get_pe_model_data("ccs")
    technology_business_cases = extract_data(
        IMPORT_DATA_PATH, "Technology Business Cases", "csv"
    )

    # Define a data dictionary
    df_dict = {
        "greenfield_capex": greenfield_capex,
        "brownfield_capex": brownfield_capex,
        "other_opex": other_opex,
        "ccs_co2": ccs_co2,
        "country_ref": country_ref,
        "s1_emissions_factors": s1_emissions_factors,
        "static_energy_prices": static_energy_prices,
        "feedstock_prices": feedstock_prices,
        "regional_steel_demand": regional_steel_demand,
        "steel_plants": steel_plants,
        "tech_availability": tech_availability,
        "s3_emissions_factors_1": s3_emissions_factors_1,
        "s3_emissions_factors_2": s3_emissions_factors_2,
        "technology_business_cases": technology_business_cases,
        "plastic_prices": plastic_prices,
        "power_model": power_model,
        "hydrogen_model": hydrogen_model,
        "bio_model": bio_model,
        "ccs_model": ccs_model,
        "wsa_production": wsa_production,
        "fossil_fuel_model": fossil_fuel_model,
    }

    if serialize:
        # Turn dataframes into pickle files
        serialize_df_dict(PKL_DATA_IMPORTS, df_dict)
    return df_dict
