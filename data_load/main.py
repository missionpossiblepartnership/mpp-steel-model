"""Runs the data loading scripts"""

from data_import import load_data
from hydrogen_minimodel import generate_hydrogen_timeseries
from timeseries_generator import generate_timeseries
from business_case_standardisation import standardise_business_cases
from natural_resource_data_interface import natural_resource_preprocessor
from steel_plant_formatter import steel_plant_preprocessor
from country_reference import create_country_ref
from data_interface import create_capex_opex_dict, generate_preprocessed_emissions_data
from prices_and_emissions_tables import price_and_emissions_flow
from capex_switching import create_capex_timeseries
from tco_and_emissions import calculate_emissions, calculate_tco

from model_config import DISCOUNT_RATE

if __name__ == '__main__':
    # Load all data
    load_data(serialize_only=True)

    # Run hydrogen minimodel
    generate_hydrogen_timeseries(serialize_only=True)

    # Run biomass, carbon tax and electricity models
    generate_timeseries(serialize_only=True)

    # Run Business case standardisation
    standardise_business_cases(serialize_only=True)

    # Run natural resource preprocessor
    natural_resource_preprocessor(serialize_only=True)

    # Process Steel plants
    steel_plant_preprocessor(serialize_only=True)

    # Run Country Reference
    create_country_ref(serialize_only=True)

    # Create Capex Opex dict
    create_capex_opex_dict(serialize_only=True)

    # Create preprocessed emissions dataframes
    generate_preprocessed_emissions_data(serialize_only=True)

    # Create emissions, price and opex tables
    price_and_emissions_flow(serialize_only=True)

    # Create capex tables
    create_capex_timeseries(serialize_only=True)

    # Create TCO table
    calculate_tco(
    interest_rate=DISCOUNT_RATE, year_end=2070, output_type='summary', serialize_only=True)

    # Create Emissions table
    calculate_emissions(year_end=2070, output_type='summary', serialize_only=True)
