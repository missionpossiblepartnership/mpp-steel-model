"""Runs the data loading scripts"""

from data_import import load_data
from hydrogen_minimodel import generate_hydrogen_timeseries
from timeseries_generator import generate_timeseries
from business_case_standardisation import standardise_business_cases
from natural_resource_data_interface import natural_resource_preprocessor
from steel_plant_formatter import steel_plant_preprocessor
from country_reference import create_country_ref
from data_interface import create_capex_opex_dict, generate_preprocessed_emissions_data

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
