"""Runs the data loading scripts"""

from mppsteel.data_loading.data_import import load_data
from mppsteel.minimodels.hydrogen_minimodel import generate_hydrogen_timeseries
from mppsteel.minimodels.timeseries_generator import generate_timeseries
from mppsteel.data_loading.business_case_standardisation import (
    standardise_business_cases,
)
from mppsteel.data_loading.natural_resource_data_interface import (
    natural_resource_preprocessor,
)
from mppsteel.data_loading.steel_plant_formatter import steel_plant_preprocessor
from mppsteel.data_loading.country_reference import create_country_ref
from mppsteel.data_loading.data_interface import (
    create_capex_opex_dict,
    generate_preprocessed_emissions_data,
)
from mppsteel.model.prices_and_emissions_tables import price_and_emissions_flow
from mppsteel.model.capex_switching import create_capex_timeseries
from mppsteel.model.tco_and_emissions import calculate_emissions, calculate_tco

from mppsteel.model_config import DISCOUNT_RATE

if __name__ == "__main__":
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
    calculate_tco(year_end=2070, output_type="summary", serialize_only=True)

    # Create Emissions table
    calculate_emissions(year_end=2070, output_type="summary", serialize_only=True)
