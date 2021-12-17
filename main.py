"""Runs the data loading scripts"""
import time
from mppsteel.utility.utils import get_logger

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
from mppsteel.model.emissions import calculate_emissions
from mppsteel.model.investment_cycles import investment_cycle_flow
from mppsteel.model.variable_plant_cost_archetypes import generate_variable_plant_summary
from mppsteel.model.solver import solver_flow
from mppsteel.results.production import production_results_flow
from mppsteel.results.investments import investment_results

from mppsteel.model_config import MODEL_YEAR_END

logger = get_logger("Main Model Code")

if __name__ == "__main__":
    starttime = time.time()

    # Load all data
    load_data(serialize_only=True)

    # Run hydrogen minimodel
    generate_hydrogen_timeseries(serialize_only=True)

    # Run biomass, carbon tax and electricity models
    generate_timeseries(serialize_only=True)

    # Run Business case standardisation
    standardise_business_cases(serialize_only=True)

    # Run natural resource preprocessor
    # natural_resource_preprocessor(serialize_only=True)

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

    # Create Emissions table
    calculate_emissions(year_end=MODEL_YEAR_END, output_type="summary", serialize_only=True)

    # Create Investments table
    investment_cycle_flow(serialize_only=True)

    # Create variable costs reference table
    generate_variable_plant_summary(serialize_only=True)

    # Create Solver dictionary
    solver_flow(year_end=MODEL_YEAR_END, serialize_only=True)

    # Create Investments Table
    investment_results(serialize_only=True)

    # Create Production Table
    production_results_flow(serialize_only=True)

    endtime = time.time()
    logger.info(f'Total runtime is {endtime - starttime:0.4f} seconds')