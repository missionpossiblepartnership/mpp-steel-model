from mppsteel.data_loading.business_case_standardisation import (
    standardise_business_cases,
)

from mppsteel.model.solver import (
    solver_flow,
)

from mppsteel.model.investment_cycles import investment_cycle_flow

from mppsteel.model.variable_plant_cost_archetypes import generate_variable_plant_summary

from mppsteel.model_config import MODEL_YEAR_END

from mppsteel.results.investments import investment_results

from mppsteel.results.production import production_results_flow

from mppsteel.data_loading.steel_plant_formatter import steel_plant_processor

# solver_flow(2025, serialize_only=True)

# standardise_business_cases(serialize_only=True)

steel_plant_processor(serialize_only=True, remove_non_operating_plants=True)