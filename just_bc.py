from mppsteel.data_loading import pe_model_formatter
from mppsteel.data_loading.business_case_standardisation import (
    standardise_business_cases,
)

from mppsteel.model.solver import (
    solver_flow,
)

from mppsteel.model.investment_cycles import investment_cycle_flow

from mppsteel.model.variable_plant_cost_archetypes import generate_variable_plant_summary

from mppsteel.model_config import MODEL_YEAR_END, PKL_DATA_INTERMEDIATE

from mppsteel.results.investments import investment_results

from mppsteel.results.production import production_results_flow

from mppsteel.data_loading.steel_plant_formatter import steel_plant_processor

from mppsteel.utility.utils import pickle_to_csv, TIME_CONTAINER, read_pickle_folder

from mppsteel.data_loading.reg_steel_demand_formatter import get_steel_demand, steel_demand_getter

from mppsteel.data_loading.pe_model_formatter import format_pe_data, power_data_getter, hydrogen_data_getter, ccus_data_getter, RE_DICT

# solver_flow(2025, serialize_only=True)

# standardise_business_cases(serialize_only=True)

# generate_variable_plant_summary(serialize_only=True)
# print(TIME_CONTAINER.return_time_container())

# steel_demand_f = get_steel_demand(serialize_only=False)
# print(steel_demand_getter(steel_demand_f, 2020, 'BAU', 'crude', 'TWN'))

#data_dict = format_pe_data(serialize_only=True)

power_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'power_model_formatted', 'df')
hydrogen_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'hydrogen_model_formatted', 'df')
ccus_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, 'ccus_model_formatted', 'df')

print(power_data_getter(power_dict, 'renewable', 2020, 'IND', RE_DICT, 'wind_and_solar'))
print(hydrogen_data_getter(hydrogen_dict, 'emissions', 2020, 'IND'))
print(ccus_data_getter(ccus_dict, 'storage', 'IND'))
print(ccus_data_getter(ccus_dict, 'transport', 'IND'))