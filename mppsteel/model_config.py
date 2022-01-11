"""Config file for model parameters"""

# Define Data Path
CORE_DATA_PATH = "mppsteel/data"
LOG_PATH = "logs/"
IMPORT_DATA_PATH = f"{CORE_DATA_PATH}/import_data"
OUTPUT_FOLDER = f"{CORE_DATA_PATH}/output_data"
PKL_FOLDER = f"{CORE_DATA_PATH}/pkl_data"
PKL_DATA_IMPORTS = f"{PKL_FOLDER}/imported_data"
PKL_DATA_INTERMEDIATE = f"{PKL_FOLDER}/intermediate_data"
PKL_DATA_FINAL = f"{PKL_FOLDER}/final_data"

PE_MODEL_FILENAME_DICT = {
    'power': 'Power Model.xlsx',
    'ccus': 'CCUS Model.xlsx',
    'hydrogen': 'H2 Model.xlsx',
}

PE_MODEL_SHEETNAME_DICT = {
    'power': ['GridPrice', 'GridEmissions', 'RESPrice'],
    'ccus': ['Transport', 'Storage'],
    'hydrogen': ['Prices', 'Emissions']
}

MODEL_YEAR_START = 2020
MODEL_YEAR_END = 2050

BIOMASS_AV_TS_START_YEAR = 2020
BIOMASS_AV_TS_END_YEAR = 2050
BIOMASS_AV_TS_END_VALUE = 2000

CARBON_TAX_START_YEAR = 2020
CARBON_TAX_END_YEAR = 2050
CARBON_TAX_START_VALUE = 0
CARBON_TAX_END_VALUE = 210

ELECTRICITY_PRICE_START_YEAR = 2020
ELECTRICITY_PRICE_MID_YEAR = 2035
ELECTRICITY_PRICE_END_YEAR = 2050

HYDROGEN_PRICE_START_YEAR = 2020
HYDROGEN_PRICE_END_YEAR = 2050

EMISSIONS_FACTOR_SLAG = 0.55
ENERGY_DENSITY_MET_COAL = 28  # [MJ/kg]

DISCOUNT_RATE = 0.07
EUR_USD_CONVERSION = 0.877
STEEL_PLANT_LIFETIME = 40  # years
INVESTMENT_CYCLE_LENGTH = 20  # years
INVESTMENT_CYCLE_VARIANCE = 3 # years
INVESTMENT_OFFCYCLE_BUFFER_TOP = 3
INVESTMENT_OFFCYCLE_BUFFER_TAIL = 8
NET_ZERO_TARGET = 2050
NET_ZERO_VARIANCE = 3 # years

SWITCH_CAPEX_DATA_POINTS = {
    "2020": 319.249187119815,
    "2030": 319.249187119815,
    "2050": 286.218839300307,
}

CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION = 0.95
CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION = 0.6
SCRAP_OVERSUPPLY_CUTOFF_FOR_NEW_EAF_PLANT_DECISION = 0.15
AVERAGE_LEVEL_OF_CAPACITY = 0.95

TCO_RANK_2_SCALER = 1.3
TCO_RANK_1_SCALER = 1.1
ABATEMENT_RANK_2 = 2.37656461606311 # Switching from Avg BF-BOF to BAT BF-BOF+CCUS
ABATEMENT_RANK_3 = 0.932690243851946 # Switching from Avg BF-BOF to BAT BF-BOF_bio PCI

GREEN_PREMIUM_MIN_PCT = 0.01
GREEN_PREMIUM_MAX_PCT = 0.05

RESULTS_REGIONS_TO_MAP = ['wsa_region', 'continent', 'region']

COST_SCENARIO_MAPPER = {
    'low': 'Min',
    'average': 'Baseline',
    'high': 'Max',
}

STEEL_DEMAND_SCENARIO_MAPPER = {
    'bau': 'BAU',
    'high': 'High Circ',
    'average': 'average'
}

TECH_SWITCH_SCENARIOS = {
    'max_abatement': {'tco': 0, 'emissions': 1},
    'lowest_cost': {'tco': 1, 'emissions': 0},
    'equal_weight': {'tco': 0.5, 'emissions': 0.5},
}

GREEN_PREMIUM_SCENARIOS = {
    'off': (0, 0),
    'low': (0.01, 0.03),
    'average': (0.025, 0.05),
    'high': (0.05, 0.08),
}

GRID_DECARBONISATION_SCENARIOS = {
    'high': 'Accelerated',
    'low': 'Central',
}

true_false = [True, False]
low_avg_high = list(COST_SCENARIO_MAPPER.keys())

SCENARIO_OPTIONS = {
    'tech_moratorium': true_false,
    'carbon_tax': true_false,
    'green_premium_scenario': GREEN_PREMIUM_SCENARIOS.keys(),
    'electricity_cost_scenario': low_avg_high,
    'grid_scenario': GRID_DECARBONISATION_SCENARIOS.keys(),
    'hydrogen_cost_scenario': low_avg_high,
    'steel_demand_scenario': STEEL_DEMAND_SCENARIO_MAPPER.keys(),
    'tech_switch_scenario': TECH_SWITCH_SCENARIOS.keys()
}

DEFAULT_SCENARIO = {
    'tech_moratorium': True, # bool
    'carbon_tax': False, # bool
    'green_premium': True, # bool
    'electricity_cost_scenario': 'average', # low / average / high
    'grid_scenario': 'high', # low / high
    'hydrogen_cost_scenario': 'average', # low / average / high
    'steel_demand_scenario': 'average', # bau / average / high
    'tech_switch_scenario': 'equal_weight', # max_abatement / lowest_cost / equal_weight / default
}
