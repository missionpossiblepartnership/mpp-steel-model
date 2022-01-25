"""Config file for model parameters"""

# Define Data Path
CORE_DATA_PATH = "mppsteel/data"
LOG_PATH = "logs/"
TEST_FOLDER = "tests/"
IMPORT_DATA_PATH = f"{CORE_DATA_PATH}/import_data"
OUTPUT_FOLDER = f"{CORE_DATA_PATH}/output_data"
PKL_FOLDER = f"{CORE_DATA_PATH}/pkl_data"
PKL_DATA_IMPORTS = f"{PKL_FOLDER}/imported_data"
PKL_DATA_INTERMEDIATE = f"{PKL_FOLDER}/intermediate_data"
PKL_DATA_FINAL = f"{PKL_FOLDER}/final_data"
BC_TEST_FOLDER = f"{TEST_FOLDER}/business_case_tests"

FOLDERS_TO_CHECK_IN_ORDER = [
    # Top level folders
    CORE_DATA_PATH, LOG_PATH, TEST_FOLDER,
    # Second level folders
    IMPORT_DATA_PATH, PKL_FOLDER, OUTPUT_FOLDER, BC_TEST_FOLDER,
    # Third level folders
    PKL_DATA_IMPORTS, PKL_DATA_INTERMEDIATE, PKL_DATA_FINAL
    ]

PE_MODEL_FILENAME_DICT = {
    'power': 'Power Model.xlsx',
    'hydrogen': 'H2 Model.xlsx',
    'bio': 'Bio Model.xlsx',
    'ccus': 'CCUS Model.xlsx',

}

PE_MODEL_SHEETNAME_DICT = {
    'power': ['GridPrice', 'GridEmissions', 'RESPrice'],
    'hydrogen': ['Prices', 'Emissions'],
    'bio': ['Feedstock_Prices', 'Biomass_constraint'],
    'ccus': ['Transport', 'Storage']
}

MODEL_YEAR_START = 2020
MODEL_YEAR_END = 2050

TECH_MORATORIUM_DATE = 2030

BIOMASS_AV_TS_END_VALUE = 2000

ELECTRICITY_PRICE_MID_YEAR = 2035

EMISSIONS_FACTOR_SLAG = 0.55
ENERGY_DENSITY_MET_COAL = 28  # [MJ/kg]

DISCOUNT_RATE = 0.07
EUR_USD_CONVERSION_DEFAULT = 0.877
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

CARBON_TAX_SCENARIOS = {
    'off': (0, 0),
    'low': (0, 30),
    'average': (0, 100),
    'high': (0, 210),
}

GRID_DECARBONISATION_SCENARIOS = {
    'high': 'Accelerated ',
    'low': 'Central',
}

BIOMASS_SCENARIOS = {
    'average': 'Medium',
}

SOLVER_LOGICS = {
    'rank': 'ranked',
    'scale': 'scaled'
}

SCENARIO_SETTINGS = {
    'tech_moratorium': [True, False],
    'carbon_tax': CARBON_TAX_SCENARIOS.keys(),
    'green_premium_scenario': GREEN_PREMIUM_SCENARIOS.keys(),
    'electricity_cost_scenario': COST_SCENARIO_MAPPER.keys(),
    'grid_scenario': GRID_DECARBONISATION_SCENARIOS.keys(),
    'hydrogen_cost_scenario': COST_SCENARIO_MAPPER.keys(),
    'biomass_cost_scenario': BIOMASS_SCENARIOS.keys(),
    'steel_demand_scenario': STEEL_DEMAND_SCENARIO_MAPPER.keys(),
    'tech_switch_scenario': TECH_SWITCH_SCENARIOS.keys(),
    'solver_logic': SOLVER_LOGICS.keys()
}

DEFAULT_SCENARIO = {
    'tech_moratorium': True, # bool
    'carbon_tax_scenario': 'off', # off / low / average / high
    'green_premium_scenario': 'off', # off / low / average / high
    'electricity_cost_scenario': 'average', # low / average / high
    'grid_scenario': 'high', # low / high
    'hydrogen_cost_scenario': 'average', # low / average / high
    'biomass_cost_scenario': 'average', # average
    'steel_demand_scenario': 'average', # bau / average / high
    'tech_switch_scenario': 'equal_weight', # max_abatement / lowest_cost / equal_weight
    'solver_logic': 'rank' # scale / rank
}

SCENARIO_OPTIONS = {
    'default': DEFAULT_SCENARIO,
}
