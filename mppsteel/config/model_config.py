"""Config file for model parameters"""

from pathlib import Path

# PATH NAMES
PROJECT_PATH = Path(__file__).parent.parent.parent

INTERMEDIATE_DATA_OUTPUT_NAME = "intermediate_data"
FINAL_DATA_OUTPUT_NAME = "final_data"
COMBINED_OUTPUT_FOLDER_NAME = "combined_output"
MULTIPLE_RUN_SCENARIO_FOLDER_NAME = "multiple_run_scenarios"

CORE_DATA_PATH = "mppsteel/data"
LOG_PATH = "logs/"
TEST_FOLDER = "tests/"
IMPORT_DATA_PATH = f"{CORE_DATA_PATH}/import_data"
OUTPUT_FOLDER = f"{CORE_DATA_PATH}/output_data"
PKL_FOLDER = f"{CORE_DATA_PATH}/pkl_data"
PKL_DATA_IMPORTS = f"{PKL_FOLDER}/imported_data"
PKL_DATA_FORMATTED = f"{PKL_FOLDER}/formatted_import_data"
PKL_DATA_INTERMEDIATE = f"{PKL_FOLDER}/{INTERMEDIATE_DATA_OUTPUT_NAME}"
PKL_DATA_FINAL = f"{PKL_FOLDER}/{FINAL_DATA_OUTPUT_NAME}"
PKL_DATA_COMBINED = f"{PKL_FOLDER}/{COMBINED_OUTPUT_FOLDER_NAME}"


FOLDERS_TO_CHECK_IN_ORDER = [
    # Top level folders
    CORE_DATA_PATH,
    LOG_PATH,
    TEST_FOLDER,
    # Second level folders
    IMPORT_DATA_PATH,
    PKL_FOLDER,
    OUTPUT_FOLDER,
    # Third level folders
    PKL_DATA_IMPORTS,
    PKL_DATA_FORMATTED,
    PKL_DATA_INTERMEDIATE,
    PKL_DATA_FINAL,
]

# DATE / TIME FORMAT
DATETIME_FORMAT = "%Y-%m-%d %H-%M"

# MODEL YEAR PARAMETERS
MODEL_YEAR_START = 2020
MODEL_YEAR_END = 2050
MODEL_YEAR_RANGE = range(MODEL_YEAR_START, MODEL_YEAR_END + 1)
CARBON_TAX_START_YEAR = 2023
CARBON_TAX_END_YEAR = 2050
GREEN_PREMIUM_START_YEAR = 2023
GREEN_PREMIUM_END_YEAR = 2050
TECH_MORATORIUM_DATE = 2030
NET_ZERO_TARGET_YEAR = 2050
CAPACITY_CONSTRAINT_FIXED_RATE_END_YEAR = 2026
CAPACITY_CONSTRAINT_FIXED_RATE_YEAR_RANGE = range(
    MODEL_YEAR_START,
    CAPACITY_CONSTRAINT_FIXED_RATE_END_YEAR + 1,
)
STEEL_PLANT_EARLIEST_START_DATE = 2000
STEEL_PLANT_LATEST_START_DATE = 2013
YEARS_TO_SKIP_FOR_SOLVER = [MODEL_YEAR_START]
MID_MODEL_CHECKPOINT_YEAR_FOR_GRAPHS = 2030

# FINANCIAL PARAMETERS
DISCOUNT_RATE = 0.07
USD_TO_EUR_CONVERSION_DEFAULT = 0.877
SWITCH_CAPEX_DATA_POINTS = {
    "2020": 319.249187119815,
    "2030": 319.249187119815,
    "2050": 286.218839300307,
}

# INVESTMENT PARAMETERS
STEEL_PLANT_LIFETIME_YEARS = 40
INVESTMENT_CYCLE_DURATION_YEARS = 20
INVESTMENT_CYCLE_VARIANCE_YEARS = 3
INVESTMENT_OFFCYCLE_BUFFER_TOP = 3
INVESTMENT_OFFCYCLE_BUFFER_TAIL = 8
NET_ZERO_VARIANCE_YEARS = 3

# CONVERSION FACTORS: WEIGHT
GIGATON_TO_MEGATON_FACTOR = 1000
MEGATON_TO_KILOTON_FACTOR = 1000
KILOTON_TO_TON_FACTOR = 1000
TON_TO_KILOGRAM_FACTOR = 1000
MEGATON_TO_TON = MEGATON_TO_KILOTON_FACTOR * KILOTON_TO_TON_FACTOR
BILLION_NUMBER = 1000000000

# CONVERSION FACTORS: ENERGY
MEGAWATT_HOURS_TO_GIGAJOULES = 3.6
TERAWATT_TO_PETAJOULE_FACTOR = 3.6
EXAJOULE_TO_PETAJOULE = 1000
PETAJOULE_TO_TERAJOULE = 1000
TERAJOULE_TO_GIGAJOULE = 1000
GIGAJOULE_TO_MEGAJOULE_FACTOR = 1000
PETAJOULE_TO_GIGAJOULE = PETAJOULE_TO_TERAJOULE * TERAJOULE_TO_GIGAJOULE
EXAJOULE_TO_GIGAJOULE = (
    EXAJOULE_TO_PETAJOULE * PETAJOULE_TO_TERAJOULE * TERAJOULE_TO_GIGAJOULE
)
MMBTU_TO_GJ = 1.055
INITIAL_MET_COAL_PRICE_USD_PER_GJ = 106.5

# ENERGY DENSITY FACTORS
BIOMASS_ENERGY_DENSITY_GJ_PER_TON = 18
EMISSIONS_FACTOR_SLAG = 0.55
MET_COAL_ENERGY_DENSITY_MJ_PER_KG = 28
HYDROGEN_ENERGY_DENSITY_MJ_PER_KG = 120
PLASTIC_WASTE_ENERGY_DENSITY_MJ_PER_KG = 45
THERMAL_COAL_ENERGY_DENSITY = 24

# CAPACITY UTILIZATION PARAMETERS
CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION = 0.95
CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION = 0.6

# CONSTRAINT PARAMETERS
SCRAP_CONSTRAINT_TOLERANCE_FACTOR = 0.1
CAPACITY_CONSTRAINT_FIXED_RATE_MTPA = 156
CAPACITY_CONSTRAINT_FIXED_GROWTH_RATE = 0.1
MAX_WAITING_LIST_YEARS = 6
NUMBER_OF_TECHNOLOGIES_PER_BIN_GROUP = 2

# TECHNOLOGY RANKING FACTORS
TCO_RANK_2_SCALER = 1.3
TCO_RANK_1_SCALER = 1.1
ABATEMENT_RANK_2 = 2.37656461606311  # Switching from Avg BF-BOF to BAT BF-BOF+CCUS
ABATEMENT_RANK_3 = 0.932690243851946  # Switching from Avg BF-BOF to BAT BF-BOF_bio PCI

# LEVELIZED COST PARAMETERS
AVERAGE_CAPACITY_MT = 2.5
AVERAGE_CUF = 0.8

# REGIONAL PARAMETERS
MAIN_REGIONAL_SCHEMA = "rmi_region"
RESULTS_REGIONS_TO_MAP = ["continent", "wsa", "rmi"]

# ROUNDING PARAMETERS
TRADE_ROUNDING_NUMBER = 3
UTILIZATION_ROUNDING_NUMBER = 2

# SPECIAL MODEL RUN PARAMTERS
DEFAULT_NUMBER_OF_RUNS = 10
MULTIPLE_MODEL_RUN_EVALUATION_YEARS = [2050]

# OUTPUT FILE NAMES
INTERMEDIATE_RESULT_PKL_FILES = [
    "plant_result_df",
    "calculated_emissivity_combined",
    "levelized_cost_standardized",
    "emissivity_abatement_switches",
    "tco_summary_data",
    "full_trade_summary",
    "tech_choice_records",
    "tech_rank_records",
]

FINAL_RESULT_PKL_FILES = [
    "production_resource_usage",
    "production_emissions",
    "global_metaresults",
    "investment_results",
    "green_capacity_ratio",
    "cost_of_steelmaking",
    "levelized_cost_results",
]

TRADE_PCT_BOUNDARY_FACTOR_DICT = {
    "Middle East": 0.05,
    "Europe": 0,
    "Africa": 0.05,
    "Southeast Asia": 0.05,
    "South and Central America": 0.05,
    "RoW": 0,
    "CIS": 0.05,
    "NAFTA": 0,
    "China": 0.05,
    "India": 0.05,
    "Japan, South Korea, and Taiwan": 0,
}

UNDERSCORE_NUMBER_REGEX = r"\_\d+"
NUMBER_REGEX = r"\d+"

MAX_TASKS_PER_MP_CHILD = 4
