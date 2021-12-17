"""Config file for model parameters"""

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
INVESTMENT_CYCLE_LENGTH = 20  # Years

# Define Data Path
IMPORT_DATA_PATH = "mppsteel/data/import_data"
PKL_FOLDER = "mppsteel/data/pkl_data"
CSV_OUTPUT_FOLDER = "mppsteel/data/csv_output_data"

TECH_REFERENCE_LIST = [
    "Avg BF-BOF",
    "BAT BF-BOF",
    "BAT BF-BOF_bio PCI",
    "BAT BF-BOF_H2 PCI",
    "BAT BF-BOF+CCUS",
    "DRI-EAF",
    "DRI-EAF_50% green H2",
    "DRI-EAF_50% bio-CH4",
    "DRI-EAF+CCUS",
    "DRI-EAF_100% green H2",
    "Smelting Reduction",
    "Smelting Reduction+CCUS",
    "EAF",
    "Electrolyzer-EAF",
    "BAT BF-BOF+CCU",
    "DRI-Melt-BOF",
    "DRI-Melt-BOF+CCUS",
    "DRI-Melt-BOF_100% zero-C H2",
    "Electrowinning-EAF",
    "BAT BF-BOF+BECCUS",
]

FURNACE_GROUP_DICT = {
    "blast_furnace": [
        "Avg BF-BOF",
        "BAT BF-BOF",
        "BAT BF-BOF_bio PCI",
        "BAT BF-BOF_H2 PCI",
        "BAT BF-BOF+CCUS",
        "BAT BF-BOF+BECCUS",
        "BAT BF-BOF+CCU",
    ],
    "dri-bof": ["DRI-Melt-BOF", "DRI-Melt-BOF_100% zero-C H2", "DRI-Melt-BOF+CCUS"],
    "dri-eaf": [
        "DRI-EAF",
        "DRI-EAF_50% bio-CH4",
        "DRI-EAF_50% green H2",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
    ],
    "smelting_reduction": ["Smelting Reduction", "Smelting Reduction+CCUS"],
    "eaf-basic": ["EAF"],
    "eaf-advanced": ["Electrolyzer-EAF", "Electrowinning-EAF"],
    "ccs": [
        "BAT BF-BOF+BECCUS",
        "BAT BF-BOF+CCUS",
        "DRI-Melt-BOF+CCUS",
        "DRI-EAF+CCUS",
        "Smelting Reduction+CCUS",
    ],
    "ccu": ["BAT BF-BOF+CCU"],
}
FURNACE_GROUP_DICT["dri"] = (
    FURNACE_GROUP_DICT["dri-bof"] + FURNACE_GROUP_DICT["dri-eaf"]
)
FURNACE_GROUP_DICT["eaf-all"] = (
    FURNACE_GROUP_DICT["eaf-basic"] + FURNACE_GROUP_DICT["eaf-advanced"]
)

NON_END_STATE_TECH = [
    "Avg BF-BOF",
    "BAT BF-BOF",
    "BAT BF-BOF_bio PCI",
    "BAT BF-BOF_H2 PCI",
    "DRI-EAF",
    "DRI-EAF_50% green H2",
    "DRI-EAF_50% bio-CH4",
    "Smelting Reduction",
    "EAF",
    "DRI-Melt-BOF",
]

SWITCH_DICT = {
    "Avg BF-BOF": [
        # "Close plant",
        "Avg BF-BOF",
        "BAT BF-BOF",
        "BAT BF-BOF_bio PCI",
        "BAT BF-BOF_H2 PCI",
        "BAT BF-BOF+CCUS",
        "BAT BF-BOF+BECCUS",
        "BAT BF-BOF+CCU",
        "DRI-Melt-BOF",
        "DRI-Melt-BOF_100% zero-C H2",
        "DRI-Melt-BOF+CCUS",
        "DRI-EAF",
        "DRI-EAF_50% bio-CH4",
        "DRI-EAF_50% green H2",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
        "Smelting Reduction",
        "Smelting Reduction+CCUS",
        "EAF",
        "Electrolyzer-EAF",
        "Electrowinning-EAF",
    ],
    "BAT BF-BOF": [
        # "Close plant",
        "BAT BF-BOF",
        "BAT BF-BOF_bio PCI",
        "BAT BF-BOF_H2 PCI",
        "BAT BF-BOF+CCUS",
        "BAT BF-BOF+BECCUS",
        "BAT BF-BOF+CCU",
        "DRI-Melt-BOF",
        "DRI-Melt-BOF_100% zero-C H2",
        "DRI-Melt-BOF+CCUS",
        "DRI-EAF",
        "DRI-EAF_50% bio-CH4",
        "DRI-EAF_50% green H2",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
        "Smelting Reduction",
        "Smelting Reduction+CCUS",
        "EAF",
        "Electrolyzer-EAF",
        "Electrowinning-EAF",
    ],
    "BAT BF-BOF_bio PCI": [
        # "Close plant",
        "BAT BF-BOF_bio PCI",
        "BAT BF-BOF+CCUS",
        "BAT BF-BOF+BECCUS",
        "BAT BF-BOF+CCU",
        "DRI-Melt-BOF_100% zero-C H2",
        "DRI-Melt-BOF+CCUS",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
        "Smelting Reduction+CCUS",
        "EAF",
        "Electrolyzer-EAF",
        "Electrowinning-EAF",
    ],
    "BAT BF-BOF_H2 PCI": [
        # "Close plant",
        "BAT BF-BOF_H2 PCI",
        "BAT BF-BOF+CCUS",
        "BAT BF-BOF+BECCUS",
        "BAT BF-BOF+CCU",
        "DRI-Melt-BOF_100% zero-C H2",
        "DRI-Melt-BOF+CCUS",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
        "Smelting Reduction+CCUS",
        "EAF",
        "Electrolyzer-EAF",
        "Electrowinning-EAF",
    ],
    "DRI-Melt-BOF": [
        # "Close plant",
        "DRI-Melt-BOF",
        "DRI-Melt-BOF_100% zero-C H2",
        "DRI-Melt-BOF+CCUS",
    ],
    "DRI-EAF": [
        # "Close plant",
        "DRI-EAF",
        "DRI-EAF_50% bio-CH4",
        "DRI-EAF_50% green H2",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
        "Smelting Reduction",
        "Smelting Reduction+CCUS",
        "Electrolyzer-EAF",
        "Electrowinning-EAF",
    ],
    "DRI-EAF_50% bio-CH4": [
        # "Close plant",
        "DRI-EAF_50% bio-CH4",
        "Smelting Reduction+CCUS",
        "Electrolyzer-EAF",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
    ],
    "DRI-EAF_50% green H2": [
        # "Close plant",
        "DRI-EAF_50% green H2",
        "Smelting Reduction+CCUS",
        "Electrolyzer-EAF",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
    ],
    "Smelting Reduction": [
        # "Close plant",
        "Smelting Reduction",
        "Smelting Reduction+CCUS",
    ],
    "BAT BF-BOF+CCUS": ["BAT BF-BOF+CCUS"],
    "BAT BF-BOF+BECCUS": ["BAT BF-BOF+BECCUS"],
    "BAT BF-BOF+CCU": ["BAT BF-BOF+CCU"],
    "DRI-Melt-BOF_100% zero-C H2": ["DRI-Melt-BOF_100% zero-C H2"],
    "DRI-Melt-BOF+CCUS": ["DRI-Melt-BOF+CCUS"],
    "DRI-EAF+CCUS": ["DRI-EAF+CCUS"],
    "DRI-EAF_100% green H2": ["DRI-EAF_100% green H2"],
    "Smelting Reduction+CCUS": ["Smelting Reduction+CCUS"],
    "EAF": ["EAF"],
    "Electrolyzer-EAF": ["Electrolyzer-EAF"],
    "Electrowinning-EAF": ["Electrowinning-EAF"],
}

SWITCH_CAPEX_DATA_POINTS = {
    "2020": 319.249187119815,
    "2030": 319.249187119815,
    "2050": 286.218839300307,
}

PER_T_STEEL_DICT_UNITS = {
    "Iron ore": "t / t steel",
    "Scrap": "t / t steel",
    "DRI": "t / t steel",
    "Met coal": "t / t steel",
    "Coke": "GJ / t steel",
    "Thermal coal": "GJ / t steel",
    "BF gas": "GJ / t steel",
    "COG": "GJ / t steel",
    "BOF gas": "GJ / t steel",
    "Natural gas": "GJ / t steel",
    "Plastic waste": "GJ / t steel",
    "Biomass": "GJ / t steel",
    "Biomethane": "GJ / t steel",
    "Hydrogen": "GJ / t steel",
    "Electricity": "GJ / t steel",
    "Steam": "GJ / t steel",
    "BF slag": "kg / t steel",
    "Other slag": "kg / t steel",
    "Process emissions": "t CO2 / t steel",
    "Emissivity wout CCS": "t CO2 / t steel",
    "Captured CO2": "t CO2 / t steel",
    "Used CO2": "t CO2 / t steel",
    "Emissivity": "t CO2 / t steel",
}


# Define Groups
bosc_factor_group = (
    FURNACE_GROUP_DICT["blast_furnace"]
    + FURNACE_GROUP_DICT["smelting_reduction"]
    + FURNACE_GROUP_DICT["dri-bof"]
)
eaf_factor_group = FURNACE_GROUP_DICT["dri-eaf"] + FURNACE_GROUP_DICT["eaf-all"]
electricity_and_steam_self_gen_group = FURNACE_GROUP_DICT["smelting_reduction"]
electricity_self_gen_group = (
    FURNACE_GROUP_DICT["blast_furnace"] + FURNACE_GROUP_DICT["smelting_reduction"]
)

HARD_CODED_FACTORS = {
    "dri": {"Coke Production": 0, "Sintering": 0},
    "eaf-basic": {
        "Coke Production": 1,
        "Sintering": 1,
        "Pelletisation": 1,
        "Blast Furnace": 1,
    },
    "eaf-advanced": {
        "Coke Production": 0,
        "Sintering": 0,
    },
    "smelting_reduction": {
        "Coke Production": 0,
    },
}

LOW_CARBON_TECHS = [
    "BAT BF-BOF+CCUS",
    "BAT BF-BOF+CCU",
    "Smelting Reduction+CCUS",
    "Electrolyzer-EAF",
    "DRI-EAF+CCUS",
    "DRI-EAF_100% green H2",
    "DRI-Melt-BOF+CCUS",
    "DRI-Melt-BOF_100% zero-C H2",
    "Electrowinning-EAF",
]

END_STATE_TECHS = LOW_CARBON_TECHS.copy()
END_STATE_TECHS += [r"BAT BF-BOF+BECCUS"]
END_STATE_TECHS = END_STATE_TECHS[:10].copy()

TRANSITIONAL_TECHS = [
    "BAT BF-BOF_bio PCI",
    "BAT BF-BOF_H2 PCI",
    "DRI-EAF_50% green H2",
    "DRI-EAF_50% bio-CH4",
    "DRI-Melt-BOF",
    "Smelting Reduction"
]

CURRENT_TECHS = [
    "Avg BF-BOF",
    "BAT BF-BOF",
    "DRI-EAF",
    "EAF"
]

TECHNOLOGY_STATES = {
    'current': CURRENT_TECHS,
    'transitional': TRANSITIONAL_TECHS,
    'end_state': END_STATE_TECHS
}


TECH_MATERIAL_CHECK_DICT = {
    "Avg BF-BOF": [],
    "BAT BF-BOF": [],
    "BAT BF-BOF_bio PCI": ['Bioenergy', 'Scrap'],
    "BAT BF-BOF_H2 PCI": ['Scrap'],
    "BAT BF-BOF+CCUS": ['Used CO2', 'Used CO2', 'Scrap'],
    "DRI-EAF": [],
    "DRI-EAF_50% green H2": [],
    "DRI-EAF_50% bio-CH4": ['Bioenergy'],
    "DRI-EAF+CCUS": ['Used CO2', ],
    "DRI-EAF_100% green H2": [],
    "Smelting Reduction": ['Scrap'],
    "Smelting Reduction+CCUS": ['Used CO2', 'Scrap'],
    "EAF": ['Scrap EAF'],
    "Electrolyzer-EAF": [],
    "BAT BF-BOF+CCU": ['Used CO2', 'Scrap'],
    "DRI-Melt-BOF": [],
    "DRI-Melt-BOF+CCUS": ['Used CO2', ],
    "DRI-Melt-BOF_100% zero-C H2": [],
    "Electrowinning-EAF": [],
    "BAT BF-BOF+BECCUS": ['Captured CO2', 'Bioenergy', 'Scrap'],
}

CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION = 0.95
CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION = 0.6
SCRAP_OVERSUPPLY_CUTOFF_FOR_NEW_EAF_PLANT_DECISION = 0.15
AVERAGE_LEVEL_OF_CAPACITY = 0.95

RESOURCE_CONTAINER_REF = {'Bioenergy': 'biomass', 'Scrap': 'scrap', 'Used CO2': 'used_co2', 'Captured CO2': 'captured_co2'}

TCO_RANK_2_SCALER = 1.3
TCO_RANK_1_SCALER = 1.1
ABATEMENT_RANK_2 = 2.37656461606311 # Switching from Avg BF-BOF to BAT BF-BOF+CCUS
ABATEMENT_RANK_3 = 0.932690243851946 # Switching from Avg BF-BOF to BAT BF-BOF_bio PCI

GREEN_PREMIUM_MIN_PCT = 0.01
GREEN_PREMIUM_MAX_PCT = 0.05
SWITCH_RANK_PROPORTIONS = {'tco': 0.6, 'emissions': 0.4}
NET_ZERO_TARGET = 2050