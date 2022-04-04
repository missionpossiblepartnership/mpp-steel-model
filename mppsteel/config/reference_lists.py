"""Reference Lists for the Application"""

MPP_COLOR_LIST = [
    "#A0522D",
    "#7F6000",
    "#1E3B63",
    "#9DB1CF",
    "#FFC000",
    "#59A270",
    "#BCDAC6",
    "#E76B67",
    "#A5A5A5",
    "#F2F2F2",
]

NEW_COUNTRY_COL_LIST = [
    "country_code",
    "country",
    "official_name",
    "m49_code",
    "region",
    "continent",
    "wsa_region",
    "rmi_region",
]

RESOURCE_CATEGORY_MAPPER = {
    'Iron ore': 'Feedstock',
    'Scrap': 'Feedstock',
    'DRI': 'Feedstock',
    'Met coal': 'Fossil Fuels',
    'Thermal coal': 'Fossil Fuels',
    'Coke': 'Fossil Fuels',
    'COG': 'Fossil Fuels',
    'BF gas': 'Fossil Fuels',
    'BOF gas': 'Fossil Fuels',
    'Natural gas': 'Fossil Fuels',
    'Plastic waste': 'Fossil Fuels',
    'Biomass': 'Bio Fuels',
    'Biomethane': 'Bio Fuels',
    'Hydrogen': 'Hydrogen',
    'Electricity': 'Electricity',
    'Steam': 'Other Opex',
    'BF slag': 'Other Opex',
    'Other slag': 'Other Opex',
    'Captured CO2': 'CCS',
    'Used CO2': 'CCS',
    'Process emissions': 'Emissivity',
    'Emissivity wout CCS': 'Emissivity',
    'Emissivity': 'Emissivity'
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

NEW_COUNTRY_COL_LIST = [
    "country_code",
    "country",
    "official_name",
    "m49_code",
    "region",
    "continent",
    "wsa_region",
    "rmi_region",
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
        "DRI-Melt-BOF",
        "DRI-Melt-BOF_100% zero-C H2",
        "DRI-Melt-BOF+CCUS",
    ],
    "DRI-EAF": [
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
        "DRI-EAF_50% bio-CH4",
        "Smelting Reduction+CCUS",
        "Electrolyzer-EAF",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
    ],
    "DRI-EAF_50% green H2": [
        "DRI-EAF_50% green H2",
        "Smelting Reduction+CCUS",
        "Electrolyzer-EAF",
        "DRI-EAF+CCUS",
        "DRI-EAF_100% green H2",
    ],
    "Smelting Reduction": [
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

TECH_REFERENCE_LIST = list(SWITCH_DICT.keys())

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

TRANSITIONAL_TECHS = [
    "BAT BF-BOF_bio PCI",
    "BAT BF-BOF_H2 PCI",
    "DRI-EAF_50% green H2",
    "DRI-EAF_50% bio-CH4",
    "DRI-Melt-BOF",
    "Smelting Reduction",
]

CURRENT_TECHS = ["Avg BF-BOF", "BAT BF-BOF", "DRI-EAF", "EAF"]

TECHNOLOGY_STATES = {
    "current": CURRENT_TECHS,
    "transitional": TRANSITIONAL_TECHS,
    "end_state": END_STATE_TECHS,
}

TECHNOLOGY_PHASES = {
    "Initial": ["Avg BF-BOF"],
    "Transition": [
        "BAT BF-BOF",
        "BAT BF-BOF_bio PCI",
        "BAT BF-BOF_H2 PCI",
        "DRI-EAF",
        "DRI-EAF_50% bio-CH4",
        "DRI-EAF_50% green H2",
        "Smelting Reduction",
        "DRI-Melt-BOF",
    ],
    "End State": [
        "BAT BF-BOF+CCUS",
        "DRI-EAF_100% green H2",
        "DRI-EAF+CCUS",
        "EAF",
        "BAT BF-BOF+CCU",
        "BAT BF-BOF+BECCUS",
        "Electrolyzer-EAF",
        "Smelting Reduction+CCUS",
        "DRI-Melt-BOF+CCUS",
        "DRI-Melt-BOF_100% zero-C H2",
        "Electrowinning-EAF",
    ],
}


TECH_MATERIAL_CHECK_DICT = {
    "Avg BF-BOF": [],
    "BAT BF-BOF": [],
    "BAT BF-BOF_bio PCI": ["Bioenergy", "Scrap"],
    "BAT BF-BOF_H2 PCI": ["Scrap"],
    "BAT BF-BOF+CCUS": ["Used CO2", "Used CO2", "Scrap"],
    "DRI-EAF": [],
    "DRI-EAF_50% green H2": [],
    "DRI-EAF_50% bio-CH4": ["Bioenergy"],
    "DRI-EAF+CCUS": [
        "Used CO2",
    ],
    "DRI-EAF_100% green H2": [],
    "Smelting Reduction": ["Scrap"],
    "Smelting Reduction+CCUS": ["Used CO2", "Scrap"],
    "EAF": ["Scrap EAF"],
    "Electrolyzer-EAF": [],
    "BAT BF-BOF+CCU": ["Used CO2", "Scrap"],
    "DRI-Melt-BOF": [],
    "DRI-Melt-BOF+CCUS": [
        "Used CO2",
    ],
    "DRI-Melt-BOF_100% zero-C H2": [],
    "Electrowinning-EAF": [],
    "BAT BF-BOF+BECCUS": ["Captured CO2", "Bioenergy", "Scrap"],
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

RESOURCE_CONTAINER_REF = {
    "Bioenergy": "biomass",
    "Scrap": "scrap",
    "Used CO2": "used_co2",
    "Captured CO2": "captured_co2",
}

FILES_TO_REFRESH = []

EU_COUNTRIES = [
    "AUT",
    "BEL",
    "BGR",
    "CYP",
    "CZE",
    "DEU",
    "DNK",
    "ESP",
    "EST",
    "FIN",
    "FRA",
    "GRC",
    "HRV",
    "HUN",
    "IRL",
    "ITA",
    "LTU",
    "LUX",
    "LVA",
    "MLT",
    "NLD",
    "POL",
    "PRT",
    "ROU",
    "SVK",
    "SVN",
    "SWE",
]

TECHNOLOGY_PROCESSES = {
    "Avg BF-BOF": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "Oxygen Generation",
        "Basic Oxygen Steelmaking + Casting",
        "Limestone",
        "Self-Generation Of Electricity",
    ],
    "BAT BF-BOF": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "Oxygen Generation",
        "Basic Oxygen Steelmaking + Casting",
        "Limestone",
        "Self-Generation Of Electricity",
    ],
    "BAT BF-BOF_bio PCI": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "Oxygen Generation",
        "Basic Oxygen Steelmaking + Casting",
        "Limestone",
        "Self-Generation Of Electricity",
    ],
    "BAT BF-BOF_H2 PCI": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "Oxygen Generation",
        "Basic Oxygen Steelmaking + Casting",
        "Limestone",
        "Self-Generation Of Electricity",
    ],
    "BAT BF-BOF+CCUS": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "Oxygen Generation",
        "Basic Oxygen Steelmaking + Casting",
        "Limestone",
        "Self-Generation Of Electricity",
        "CCS",
    ],
    "DRI-EAF": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "EAF (Steel-making) + Casting",
    ],
    "DRI-EAF_50% green H2": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "EAF (Steel-making) + Casting",
    ],
    "DRI-EAF_50% bio-CH4": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "EAF (Steel-making) + Casting",
    ],
    "DRI-EAF+CCUS": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "EAF (Steel-making) + Casting",
        "CCS",
    ],
    "DRI-EAF_100% green H2": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "EAF (Steel-making) + Casting",
    ],
    "Smelting Reduction": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Smelting Furnace",
        "Oxygen Generation",
        "Limestone",
        "Basic Oxygen Steelmaking + Casting",
        "Self-Generation Of Electricity",
    ],
    "Smelting Reduction+CCUS": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Smelting Furnace",
        "Oxygen Generation",
        "Limestone",
        "Basic Oxygen Steelmaking + Casting",
        "Self-Generation Of Electricity",
        "CCS",
    ],
    "EAF": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "EAF (Steel-making) + Casting",
    ],
    "Electrolyzer-EAF": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Electrolyzer",
        "EAF (Steel-making) + Casting",
    ],
    "BAT BF-BOF+CCU": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "Oxygen Generation",
        "Basic Oxygen Steelmaking + Casting",
        "Limestone",
        "Self-Generation Of Electricity",
        "CCU -CO-based",
        "CCU -CO2-based",
    ],
    "DRI-Melt-BOF": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "Remelt",
        "Oxygen Generation",
        "Limestone",
        "Basic Oxygen Steelmaking + Casting",
    ],
    "DRI-Melt-BOF+CCUS": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "Remelt",
        "Oxygen Generation",
        "Limestone",
        "Basic Oxygen Steelmaking + Casting",
        "CCS",
    ],
    "DRI-Melt-BOF_100% zero-C H2": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "Remelt",
        "Oxygen Generation",
        "Limestone",
        "Basic Oxygen Steelmaking + Casting",
    ],
    "Electrowinning-EAF": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Electrolyzer",
        "EAF (Steel-making) + Casting",
    ],
    "BAT BF-BOF+BECCUS": [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "Oxygen Generation",
        "Basic Oxygen Steelmaking + Casting",
        "Limestone",
        "Self-Generation Of Electricity",
        "CCS",
    ],
}

GRAPH_CAPEX_OPEX_DICT_SPLIT = {
    "Feedstock": ["Iron Ore", "Scrap", "DRI"],
    "Fossil Fuels": [
        "Met coal",
        "Coke",
        "Thermal coal",
        "BF gas",
        "BOF gas",
        "Natural gas",
        "Plastic waste",
    ],
    "Bio Fuels": ["Biomass", "Biomethane"],
    "Hydrogen": ["Hydrogen"],
    "Electricity": ["Electricity"],
    "CCS": ["CCS"],
    "Other OPEX": [
        ["Other OPEX"],
        ["Steam", "BF slag"],
    ],  # attention! BF slag is a co product that is sold of to other industry sectors and produces revenue (so its negative costs added to the OPEX sum which reduces the actual result of other OPEX)
    "BF Capex": ["BF Capex"],  # with WACC over 20 years
    "GF Capex": ["GF Capex"],  # with WACC over 20 years
}

GRAPH_COL_ORDER = [
    "Avg BF-BOF", "BAT BF-BOF", "DRI-EAF", "BAT BF-BOF_H2 PCI", "BAT BF-BOF_bio PCI",
    "DRI-EAF_50% bio-CH4", "DRI-EAF_50% green H2", "DRI-Melt-BOF", "Smelting Reduction",
    "BAT BF-BOF+CCUS", "BAT BF-BOF+CCU", "BAT BF-BOF+BECCUS", "DRI-EAF+CCUS",
    "DRI-EAF_100% green H2", "DRI-Melt-BOF+CCUS", "DRI-Melt-BOF_100% zero-C H2", "Electrolyzer-EAF",
    "Electrowinning-EAF", "Smelting Reduction+CCUS", "EAF"
]