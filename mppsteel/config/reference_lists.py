"""Reference Lists for the Application"""

# RESOURCE GROUPINGS
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

RESOURCE_CONTAINER_REF = {
    'scrap': ['Scrap'],
    'biomass': ['Biomass', 'Biomethane'],
    'ccs': ['Captured CO2'],
    'co2': ['Used CO2']
}

# TECHNOLOGY GROUPINGS
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

# GRAPH REFRENCE LISTS

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
    "Avg BF-BOF",
    "BAT BF-BOF",
    "DRI-EAF",
    "BAT BF-BOF_H2 PCI",
    "BAT BF-BOF_bio PCI",
    "DRI-EAF_50% bio-CH4",
    "DRI-EAF_50% green H2",
    "DRI-Melt-BOF",
    "Smelting Reduction",
    "BAT BF-BOF+CCUS",
    "BAT BF-BOF+CCU",
    "BAT BF-BOF+BECCUS",
    "DRI-EAF+CCUS",
    "DRI-EAF_100% green H2",
    "DRI-Melt-BOF+CCUS",
    "DRI-Melt-BOF_100% zero-C H2",
    "Electrolyzer-EAF",
    "Electrowinning-EAF",
    "Smelting Reduction+CCUS",
    "EAF"
]

# BUSINESS CASE UNITS
GJ_RESOURCES = [
    "BF gas",
    "COG",
    "BOF gas",
    "Natural gas",
    "Plastic waste",
    "Biomass",
    "Biomethane",
    "Hydrogen",
    "Electricity",
    "Steam"
]

KG_RESOURCES = [
    "BF slag",
    "Other slag"
]

TON_RESOURCES = [
    "Iron ore",
    "Scrap",
    "DRI",
    "Met coal",
    "Process emissions",
    "Emissivity wout CCS",
    "Captured CO2",
    "Used CO2",
    "Emissivity"
]
