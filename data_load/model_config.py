"""Config file for model parameters"""

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
ENERGY_DENSITY_MET_COAL=28 # [MJ/kg]

DISCOUNT_RATE = 0.07
EUR_USD_CONVERSION = 0.877
INVESTMENT_CYCLE_LENGTH = 20 # Years

# Define Data Path
IMPORT_DATA_PATH = "../data/import_data"
PKL_FOLDER = "../data/pkl_data"

TECH_REFERENCE_LIST = ['Avg BF-BOF', 'BAT BF-BOF', 'BAT BF-BOF_bio PCI',
    'BAT BF-BOF_H2 PCI', 'BAT BF-BOF+CCUS', 'DRI-EAF',
    'DRI-EAF_50% green H2', 'DRI-EAF_50% bio-CH4', 'DRI-EAF+CCUS',
    'DRI-EAF_100% green H2', 'Smelting Reduction',
    'Smelting Reduction+CCUS', 'EAF', 'Electrolyzer-EAF',
    'BAT BF-BOF+CCU', 'DRI-Melt-BOF', 'DRI-Melt-BOF+CCUS',
    'DRI-Melt-BOF_100% zero-C H2', 'Electrowinning-EAF',
    'BAT BF-BOF+BECCUS'
]

FURNACE_GROUP_DICT = {
    'blast_furnace': ['Avg BF-BOF', 'BAT BF-BOF', 'BAT BF-BOF_bio PCI', 'BAT BF-BOF_H2 PCI', 'BAT BF-BOF+CCUS', 'BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCU'],
    'dri-bof': ['DRI-Melt-BOF', 'DRI-Melt-BOF_100% zero-C H2', 'DRI-Melt-BOF+CCUS'],
    'dri-eaf': ['DRI-EAF', 'DRI-EAF_50% bio-CH4', 'DRI-EAF_50% green H2', 'DRI-EAF+CCUS', 'DRI-EAF_100% green H2'],
    'smelting_reduction': ['Smelting Reduction', 'Smelting Reduction+CCUS'],
    'eaf-basic': ['EAF'],
    'eaf-advanced': ['Electrolyzer-EAF', 'Electrowinning-EAF'],
    'ccs': ['BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCUS', 'DRI-Melt-BOF+CCUS', 'DRI-EAF+CCUS', 'Smelting Reduction+CCUS'],
    'ccu': ['BAT BF-BOF+CCU']
}
FURNACE_GROUP_DICT['dri'] = FURNACE_GROUP_DICT['dri-bof'] + FURNACE_GROUP_DICT['dri-eaf']
FURNACE_GROUP_DICT['eaf-all'] = FURNACE_GROUP_DICT['eaf-basic'] + FURNACE_GROUP_DICT['eaf-advanced']

NON_END_STATE_TECH = [
    'Avg BF-BOF', 'BAT BF-BOF', 'BAT BF-BOF_bio PCI',
    'BAT BF-BOF_H2 PCI','DRI-EAF',
    'DRI-EAF_50% green H2', 'DRI-EAF_50% bio-CH4',
    'Smelting Reduction','EAF', 'DRI-Melt-BOF',
]

SWITCH_DICT= {
    'Avg BF-BOF': ['Close plant', 'Avg BF-BOF', 'BAT BF-BOF', 'BAT BF-BOF_bio PCI', 'BAT BF-BOF_H2 PCI', 'BAT BF-BOF+CCUS', 'BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCU', 'DRI-Melt-BOF', 'DRI-Melt-BOF_100% zero-C H2', 'DRI-Melt-BOF+CCUS','DRI-EAF', 'DRI-EAF_50% bio-CH4', 'DRI-EAF_50% green H2', 'DRI-EAF+CCUS', 'DRI-EAF_100% green H2','Smelting Reduction', 'Smelting Reduction+CCUS', 'EAF', 'Electrolyzer-EAF', 'Electrowinning-EAF'],
    'BAT BF-BOF': ['Close plant', 'BAT BF-BOF','BAT BF-BOF_bio PCI', 'BAT BF-BOF_H2 PCI', 'BAT BF-BOF+CCUS', 'BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCU', 'DRI-Melt-BOF', 'DRI-Melt-BOF_100% zero-C H2', 'DRI-Melt-BOF+CCUS','DRI-EAF', 'DRI-EAF_50% bio-CH4', 'DRI-EAF_50% green H2', 'DRI-EAF+CCUS', 'DRI-EAF_100% green H2','Smelting Reduction', 'Smelting Reduction+CCUS', 'EAF', 'Electrolyzer-EAF', 'Electrowinning-EAF'],
    'BAT BF-BOF_bio PCI': ['Close plant', 'BAT BF-BOF_bio PCI','BAT BF-BOF+CCUS', 'BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCU',  'DRI-Melt-BOF_100% zero-C H2', 'DRI-Melt-BOF+CCUS', 'DRI-EAF+CCUS', 'DRI-EAF_100% green H2', 'Smelting Reduction+CCUS', 'EAF', 'Electrolyzer-EAF', 'Electrowinning-EAF'],
    'BAT BF-BOF_H2 PCI': ['Close plant','BAT BF-BOF_H2 PCI', 'BAT BF-BOF+CCUS', 'BAT BF-BOF+BECCUS', 'BAT BF-BOF+CCU',  'DRI-Melt-BOF_100% zero-C H2', 'DRI-Melt-BOF+CCUS',  'DRI-EAF+CCUS', 'DRI-EAF_100% green H2','Smelting Reduction+CCUS', 'EAF', 'Electrolyzer-EAF', 'Electrowinning-EAF'],
    'DRI-Melt-BOF':['Close plant','DRI-Melt-BOF','DRI-Melt-BOF_100% zero-C H2', 'DRI-Melt-BOF+CCUS'],
    'DRI-EAF':['Close plant','DRI-EAF', 'DRI-EAF_50% bio-CH4', 'DRI-EAF_50% green H2','DRI-EAF+CCUS', 'DRI-EAF_100% green H2','Smelting Reduction','Smelting Reduction+CCUS', 'Electrolyzer-EAF', 'Electrowinning-EAF'],
    'DRI-EAF_50% bio-CH4': ['Close plant', 'DRI-EAF_50% bio-CH4','Smelting Reduction+CCUS','Electrolyzer-EAF', 'DRI-EAF+CCUS', 'DRI-EAF_100% green H2'],
    'DRI-EAF_50% green H2':['Close plant','DRI-EAF_50% green H2','Smelting Reduction+CCUS','Electrolyzer-EAF', 'DRI-EAF+CCUS', 'DRI-EAF_100% green H2'],
    'Smelting Reduction':['Close plant','Smelting Reduction','Smelting Reduction+CCUS'],
    'BAT BF-BOF+CCUS':['BAT BF-BOF+CCUS'],
    'BAT BF-BOF+BECCUS':['BAT BF-BOF+BECCUS'],
    'BAT BF-BOF+CCU':['BAT BF-BOF+CCU'],
    'DRI-Melt-BOF_100% zero-C H2':['DRI-Melt-BOF_100% zero-C H2'],
    'DRI-Melt-BOF+CCUS':['DRI-Melt-BOF+CCUS'],
    'DRI-EAF+CCUS':['DRI-EAF+CCUS'],
    'DRI-EAF_100% green H2':['DRI-EAF_100% green H2'],
    'Smelting Reduction+CCUS':['Smelting Reduction+CCUS'],
    'EAF':['EAF'],
    'Electrolyzer-EAF':['Electrolyzer-EAF'],
    'Electrowinning-EAF':['Electrowinning-EAF']
}
