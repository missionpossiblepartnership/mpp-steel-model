"""Script for establishing capex switching values"""

# For Data Manipulation
import pandas as pd

# For logger
from utils import get_logger, read_pickle_folder, serialise_file, serialize_df

from model_config import PKL_FOLDER, FURNACE_GROUP_DICT, TECH_REFERENCE_LIST, SWITCH_DICT
from data_interface import capex_generator

# Create logger
logger = get_logger('Capex Switching')

capex_dict = read_pickle_folder(PKL_FOLDER, 'capex_dict')

def create_switching_dfs(technology_list: list) -> dict:
    """Creates a dictionary that hold a DataFrame with three columns fo each tehnology passed to it in a list.

    Args:
        technology_list (list): A list of technologies that will be the dictionary keys.

    Returns:
        dict: A dictionary with each key as the technologies.
    """
    logger.info(f'Creating the base switching dict')
    df_dict = {}
    for technology in technology_list:
        df_temp = pd.DataFrame(data={'Start Technology': technology, 'New Technology': technology_list, 'value': ''})
        df_dict[technology] = df_temp
    return df_dict



def get_capex_values(
    df_switching_dict: dict, capex_dict_ref: dict, year: int) -> dict:
    """Assign values to the a DataFrame based on start and potential switching technology.

    Args:
        df_switching_dict (dict): A dictionary with each technology as the key
        capex_dict_ref (dict): A dictionary with greenfield, brownfield and other_opex values for each technology
        year (int):

    Returns:
        dict: A dictionary with vales
    """
    logger.info(f'Gnerating the capex values for each technology')
    df_dict_c = df_switching_dict.copy()

    def get_hard_code_capex(year : int):
        if year == 2020 or year == 2030:
            return 319.249187119815
        return 286.218839300307

    for technology in SWITCH_DICT.keys():
        logger.info(f'-- Generating Capex values for {technology}')
        for new_technology in SWITCH_DICT[technology]:
            df_temp = df_dict_c[technology].copy()
            capex_difference = capex_generator(capex_dict_ref, new_technology, year)['greenfield'] - capex_generator(capex_dict_ref, technology, year)['greenfield']
            if technology == new_technology:
                brownfield_capex_value = capex_generator(capex_dict_ref, technology, year)['brownfield']
                df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == technology), 'value'] = brownfield_capex_value
                df_dict_c[technology] = df_temp
            elif new_technology == 'Close plant':
                switch_capex_value = capex_generator(capex_dict_ref, technology, year)['greenfield']*0.05
                df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                df_dict_c[technology] = df_temp
            else:
                if technology in FURNACE_GROUP_DICT['blast_furnace'] and new_technology in FURNACE_GROUP_DICT['blast_furnace']:
                    if new_technology == 'BAT BF-BOF':
                        switch_capex_value = capex_generator(capex_dict_ref, new_technology, year)['brownfield']
                        df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                        df_dict_c[technology] = df_temp
                    elif new_technology == 'BAT BF-BOF+CCUS' or new_technology == 'BAT BF-BOF+CCU' or new_technology == 'BAT BF-BOF+BECCUS':
                        if technology == 'Avg BF-BOF':
                            switch_capex_value = capex_generator(capex_dict_ref, 'BAT BF-BOF', year)['brownfield']+capex_difference
                            df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                            df_dict_c[technology] = df_temp
                        elif technology =='BAT BF-BOF_bio PCI' or technology== 'BAT BF-BOF_H2 PCI':
                            switch_capex_value = get_hard_code_capex(year)
                            df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                            df_dict_c[technology] = df_temp

                        else: #if technology is BAT BF BOF
                            switch_capex_value = capex_generator(capex_dict_ref, 'BAT BF-BOF', year)['brownfield'] + capex_difference
                            df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                            df_dict_c[technology] = df_temp
                    else: #bio PCI or H2PCI
                        if technology == 'Avg BF-BOF':
                            switch_capex_value=195
                            df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                            df_dict_c[technology] = df_temp
                        else: #technology is BAT BF BOF
                            switch_capex_value = capex_generator(capex_dict_ref, technology, year)['brownfield']
                            df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                            df_dict_c[technology] = df_temp

                elif technology in FURNACE_GROUP_DICT['dri-bof'] and new_technology in FURNACE_GROUP_DICT['dri-bof']:
                    if new_technology == 'DRI-Melt BOF 100% zero CH2':
                        switch_capex_value = capex_generator(capex_dict_ref, technology, year)['brownfield']
                        df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                        df_dict_c[technology] = df_temp
                    else: # 'DRI-Melt BOF + CCUS'
                        switch_capex_value=capex_generator(capex_dict_ref, technology, year)['brownfield']+capex_difference
                        df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                        df_dict_c[technology] = df_temp

                elif technology in FURNACE_GROUP_DICT['dri-eaf'] and new_technology in FURNACE_GROUP_DICT['dri-eaf']:
                    if new_technology == 'DRI-EAF 50% bio-CH4' or new_technology == 'DRI-EAF 50% green H2' or new_technology == 'DRI-EAF 100% green H2':
                        switch_capex_value = capex_generator(capex_dict_ref, technology, year)['brownfield']
                        df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                        df_dict_c[technology] = df_temp
                    else:# new_technology='DRI-EAF+CCUS':
                        switch_capex_value=capex_generator(capex_dict_ref, technology, year)['brownfield'] + capex_difference
                        df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                        df_dict_c[technology] = df_temp

                elif technology in FURNACE_GROUP_DICT['smelting_reduction'] and new_technology in FURNACE_GROUP_DICT['smelting_reduction']:
                    switch_capex_value = capex_generator(capex_dict_ref, technology, year)['brownfield'] + capex_difference
                    df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                    df_dict_c[technology] = df_temp


                elif technology in FURNACE_GROUP_DICT['blast_furnace'] and new_technology in FURNACE_GROUP_DICT['dri-bof']:
                    if technology== 'Avg BF-BOF'or technology== 'BAT BF-BOF' and new_technology =='DRI-Melt-BOF' or new_technology =='DRI-Melt-BOF_100% zero-C H2':#check H2 assumption with Rafal
                        switch_capex_value = capex_generator(capex_dict_ref, 'DRI-EAF', year)['greenfield'] - capex_generator(capex_dict_ref, 'EAF', year)['greenfield']
                        df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                        df_dict_c[technology] = df_temp

                    elif technology== 'Avg BF-BOF' or technology == 'BAT BF-BOF' or technology == 'BAT BF-BOF_bio PCI' or technology == 'BAT BF-BOF_H2 PCI' and new_technology =='DRI-Melt-BOF+CCUS':
                        switch_capex_value = capex_generator(capex_dict_ref, new_technology, year)['greenfield']-460/4
                        df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                        df_dict_c[technology] = df_temp
                else:
                    switch_capex_value = capex_generator(capex_dict_ref, new_technology, year)['greenfield']
                    df_temp.loc[(df_temp['Start Technology'] == technology) & (df_temp['New Technology'] == new_technology), 'value'] = switch_capex_value
                    df_dict_c[technology] = df_temp

    return df_dict_c

def create_capex_timeseries(serialize_only: bool = False) -> dict:
    """Runs through the flow to create a capex dictionary.

    Args:
        serialize_only (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary with 
    """
    logger.info('Creating the base switching dict')
    switching_dict = create_switching_dfs(TECH_REFERENCE_LIST)
    switching_dict_with_capex = get_capex_values(switching_dict, capex_dict, 2020)
    if serialize_only:
        serialize_df(switching_dict_with_capex, PKL_FOLDER, 'capex_switching_dict')
    return switching_dict_with_capex
