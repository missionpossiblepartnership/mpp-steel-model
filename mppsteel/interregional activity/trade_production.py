import pandas as pd 
from tqdm import tqdm

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import return_furnace_group
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.data_loading.reg_steel_demand_formatter import extend_steel_demand

from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    PKL_DATA_IMPORTS,
    PKL_DATA_INTERMEDIATE,
)

from mppsteel.config.model_scenarios import TECH_SWITCH_SCENARIOS, SOLVER_LOGICS

from mppsteel.config.reference_lists import (
    SWITCH_DICT,
    TECHNOLOGY_STATES,
    FURNACE_GROUP_DICT,
    TECH_MATERIAL_CHECK_DICT,
    RESOURCE_CONTAINER_REF,
    TECHNOLOGY_PHASES,
)

from mppsteel.data_loading.data_interface import load_materials, load_business_cases

from mppsteel.model.solver_constraints import (
    tech_availability_check,
    read_and_format_tech_availability,
    plant_tech_resource_checker,
    create_plant_capacities_dict,
    material_usage_per_plant,
    load_resource_usage_dict,
)

from mppsteel.model.tco_and_abatement_optimizer import get_best_choice
from mppsteel.utility.log_utility import get_logger


def get_region_capacity(
    df: pd.DataFrame,
    region: str=None
) -> dict:
    """_summary_

    Args:
        df (pd.DataFrame): _description_

    Returns:
        dict: _description_
    """
    # Steel plants
    steel_plant_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df")
    df_c=steel_plant_df.copy()# df_c = df.copy and df when calling function is steel_plant_df
    year_range = range(MODEL_YEAR_START, year_end + 1)

    region_capacity_dict={}

    for year in tqdm(year_range, total=len(year_range), desc="Years"):
        
        for region in tqdm(region_list, total=len(region_list), desc='Region'):
            if year == 2020:
                region_list = steel_plant_df['region'].tolist()
                region_list = list(set(region_list))
                
            
            
                df_c=df_c.loc[df_c['region']==region]
                df_c['total_capacity']=df_c['primary_capacity_2020']+df_c['secondary_capacity_2020']
                df_c['region_capacity']=df_c['total_capacity'].cumsum()
                region_capacity=df_c['region_capacity'].iloc[-1]
                region_capacity_dict[str(year)][str(region)]={region_capacity}
            else:
                region_capacity_dict[str(year)][str(region)]=region_capacity_dict[str(year - 1)][str(region)] 
    return region_capacity_dict
    

def get_region_cuf(
    df: pd.DataFrame,
    region: str = None,
    region_capacity_dict: dict = region_capacity_dict,
) -> dict:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        region (str, optional): _description_. Defaults to None.

    Returns:
        dict: _description_
    """
    region_cuf={}
    for year in tqdm(year_range, total=len(year_range), desc="Years"):
        for region in tqdm(region_list, total=len(region_list), desc='Region'):
            region_cuf[str(year)] = {}
            if year == 2020:
                region_cuf[str(year)][region]=cuf2020 #get calculated cuf 2020 from world steel data
            else:
                region_cuf[str(year)][region]=region_cuf[str (year - 1)][region]
    return region_cuf 


def get_region_production(
    df: pd.DataFrame,
    region: str = None,
    region_cuf: dict = region_cuf
) -> dict:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        region (str, optional): _description_. Defaults to None.

    Returns:
        dict: _description_
    """
    for year in tqdm(year_range, total=len(year_range), desc="Years"):
        for region in tqdm(region_list, total=len(region_list), desc='Region'):
            production_dict={}
            if year == 2020:
                production_dict[str(year)][region]= pass #get production per region from calculated cfu2020
            else:
                production_dict[str(year)][region]=region_cuf[(year)][region]*region_capacity_dict[(year)][region]

            if production_dict[str(year)][region].value < demand[str(year)][region]:
                    # what happens if production < demand 
                if region_cuf[year][region] >=0.95:
                    if cost_of_steelmaking[year][region] < (cost_of_steelmaking[year][global_avg] * 1.1):
                        #Q: should we check this per tech or as an tech average?
                        pass  #build plant: call build plant function from build_close_plant.py
                    else:
                        pass #import: call import function from import_export.py
                else:
                        # increase capacity utilisation factor and check again
                        region_cuf[year][region]= demand[year][region]/region_capacity_dict[year][region] # overwrite cuf with new value
                        production_dict[year][region]=region_cuf[(year)][region]*region_capacity_dict[(year)][region]# calculate production again with new cuf
                        # HM insert if statement on what to do if cuf is bigger than 0.95 after that iteration
            elif production_dict[str(year)][region].value > demand[str(year)][region]:
                    #insert what happens if production > demand
                for plant in region:# for each plant in that region  
                    if cost_of_steelmaking[year][plant]< (cost_of_steelmaking[year][global_avg] * 1.1):
                        #Q: should we check this per tech or as an tech average?
                        pass     #export: call export function from import_export.py
                    else:
                        if region_cuf[year][region] > 0.6:
                                #reduce capacity utilisation factor and check again
                                region_cuf[year][region]= demand[year][region]/region_capacity_dict[year][region] # overwrite cuf with new value
                                production_dict[year][region]=region_cuf[(year)][region]*region_capacity_dict[(year)][region]  #calculate production with new cuf
                                # HM insert if statement on what to do id cuf is smaller then 0.6 after this iteration or condition not smaller than this and rest goes in export and you exept small price
                        else:
                            pass
                                #close down least cost competitive plant where age > 11years 
                                #call close plant function from build_close_plant.py
    
    return production_dict

