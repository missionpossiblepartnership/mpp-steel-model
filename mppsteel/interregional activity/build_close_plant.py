def build_plant (
    year: int = None, #or string??
    region: str=None,
    cost_of_steelmaking: dict = cost_of_steelmaking,
    build_plant_dict:dict = build_plant_dict,
    rate_tech_dict:dict =rate_tech_dict,
    #HM: define costof steel making dict

) -> dict:
    """_summary_

    Args:
        year_end (int): _description_

    Returns:
        dict: _description_
    """

    # Steel plants
    steel_plant_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df")

        
    for tech in tqdm(tech_list, total = len(tech_list), desc='Tech'):
        rate_tech_dict[year][region][tech]={}# Q: does this overwrite values from previous calls of the function?
        build_plant_dict[year][region]={}

        rate_tech_dict[year][region][tech]= cost_of_steelmaking[year][region][tech]/cost_of_steelmaking[year][global_avg][tech]
        # Q: should we make this forward looking? 10 years?
    #HM: define tech list
    build_tech = rate_tech_dict[year][region][tech].min() #chose the tech where factor =cost in region / cost global is min 
    #HM: append a new plant with {build_tech} in that {region} to steel_plant_df with (make sure its then fed into global model):

    # - global average capacity
    # - start it with a global average cuf 
    # - start year of operation is {year}
    build_plant_dict[year][regoin][new_plant_id]=build_tech
    build_plant_dict[year][region][new_plant_id][build_plant_capacity]=build_plant_capacity
    build_plant_dict[year][region][new_plant_id][cuf_start_year]=cuf_start_year

    return build_plant_dict, rate_tech_dict

def close_plant (
    year: int = None, #or string??
    region: str=None,
    tco: dict = tco,
    close_plant_dict:dict = close_plant_dict,
    rate_plant_dict:dict =rate_plant_dict,
    #HM: define tco dict
) -> dict:
    """_summary_

    Args:
        year (int, optional): _description_. Defaults to None.
        cost_of_steelmaking (dict, optional): _description_. Defaults to cost_of_steelmaking.
        build_plant_dict (dict, optional): _description_. Defaults to build_plant_dict.
        rate_tech_dict (dict, optional): _description_. Defaults to rate_tech_dict.

    Returns:
        dict: _description_
    """
    
    # Steel plants
    steel_plant_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df")

    for plant in region:# for each plant in that region 
        close_plant_dict[year][region][plant]={}
        rate_plant_dict[year][region][plant]={}
        rate_plant_dict[year][region][plant]= tco[year][region][plant]/tco[year][global_avg][plant]

        # Q: should we make this forward looking? 10 years?
    
    close_plant = rate_tech_dict[year][region][tech].max() #chose the plant where factor =cost in region / cost global is max 
    # plus add condition that only plant older than 11 yrs
    #HM: change status of {close_plant} in that {region} in steel_plant_df to "not operating" (make sure its then fed into global model):

    close_plant_dict[year][regoin][plant_id]=close_tech
    close_plant_dict[year][region][plant_id][close_plant_capacity]=close_plant_capacity
    close_plant_dict[year][region][plant_id][cuf_end_year]=cuf_end_year

    return close_plant_dict, rate_plant_dict