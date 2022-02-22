def calc_trade_flows(
    year_end: int,
    interregional_acticity: bool = True,
    steel_demand_scenario: str =scenario_dict["steel_demand_scenario"], #receive tis info from scenario dict
    region_select: str = 'RMI Model Region',
    cost_of_steelmaking: dict = cost-cost_of_steelmaking,
    potential_trade_flows:dict = potential_trade_flows
) -> dict:
    """_summary_

    Args:
        year_end (int): _description_
        interregional_acticity (bool, optional): _description_. Defaults to True.
        steel_demand_scenario (str, optional): _description_. Defaults to 'bau'.

    Returns:
        dict: _description_
    """
    # Demand data
    steel_demand_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted", "df" #Q:is this reading the bau scenario?
    )
    # Plant capacities
    plant_capacities_dict = create_plant_capacities_dict()
    # Steel plants
    steel_plant_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "steel_plants_processed", "df"
    )
    # General Reference data
    year_range = range(MODEL_YEAR_START, year_end + 1)
    region_df = read_pickle_folder (PKL_DATA_IMPORTS, region_select, 'df')
    region_list = region_df[region_select].to_list() #HM insert whatever selection made in region_select
    current_trade_flows = {}

    for year in tqdm(year_range, total = len(year_range), desc="Years"):
        for region in  tqdm(region_list, total=len(region_list), desc='Regions' ):  #HM insert list of regions
            logger.info(f'Calculating trade flows for {year}')
            current_trade_flows[str(year)][region] = {}

            potential_importers = #all the regions under staus import in potential trade flows
            potential_exporters = #all the regions under staus export in potential trade flows
            non_porters = # all the regions that are not in exporters or importers


            if potential_importers[year].agg(sum)<= (potential_exporters[year].agg(sum)+current_trade_flows[year-1].agg(sum)):
                current_trade_flows[str(year)][region] = -potential_importers # write everything in current_trade_flow_dict but negativeto indicate outflow of global steel market to importing region
                current_trade_flows[str(year)][region] = +potential_exporters
                
                #remaining steel stays in global steel market 

            else: # potential imports > potential exports 
                #unmet_demand=potential importers.agg(sum)- potential_exporters.agg(sum)
                # to meet unmet demand we need to ramp up production in another region 
                # ramp up production in region with cheapest cost of steelmaking (with either 0 capacity (building GF) or with capa <95%)
                #check: does this region have capacity already?
                #yes: 
                # new_region_cuf[year][region]= unmet_demand[year]/region_capacity_dict[year][region] 
                #overwrite region_cuf for that region globaly and recalculate production in that region
                #no:
                #call build_plant function
                #add capacity to that region and add plant to steel plant_df
                #could it happen that you ramp up production in a region now although you reduced cuf or capacity in that region already in trade_production?
                # then we have enough extra production so:
                #current_trade_flows[str(year)][region] = -potential_importers # write everything in current_trade_flow_dict but negativeto indicate outflow of global steel market to importing region
                #current_trade_flows[str(year)][region] = +potential_exporters

                #logic for prioritising imports not necessary anymore ???? bc have enough production now
                #get all potential import regions and rank based on cost of steemaking as average over all techs in that region in descenading order
                #while potential exporters.agg(sum)>0:
                    #current_trade_flows[year][region]=-potential_importers proceeding in order of ranked potential imorters
                
                # 
                 


@timer_func
def trade_flow(scenario_dict: dict, year_end: int, serialize: bool = False) -> dict:
    """[summary]

    Args:
        year_end (int): [description]
        serialize (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """

    trade_flow_dict = calc_trade_flows(
        year_end=year_end,
        interregional_acticity= scenario_dict['interregional_activity'], #HM set in scenario dict
        #solver_logic=SOLVER_LOGICS[scenario_dict["solver_logic"]],
        #tech_moratorium=scenario_dict["tech_moratorium"],
        #enforce_constraints=scenario_dict["enforce_constraints"],
        steel_demand_scenario=scenario_dict["steel_demand_scenario"],
        #trans_switch_scenario=scenario_dict["transitional_switch"],
        #tech_switch_scenario=TECH_SWITCH_SCENARIOS[
            #scenario_dict["tech_switch_scenario"]
        #],
    )

    if serialize:
        logger.info(f"-- Serializing dataframes")
        serialize_file(trade_flow_dict, PKL_DATA_INTERMEDIATE, "trade_flow_dict")
    return trade_flow_dict