"""Module that determines functionality for opening and closing plants"""

import math

import pandas as pd

import random
from mppsteel.config.reference_lists import TECH_REFERENCE_LIST
from mppsteel.data_preprocessing.investment_cycles import PlantInvestmentCycle

from mppsteel.utility.location_utility import pick_random_country_from_region_subset
from mppsteel.utility.utils import replace_dict_items, get_dict_keys_by_value
from mppsteel.utility.plant_container_class import PlantIdContainer
from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter

from mppsteel.config.model_config import (
    MAIN_REGIONAL_SCHEMA,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    MEGATON_TO_KILOTON_FACTOR,
    MODEL_YEAR_START,
)

from mppsteel.model_solver.solver_classes import (
    CapacityContainerClass,
    UtilizationContainerClass,
    MarketContainerClass,
    PlantChoices,
    MaterialUsage,
    apply_constraints_for_min_cost_tech,
)
from mppsteel.model_solver.trade import trade_flow

from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def create_new_plant(plant_row_dict: dict, plant_columns: list) -> dict:
    """Creates metadata for a new plant based on defined key_value pairs in `plant_row_dict` and fills the remaining keys from `plant_columns` with a blank string.

    Args:
        plant_row_dict (dict): Key value pairs with the new plant metadata
        plant_columns (list): A list of all the plant columns necessary to add a new plant as a row to the steel plant database.

    Returns:
        dict: A dictionary with keys from `plant_columns` and values from `plant_columns`
    """
    base_dict = {col: "" for col in plant_columns}
    return replace_dict_items(base_dict, plant_row_dict)


def get_min_cost_tech_for_region(
    lcost_df: pd.DataFrame,
    business_case_ref: dict,
    plant_capacities_dict: dict,
    tech_availability: pd.DataFrame,
    material_container: MaterialUsage,
    tech_moratorium: bool,
    regional_scrap: bool,
    year: int,
    region: str,
    plant_capacity: float,
    plant_name: str,
    enforce_constraints: bool = False,
) -> str:
    """Gets the minimum cost technology for plants in a specified `year` and `region` based on a Levelised Cost Reference.

    Args:
        lcost_df (pd.DataFrame): Levelized Cost Reference DataFrame.
        business_case_ref (dict): Business Case Reference dictionary
        plant_capacities_dict (dict): A Dictionary with plants as keys and capacity values as values.
        tech_availability (pd.DataFrame): DataFrame of technology availability metadata
        material_container (MaterialUsage): A material usage class containing information on constraints and current usage.
        tech_moratorium (bool): Scenario boolean flag for whether there is a technology moratorium in effect.
        regional_scrap (bool): Scenario boolean flag for whether there is a regional scrap or global scrap constraint.
        year (int): The current model year.
        region (str): The region for consideration.
        plant_capacity (float): The average capacity for plants in the specified `region` and/or `plant_name`.
        plant_name (str): The name of the plant.
        enforce_constraints (bool, optional): Scenario boolean flag for whether resource constraints are enforced. Defaults to False.

    Returns:
        str: The name of the lowest cost technology archetype.
    """
    lcost_df_c = lcost_df.loc[year, region].groupby("technology").mean().copy()

    if enforce_constraints:
        potential_technologies = apply_constraints_for_min_cost_tech(
            business_case_ref,
            plant_capacities_dict,
            tech_availability,
            material_container,
            TECH_REFERENCE_LIST,
            plant_capacity,
            tech_moratorium,
            regional_scrap,
            year,
            plant_name,
            region,
        )
        lcost_df_c = lcost_df_c[lcost_df_c.index.isin(potential_technologies)]

    return lcost_df_c["levelized_cost"].idxmin()


def get_min_cost_region(lcost_df: pd.DataFrame, year: int) -> str:
    """Gets the minimum cost region based on a Levelized cost reference.

    Args:
        lcost_df (pd.DataFrame): The levelized cost DataFrame.
        year (int): The current model year.

    Returns:
        str: The name of the lowest cost region for a particular year.
    """
    lcost_df_c = lcost_df.loc[year].groupby("region").mean().copy()
    return lcost_df_c["levelized_cost"].idxmin()


def current_plant_year(
    investment_dict: pd.DataFrame,
    plant_name: str,
    current_year: int,
    cycle_length: int = 20,
) -> int:
    """Returns the age of a plant since its last main investment cycle year.

    Args:
        investment_dict (pd.DataFrame): Dictionary with plant names as keys and main investment cycles as values.
        plant_name (str): The name of the plant.
        current_year (int): The current model cycle year.
        cycle_length (int, optional): The length of the plants investment cycle. Defaults to 20.

    Returns:
        int: The age (in years) of the plant.
    """
    main_cycle_years = [yr for yr in investment_dict[plant_name] if isinstance(yr, int)]
    first_inv_year = main_cycle_years[0]
    if len(main_cycle_years) == 2:
        second_inv_year = main_cycle_years[1]
        cycle_length = second_inv_year - first_inv_year

    trans_years = [yr for yr in investment_dict[plant_name] if isinstance(yr, range)]
    if trans_years:
        first_trans_years = list(trans_years[0])
        if first_trans_years:
            potential_start_date = first_trans_years[0]
            if current_year in first_trans_years:
                return current_year - potential_start_date

    if current_year < first_inv_year:
        potential_start_date = first_inv_year - cycle_length
        return current_year - potential_start_date

    if len(main_cycle_years) == 1:
        if current_year >= first_inv_year:
            return current_year - first_inv_year

    if len(main_cycle_years) == 2:
        if current_year >= first_inv_year <= second_inv_year:
            if not trans_years:
                return current_year - first_inv_year
            else:
                if current_year in first_trans_years:
                    return current_year - potential_start_date
                else:
                    return current_year - second_inv_year
        elif current_year > second_inv_year:
            return current_year - second_inv_year


def new_plant_metadata(
    plant_container: PlantIdContainer,
    production_demand_dict: dict,
    levelized_cost_df: pd.DataFrame,
    plant_df: pd.DataFrame,
    ng_mapper: dict,
    year: int,
    region: str = None,
    low_cost_region: bool = False,
) -> dict:
    """Creates the essential metadata fields for a new plant.

    Args:
        plant_container (PlantIdContainer): Plant Container class containing a track of plants and their unique IDs.
        production_demand_dict (dict): Dictionary with the results of the utilization and open/close and trade optimization process.
        levelized_cost_df (pd.DataFrame): Levelized Cost Reference DataFrame.
        plant_df (pd.DataFrame): The steel plant DataFrame.
        ng_mapper (dict): Mapper that includes country codes as keys and natural gas boolean flag as values.
        year (int): The current model year.
        region (str, optional): The region for the new plant. Defaults to None.
        low_cost_region (bool, optional): The low cost region for the. Defaults to False.

    Raises:
        AttributeError: When values are entered for both `region` and `low_cost_region`.

    Returns:
        dict: A dictionary with key value fields for new plants.
    """

    if region and low_cost_region:
        raise AttributeError(
            "You entered a value for `region` and set `low_cost_region` to true. Select ONE or the other, NOT both."
        )
    new_id = plant_container.generate_plant_id(add_to_container=True)
    if low_cost_region:
        region = get_min_cost_region(levelized_cost_df, year=year)
    capacity_value = production_demand_dict[region]["avg_plant_capacity"]
    country_specific_mapper = {"China": "CHN", "India": "IND"}
    if region in country_specific_mapper:
        assigned_country = country_specific_mapper[region]
    else:
        assigned_country = pick_random_country_from_region_subset(plant_df, region)
    return {
        "plant_id": new_id,
        "plant_name": f"{new_id} - {assigned_country}",
        "status": "new model plant",
        "active_check": True,
        "start_of_operation": year,
        "country_code": assigned_country,
        "cheap_natural_gas": ng_mapper[assigned_country],
        "plant_capacity": capacity_value,
        "primary_capacity": "Y",
        MAIN_REGIONAL_SCHEMA: region,
    }


def return_oldest_plant(
    investment_dict: dict, current_year: int, plant_list: list = None
) -> str:
    """Gets the oldest plant from `plant_list` based on their respective investment cycles in `investment dict` and the `current_year`.
    If multiple plants have the same oldest age, then a plant is chosen at random.

    Args:
        investment_dict (pd.DataFrame): Dictionary with plant names as keys and main investment cycles as values.
        current_year (int): The current model year.
        plant_list (list, optional): A list of plant names. Defaults to None.

    Returns:
        str: The name of the oldest plant.
    """
    if not plant_list:
        plant_list = investment_dict.keys()
    plant_age_dict = {
        plant: current_plant_year(investment_dict, plant, current_year)
        for plant in plant_list
    }
    max_value = max(plant_age_dict.values())
    return random.choice(get_dict_keys_by_value(plant_age_dict, max_value))


def return_plants_from_region(plant_df: pd.DataFrame, region: str) -> list:
    """Gets plants from the same region.

    Args:
        plant_df (pd.DataFrame): The plant metadata dataframe.
        region (str): The requested region.

    Returns:
        list: A list of plants from the region selected.
    """
    return list(plant_df[plant_df[MAIN_REGIONAL_SCHEMA] == region]["plant_name"].values)


def return_modified_plants(
    ocp_df: pd.DataFrame, year: int, change_type: str = "open"
) -> pd.DataFrame:
    """Subsets the plant dataframe based on plants that have been newly opened in the current model year.

    Args:
        ocp_df (pd.DataFrame): The open close dataframe.
        year (int): The current model year.
        change_type (str, optional): The type of change to return values for. Can be either `open` or `close`. Defaults to 'open'.

    Returns:
        pd.DataFrame: A dataframe containing the subsetted data.
    """
    if change_type == "open":
        return ocp_df[
            (ocp_df["status"] == "new model plant")
            & (ocp_df["start_of_operation"] == year)
        ]
    elif change_type == "close":
        return ocp_df[(ocp_df["status"] == "decomissioned")]


def ng_flag_mapper(plant_df: pd.DataFrame, country_ref: pd.DataFrame) -> dict:
    """Generates a mapper of country codes as keys and natural gas encodings as values.

    Args:
        plant_df (pd.DataFrame): The steel plant dataframe/
        country_ref (pd.DataFrame): The country reference dataframe.

    Returns:
        dict: A dictionary of country codes as keys and natural gas encodings as values.
    """
    df = (
        plant_df[["country_code", "cheap_natural_gas"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .copy()
    )
    initial_mapper = dict(zip(df["country_code"], df["cheap_natural_gas"]))
    final_values = country_ref["country_code"].apply(lambda x: initial_mapper.get(x, 0))
    ng_mapper = dict(zip(country_ref["country_code"], final_values))
    ng_mapper["TWN"] = 0
    return ng_mapper


def production_demand_gap(
    steel_demand_df: pd.DataFrame,
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    year: int,
    capacity_util_max: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    capacity_util_min: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
) -> dict:
    """Generates an open close dictionary of metadata for each region. The following optimization steps are taken by the algorithm.
    1) Determine whether a plant can meet its current regional demand with its current utilization levels.
    2) Optimize the utilization levels accordingly if possible
    3) If not possible, open OR close plants as required to meet the regional demand.

    Args:
        steel_demand_df (pd.DataFrame): The Steel Demand DataFrame.
        capacity_container (CapacityContainerClass): The CapacityContainerClass Instance containing the capacity state.
        utilization_container (UtilizationContainerClass): The UtilizationContainerClass Instance containing the utilization state.
        year (int): The current model cycle year.
        capacity_util_max (float, optional): The maximum capacity utilization that plants are allowed to reach before having to open new plants. Defaults to CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION.
        capacity_util_min (float, optional): The minimum capacity utilization that plants are allowed to reach before having to close existing plants. Defaults to CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION.

    Returns:
        dict: A dictionary of open close metadata for each region.
    """
    logger.info(f"Defining the production demand gap for {year}")
    # SUBSETTING & VALIDATION
    avg_plant_global_capacity = capacity_container.return_avg_capacity_value()

    results_container = {}

    for region in capacity_container.regional_capacities_agg[year]:
        initial_utilization = (
            utilization_container.get_utilization_values(year, region)
            if year == MODEL_YEAR_START
            else utilization_container.get_utilization_values(year - 1, region)
        )
        demand = steel_demand_getter(
            steel_demand_df, year=year, metric="crude", region=region
        )
        current_capacity = capacity_container.return_regional_capacity(year, region)

        avg_plant_capacity_value = avg_plant_global_capacity

        avg_plant_capacity_at_max_production = (
            avg_plant_capacity_value * capacity_util_max
        )

        new_capacity_required = 0
        excess_capacity = 0
        new_plants_required = 0
        plants_to_close = 0
        new_total_capacity = 0

        initial_min_utilization_reqiured = demand / current_capacity
        new_min_utilization_required = 0

        if capacity_util_min <= initial_min_utilization_reqiured <= capacity_util_max:
            # INCREASE CAPACITY: Capacity can be adjusted to meet demand
            new_total_capacity = current_capacity
            new_min_utilization_required = initial_min_utilization_reqiured

        elif initial_min_utilization_reqiured < capacity_util_min:
            # CLOSE PLANT: Excess capacity even in lowest utilization option
            excess_capacity = (current_capacity * capacity_util_min) - demand
            plants_to_close = math.ceil(
                excess_capacity / avg_plant_capacity_at_max_production
            )
            new_total_capacity = current_capacity - (
                plants_to_close * avg_plant_capacity_value
            )
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = max(
                new_min_utilization_required, capacity_util_min
            )

        elif initial_min_utilization_reqiured > capacity_util_max:
            # OPEN PLANT: Capacity adjustment not enough to meet demand
            new_capacity_required = demand - (current_capacity * capacity_util_max)
            new_plants_required = math.ceil(
                new_capacity_required / avg_plant_capacity_at_max_production
            )
            new_total_capacity = current_capacity + (
                new_plants_required * avg_plant_capacity_value
            )
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = min(
                new_min_utilization_required, capacity_util_max
            )

        utilization_container.update_region(year, region, new_min_utilization_required)

        initial_utilized_capacity = current_capacity * initial_utilization
        new_utilized_capacity = new_total_capacity * new_min_utilization_required
        initial_balance_value = initial_utilized_capacity - demand
        new_balance_value = new_utilized_capacity - demand

        # RETURN RESULTS
        region_result = {
            "year": year,
            "region": region,
            "capacity": current_capacity,
            "initial_utilized_capacity": initial_utilized_capacity,
            "demand": demand,
            "initial_balance": initial_balance_value,
            "initial_utilization": initial_utilization,
            "avg_plant_capacity": avg_plant_capacity_value,
            "new_capacity_required": new_capacity_required,
            "plants_required": new_plants_required,
            "plants_to_close": plants_to_close,
            "new_total_capacity": new_total_capacity,
            "new_utilized_capacity": new_utilized_capacity,
            "new_balance": new_balance_value,
            "new_utilization": new_min_utilization_required,
            "unit": "Mt",
        }

        results_container[region] = region_result

    return results_container


def market_balance_test(
    production_supply_df: pd.DataFrame, year: int, rounding: int = 0
) -> bool:
    """A test function that checks whether the following inequality holds within the Open Close DataFrame.
    Capacity > Production >= Demand

    Args:
        production_supply_df (pd.DataFrame): _description_
        year (int): _description_
        rounding (int, optional): _description_. Defaults to 0.

    Returns:
        bool: Returns True is all inequalities hold.
    """
    demand_sum = round(production_supply_df["demand"].sum(), rounding)
    capacity_sum = round(production_supply_df["new_total_capacity"].sum(), rounding)
    production_sum = round(
        production_supply_df["new_utilized_capacity"].sum(), rounding
    )
    plants_required = production_supply_df["plants_required"].sum()
    plants_to_close = production_supply_df["plants_to_close"].sum()
    logger.info(
        f"Trade Results for {year}: Demand: {demand_sum :0.2f}  | Capacity: {capacity_sum :0.2f} | Production: {production_sum :0.2f}  | New Plants: {plants_required} | Closed Plants {plants_to_close}"
    )
    assert capacity_sum > demand_sum
    assert capacity_sum > production_sum
    assert production_sum >= demand_sum


def open_close_plants(
    steel_demand_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    country_df: pd.DataFrame,
    lev_cost_df: pd.DataFrame,
    business_case_ref: pd.DataFrame,
    tech_availability: pd.DataFrame,
    variable_costs_df: pd.DataFrame,
    capex_dict: dict,
    capacity_container: CapacityContainerClass,
    utilization_container: UtilizationContainerClass,
    material_container: MaterialUsage,
    tech_choices_container: PlantChoices,
    plant_id_container: PlantIdContainer,
    market_container: MarketContainerClass,
    investment_container: PlantInvestmentCycle,
    year: int,
    trade_scenario: bool = False,
    tech_moratorium: bool = False,
    regional_scrap: bool = False,
    enforce_constraints: bool = False,
    open_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    close_plant_util_cutoff: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
) -> pd.DataFrame:
    """Adjusts a Plant Dataframe by determining how each region should achieve its demand for the year.
    The function works by either
    1) Ensuring that each region can only fulfill its own demand (if `trade_scenario` is set to `False`)
    2) Using Trade rules to allow demand to be partially filled by other region's production capacity (if `trade_scenario` is set to `True`)
    3) Opens and/or closes plants based on steps 1 & 2.

    Args:
        steel_demand_df (pd.DataFrame): The steel demand DataFrame.
        steel_plant_df (pd.DataFrame): The steel plant DataFrame.
        lev_cost_df (pd.DataFrame): A levelized cost reference DataFrame.
        business_case_ref (dict): The business cases reference dictionary.
        tech_availability (pd.DataFrame): The technology availability reference.
        variable_costs_df (pd.DataFrame): The variable costs reference DataFrame.
        capex_dict (dict): The capex reference dictionary.
        capacity_container (CapacityContainerClass): The CapacityContainerClass Instance containing the capacity state.
        utilization_container (UtilizationContainerClass): The UtilizationContainerClass Instance containing the utilization state.
        material_container (MaterialUsage): The MaterialUsage Instance containing the material usage state.
        tech_choices_container (PlantChoices): The PlantChoices Instance containing the Technology Choices state.
        plant_id_container (PlantIdContainer): plant_container (PlantIdContainer): Plant Container class containing a track of plants and their unique IDs.
        market_container (MarketContainerClass): The MarketContainerClass Instance containing the Trade state.
        investment_container (PlantInvestmentCycle): The PlantInvestmentCycle Instance containing the investment cycle state.
        year (int): The current model year.
        trade_scenario (bool, optional): The scenario boolean value that determines whether there is a trade scenario. Defaults to False.
        tech_moratorium (bool, optional): The scenario boolean value that determines whether there is a technology moratorium. Defaults to False.
        regional_scrap (bool, optional): The scenario boolean value that determines whether there is a regional or global scrap constraints. Defaults to False.
        enforce_constraints (bool, optional): The scenario boolean value that determines if all constraints are enforced. Defaults to False.
        open_plant_util_cutoff (float, optional): The maximum capacity utilization that plants are allowed to reach before having to open new plants. Defaults to CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION.
        close_plant_util_cutoff (float, optional): The minimum capacity utilization that plants are allowed to reach before having to close existing plants. Defaults to CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION.

    Returns:
        pd.DataFrame: The initial DataFrame passed to the function with the plants that have been opened and close updated.
    """

    logger.info(f"Running open and close decisions for {year}")
    ng_mapper = ng_flag_mapper(steel_plant_df, country_df)
    investment_dict = investment_container.return_investment_dict()
    production_demand_gap_analysis = production_demand_gap(
        steel_demand_df=steel_demand_df,
        capacity_container=capacity_container,
        utilization_container=utilization_container,
        year=year,
        capacity_util_max=open_plant_util_cutoff,
        capacity_util_min=close_plant_util_cutoff,
    )

    prior_active_plants = steel_plant_df.copy()
    steel_plant_cols = prior_active_plants.columns

    if trade_scenario:
        logger.info(f"Starting the trade flow for {year}")
        production_demand_gap_analysis = trade_flow(
            market_container=market_container,
            production_demand_dict=production_demand_gap_analysis,
            utilization_container=utilization_container,
            capacity_container=capacity_container,
            variable_cost_df=variable_costs_df,
            plant_df=prior_active_plants,
            capex_dict=capex_dict,
            tech_choices_ref=tech_choices_container.return_choices(),
            year=year,
            util_min=close_plant_util_cutoff,
            util_max=open_plant_util_cutoff,
        )

    production_demand_gap_analysis_df = (
        pd.DataFrame(production_demand_gap_analysis.values())
        .set_index(["year", "region"])
        .round(3)
    )

    market_balance_test(production_demand_gap_analysis_df, year)
    market_container.store_results(year, production_demand_gap_analysis_df)

    regions = list(production_demand_gap_analysis.keys())
    random.shuffle(regions)
    levelized_cost_for_regions = (
        lev_cost_df.set_index(["year", "region"]).sort_index(ascending=True).copy()
    )
    levelized_cost_for_tech = (
        lev_cost_df.set_index(["year", "region", "technology"])
        .sort_index(ascending=True)
        .copy()
    )

    # REGION LOOP
    for region in regions:
        plants_required = production_demand_gap_analysis[region]["plants_required"]
        plants_to_close = production_demand_gap_analysis[region]["plants_to_close"]

        # OPEN PLANT
        if plants_required > 0:
            metadata_container = []
            for _ in range(plants_required):
                new_plant_meta = new_plant_metadata(
                    plant_id_container,
                    production_demand_gap_analysis,
                    levelized_cost_for_regions,
                    prior_active_plants,
                    ng_mapper,
                    year=year,
                    region=region,
                )
                new_plant_capacity = new_plant_meta["plant_capacity"]
                new_plant_name = new_plant_meta["plant_name"]
                dict_entry = create_new_plant(new_plant_meta, steel_plant_cols)
                xcost_tech = get_min_cost_tech_for_region(
                    levelized_cost_for_tech,
                    business_case_ref,
                    capacity_container.return_plant_capacity(year=year),
                    tech_availability,
                    material_container,
                    tech_moratorium,
                    regional_scrap,
                    year,
                    region,
                    new_plant_capacity,
                    new_plant_name,
                    enforce_constraints=enforce_constraints,
                )
                dict_entry["plant_capacity"] = (
                    new_plant_capacity * MEGATON_TO_KILOTON_FACTOR
                )
                dict_entry["initial_technology"] = xcost_tech
                metadata_container.append(dict_entry)
                tech_choices_container.update_choice(year, new_plant_name, xcost_tech)
            prior_active_plants = pd.concat(
                [prior_active_plants, pd.DataFrame(metadata_container)]
            ).reset_index(drop=True)

        # CLOSE PLANT
        if plants_to_close > 0:
            for _ in range(abs(plants_to_close)):
                # define cos function
                # rank descending
                # filter for age < 11 years
                # pick highest
                plant_to_close = return_oldest_plant(
                    investment_dict,
                    year,
                    return_plants_from_region(prior_active_plants, region),
                )
                idx_close = prior_active_plants.index[
                    prior_active_plants["plant_name"] == plant_to_close
                ].tolist()[0]
                prior_active_plants.loc[idx_close, "status"] = "decomissioned"
                prior_active_plants.loc[idx_close, "end_of_operation"] = year
                prior_active_plants.loc[idx_close, "active_check"] = False
                tech_choices_container.update_choice(
                    year, plant_to_close, "Close plant"
                )

    # dataframe_modification_test(prior_active_plants, production_demand_gap_analysis_df, year)
    new_active_plants = prior_active_plants[prior_active_plants["active_check"] == True]
    capacity_container.map_capacities(new_active_plants, year)
    regional_capacities = capacity_container.return_regional_capacity(year)
    global_demand = steel_demand_getter(
        steel_demand_df, year=year, metric="crude", region="World"
    )
    utilization_container.calculate_world_utilization(
        year, regional_capacities, global_demand
    )

    # Standardize utilization rates across each region to ensure global production = global demand
    world_utilization = utilization_container.get_utilization_values(year, "World")
    for region in regions:
        utilization_container.update_region(year, region, world_utilization)
    logger.info(
        f"Balanced Supply Demand results for {year}: Demand: {global_demand :0.2f}  | Production: {sum(regional_capacities.values()) * world_utilization :0.2f}"
    )
    new_open_plants = return_modified_plants(new_active_plants, year, "open")
    investment_container.add_new_plants(
        new_open_plants["plant_name"], new_open_plants["start_of_operation"]
    )
    return prior_active_plants


def dataframe_modification_test(
    plant_df: pd.DataFrame, pdga_df: pd.DataFrame, year: int, rounding: int = 1
) -> bool:
    """Test function that checks if the pre_optimized plant DataFrame has the same capacity as the newly optimized plant DataFrame.

    Args:
        plant_df (pd.DataFrame): The original steel plant DataFrame.
        pdga_df (pd.DataFrame): The newly optimized steel plant DataFrame
        year (int): The current model cycle year.
        rounding (int, optional): Rounds the DataFrame to 1. Defaults to 1.

    Returns:
        bool: Returns True if the inequality holds.
    """
    plant_df_capacity_sum = round(
        plant_df.set_index(["active_check"]).loc[True]["plant_capacity"].sum()
        / MEGATON_TO_KILOTON_FACTOR,
        rounding,
    )
    new_pdga_df_capacity_sum = round(pdga_df["new_total_capacity"].sum(), rounding)
    old_pdga_df_capacity_sum = round(pdga_df["capacity"].sum(), rounding)
    logger.info(
        f"Capacity equality check in {year} -> Pre-trade Capacity: {old_pdga_df_capacity_sum :0.2f} | Post-trade Capacity: {new_pdga_df_capacity_sum :0.2f} | Plant DF Capacity: {plant_df_capacity_sum :0.2f}"
    )
    plants_to_close = pdga_df["plants_to_close"].sum()
    plants_required = pdga_df["plants_required"].sum()
    if (plants_to_close == 0) & (plants_required == 0):
        assert plant_df_capacity_sum == old_pdga_df_capacity_sum
    elif plants_to_close > 0:
        pass
    else:
        assert plant_df_capacity_sum == new_pdga_df_capacity_sum
