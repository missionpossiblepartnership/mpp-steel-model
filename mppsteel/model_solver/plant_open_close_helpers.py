"""Module with functions used in the plant open close flow"""

from copy import deepcopy
import math

import pandas as pd

import random

from mppsteel.config.reference_lists import TECH_REFERENCE_LIST
from mppsteel.model_solver.solver_constraints import tech_availability_check

from mppsteel.utility.location_utility import pick_random_country_from_region_subset
from mppsteel.utility.utils import replace_dict_items, get_dict_keys_by_value, get_closest_number_in_list
from mppsteel.plant_classes.plant_container_class import PlantIdContainer
from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter

from mppsteel.config.model_config import (
    MAIN_REGIONAL_SCHEMA,
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    TRADE_ROUNDING_NUMBER
)

from mppsteel.model_solver.solver_classes import (
    CapacityContainerClass,
    UtilizationContainerClass,
    MaterialUsage,
    apply_constraints_for_min_cost_tech,
)
from mppsteel.trade_module.trade_helpers import utilization_boundary, get_initial_utilization

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


def least_consuming_tech(
    business_case_ref: dict,
    tech_availability: pd.DataFrame,
    resource_to_optimize: str,
    year: int,
    tech_moratorium: bool = False
):
    keys = [key for key in business_case_ref if f"{resource_to_optimize}" in key]
    combined_available_list = [
        tech
        for tech in TECH_REFERENCE_LIST
        if tech_availability_check(
            tech_availability, tech, year, tech_moratorium=tech_moratorium
        )
    ]
    resource_value_dict = {k[0]: business_case_ref[k] for k in keys if k[0] in combined_available_list}
    return min(resource_value_dict, key=resource_value_dict.get)


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
        year (int): The current model year.
        region (str): The region for consideration.
        plant_capacity (float): The average capacity for plants in the specified `region` and/or `plant_name`.
        plant_name (str): The name of the plant.
        enforce_constraints (bool, optional): Scenario boolean flag for whether resource constraints are enforced. Defaults to False.

    Returns:
        str: The name of the lowest cost technology archetype.
    """
    lcost_df_c = lcost_df.loc[year, region].groupby("technology").mean().copy()
    assert lcost_df_c.isnull().values.any() == False, f"DF entry has nans: {lcost_df_c}"
    lowest_cost_tech = lcost_df_c["levelized_cost"].idxmin()

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
        scrap_balance = material_container.get_current_balance(year, "scrap")

        if regional_scrap:
            scrap_balance = (material_container.get_current_balance(year, "scrap", region))
        # handle case if potential technologies is empty due to resource constraints
        if not potential_technologies:
            # if no scrap, append least scrap consuming technology
            if scrap_balance <= 0:
                lowest_scrap_resource = least_consuming_tech(
                    business_case_ref,
                    tech_availability,
                    "Scrap",
                    year,
                    tech_moratorium=tech_moratorium
                )
                potential_technologies.append(lowest_scrap_resource)
            # if scrap, append least cost technology regardless of other non-scrap constraints
            else:
                lowest_cost_tech = lcost_df_c["levelized_cost"].idxmin()
                potential_technologies.append(lowest_cost_tech)
        lcost_df_c = lcost_df_c[lcost_df_c.index.isin(potential_technologies)]
        lowest_cost_tech = lcost_df_c["levelized_cost"].idxmin()
        # print(f"lowest cost technologies: {lowest_cost_tech}")

    return lowest_cost_tech


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


def check_year_range_for_switch_type(tech_choices_dict: pd.DataFrame, plant_name: str, list_with_years: list, switch_type: str):
    df_c = tech_choices_dict[tech_choices_dict["plant_name"] == plant_name].copy()
    if list_with_years:
        check_df = df_c[df_c["year"].isin(list_with_years)].copy()
        if switch_type in check_df.switch_type.unique():
            return check_df[check_df["switch_type"] == switch_type]["year"]
    return None

def get_trans_switch_range(list_of_ranges: list, number_to_check: int) -> list:
    for year_range in list_of_ranges:
        if number_to_check in year_range:
            return list(range(year_range[0], number_to_check + 1))
    return []


def get_closest_year_main_switch(list_with_years: list, year_to_check: int):
    my_list = [year for year in list_with_years if year <= year_to_check]
    if my_list:
        return get_closest_number_in_list(my_list, year_to_check)
    else:
        return None


def get_closest_year_trans_switch(list_of_ranges, tech_choices_dict, year_to_check, plant_name):
    list_with_years = get_trans_switch_range(
        list_of_ranges,
        year_to_check
    )
    return check_year_range_for_switch_type(
        tech_choices_dict,
        plant_name,
        list_with_years,
        "Transitional switch in off-cycle investment year"
    )


def current_plant_year(
    investment_dict: pd.DataFrame,
    tech_choices_dict: pd.DataFrame,
    plant_start_years: dict,
    plant_cycle_lengths: dict,
    plant_name: str,
    current_year: int,
) -> int:
    """Returns the age of a plant since its last main investment cycle year.

    Args:
        investment_dict (pd.DataFrame): Dictionary with plant names as keys and main investment cycles as values.
        plant_name (str): The name of the plant.
        current_year (int): The current model cycle year.

    Returns:
        int: The age (in years) of the plant.
    """
    plant_start_year = plant_start_years[plant_name]
    if current_year <=  plant_start_year:
        return 0
    main_cycle_years = [yr for yr in investment_dict[plant_name] if isinstance(yr, int)]
    trans_years = [yr for yr in investment_dict[plant_name] if isinstance(yr, range)]
    main_switch_year = get_closest_year_main_switch(main_cycle_years, current_year)
    trans_switch_year = get_closest_year_trans_switch(trans_years, tech_choices_dict, current_year, plant_name)
    potential_investment_years = [num for num in [main_switch_year, trans_switch_year] if isinstance(num, int)]
    # print(f"plant_name: {plant_name} | plant_start_year: {plant_start_year} | main_cycle_years: {main_cycle_years} | main_switch_year: {main_switch_year} | trans_switch_years: {trans_years} | trans_switch_year: {trans_switch_year} | potential_investment_years: {potential_investment_years}")
    closest_investment_year = get_closest_number_in_list(potential_investment_years, current_year)
    year_remainder = (current_year - plant_start_year) % plant_cycle_lengths[plant_name]
    return current_year - closest_investment_year if closest_investment_year else year_remainder


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
    investment_dict: dict, tech_choices_dict: pd.DataFrame, plant_start_years: dict, plant_cycle_lengths: dict, current_year: int, plant_list: list = None
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
        plant_name: current_plant_year(
            investment_dict, 
            tech_choices_dict,
            plant_start_years,
            plant_cycle_lengths,
            plant_name, current_year
        )
        for plant_name in plant_list
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
    region_list = capacity_container.regional_capacities_agg[year]
    cases = {region: [] for region in region_list}

    for region in region_list:

        demand = steel_demand_getter(
            steel_demand_df, year=year, metric="crude", region=region
        )
        capacity = capacity_container.return_regional_capacity(year, region)
        initial_utilization = get_initial_utilization(utilization_container, year, region)
        bounded_utilization = utilization_boundary(initial_utilization, capacity_util_min, capacity_util_max)
        avg_plant_capacity_value = deepcopy(avg_plant_global_capacity)

        avg_plant_capacity_at_max_production = (
            avg_plant_capacity_value * capacity_util_max
        )

        initial_min_utilization_reqiured = round(demand / capacity, TRADE_ROUNDING_NUMBER)

        new_capacity_required = 0
        excess_capacity = 0
        new_plants_required = 0
        plants_to_close = 0

        if capacity_util_min <= initial_min_utilization_reqiured <= capacity_util_max:
            cases[region].append("INCREASE CAPACITY: Capacity can be adjusted to meet demand")
            new_total_capacity = deepcopy(capacity)
            new_min_utilization_required = demand / capacity

        elif initial_min_utilization_reqiured < capacity_util_min:
            cases[region].append("CLOSE PLANT: Excess capacity even in lowest utilization option")
            required_capacity = demand / capacity_util_min
            excess_capacity = capacity - required_capacity
            plants_to_close = math.ceil(
                excess_capacity / avg_plant_capacity_value
            )
            new_total_capacity = capacity - (
                plants_to_close * avg_plant_capacity_value
            )
            new_min_utilization_required = demand / new_total_capacity
            new_min_utilization_required = utilization_boundary(
                new_min_utilization_required, capacity_util_min, capacity_util_max
            )
            new_capacity_required = -(capacity - new_total_capacity)

        elif initial_min_utilization_reqiured > capacity_util_max:
            cases[region].append("OPEN PLANT: Capacity adjustment not enough to meet demand")
            new_capacity_required = demand - (capacity * capacity_util_max)
            new_plants_required = math.ceil(
                new_capacity_required / avg_plant_capacity_at_max_production
            )
            new_total_capacity = capacity + (
                new_plants_required * avg_plant_capacity_value
            )
            new_min_utilization_required = utilization_boundary(
                demand / new_total_capacity, capacity_util_min, capacity_util_max
            )

        utilization_container.update_region(year, region, new_min_utilization_required)

        initial_utilized_capacity = capacity * initial_utilization
        initial_utilized_capacity_bounded = capacity * bounded_utilization
        new_utilized_capacity = new_total_capacity * new_min_utilization_required
        initial_balance_value = initial_utilized_capacity_bounded - demand
        new_balance_value = new_utilized_capacity - demand

        # RETURN RESULTS
        region_result = {
            "year": year,
            "region": region,
            "capacity": capacity,
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
            "cases": []
        }

        results_container[region] = region_result

        assert round(demand, TRADE_ROUNDING_NUMBER) == round(new_utilized_capacity, TRADE_ROUNDING_NUMBER), f"Demand - Production Imbalance for {region} -> Demand: {demand : 2f} Production: {new_utilized_capacity: 2f} Case: {cases[region]}"

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
        f"Market Balance Results for {year}: Capacity: {capacity_sum :0.2f} | Production: {production_sum :0.2f} | Demand: {demand_sum :0.2f} | New Plants: {plants_required} | Closed Plants {plants_to_close}"
    )
    assert capacity_sum > demand_sum
    assert capacity_sum > production_sum
    assert production_sum >= demand_sum

def create_and_test_market_df(market_dict: dict, year: int, test_df: bool = False) -> pd.DataFrame:
    df = pd.DataFrame(market_dict.values()).set_index(["year", "region"]).round(3)
    if test_df:
        market_balance_test(df, year)
    return df

def create_test_production_df():
        # just a minimal dataframe to make the empty test pass FIXME
        my_minimal_row = [2020, "DEU", 0, 1, 0, 0, 0]
        my_df_columns = [
            "year", "region", "demand", "new_total_capacity", 
            "new_utilized_capacity", "plants_required", "plants_to_close"
        ]
        return  pd.DataFrame([my_minimal_row], columns=my_df_columns)
