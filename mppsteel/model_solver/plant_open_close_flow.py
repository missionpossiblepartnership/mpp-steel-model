"""Module that determines functionality for opening and closing plants"""

import pandas as pd

import random

from mppsteel.plant_classes.plant_investment_cycle_class import PlantInvestmentCycle
from mppsteel.utility.utils import join_list_as_string
from mppsteel.plant_classes.plant_container_class import PlantIdContainer
from mppsteel.plant_classes.capacity_constraint_class import PlantCapacityConstraint
from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.model_solver.plant_open_close_helpers import (
    create_and_test_market_df,
    create_new_plant,
    get_min_cost_tech_for_region,
    new_plant_metadata,
    ng_flag_mapper,
    production_demand_gap,
    return_modified_plants,
    return_oldest_plant,
    return_plants_from_region,
)

from mppsteel.config.model_config import (
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    MEGATON_TO_KILOTON_FACTOR,
    TRADE_ROUNDING_NUMBER,
    UTILIZATION_ROUNDING_NUMBER,
)

from mppsteel.plant_classes.plant_choices_class import PlantChoices
from mppsteel.plant_classes.capacity_container_class import CapacityContainerClass
from mppsteel.model_solver.market_container_class import MarketContainerClass
from mppsteel.plant_classes.regional_utilization_class import UtilizationContainerClass
from mppsteel.model_solver.material_usage_class import (
    MaterialUsage,
    create_material_usage_dict,
)
from mppsteel.trade_module.trade_helpers import test_utilization_values
from mppsteel.trade_module.trade_flow import trade_flow

from mppsteel.utility.log_utility import get_logger


logger = get_logger(__name__)


def open_real_plants(
    production_demand_gap_analysis: pd.DataFrame,
    plant_id_container: PlantIdContainer,
    material_container: MaterialUsage,
    capacity_container: CapacityContainerClass,
    capacity_constraint_container: PlantCapacityConstraint,
    tech_choices_container: PlantChoices,
    lev_cost_df: pd.DataFrame,
    steel_plant_df: pd.DataFrame,
    tech_availability: pd.DataFrame,
    country_df: pd.DataFrame,
    business_case_ref: dict,
    year: int,
    regional_scrap: bool,
    tech_moratorium: bool,
    enforce_constraints: bool,
) -> pd.DataFrame:
    """Open plants based on cost competitiveness of technologies.

    Args:
        production_demand_gap_analysis (pd.DataFrame): A DataFrame containing the analysis of past state and current state of demand, capacity and trade data.
        plant_id_container (PlantIdContainer): plant_container (PlantIdContainer): Plant Container class containing a track of plants and their unique IDs.
        material_container (MaterialUsage): The MaterialUsage Instance containing the material usage state.
        capacity_container (CapacityContainerClass): The CapacityContainerClass Instance containing the capacity state.
        capacity_constraint_container (PlantCapacityConstraint): The PlantCapacityConstraint Instance containing the capacity constraint state.
        tech_choices_container (PlantChoices): The PlantChoices Instance containing the Technology Choices state.
        lev_cost_df (pd.DataFrame): A levelized cost reference DataFrame.
        steel_plant_df (pd.DataFrame): The steel plant DataFrame.
        tech_availability (pd.DataFrame): The technology availability reference.
        country_df (pd.DataFrame): The Country Metadata DataFrame.
        business_case_ref (dict): The business cases reference dictionary.
        year (int): The current model year.
        regional_scrap (bool): The scenario boolean value that determines whether there is a regional or global scrap constraints.
        tech_moratorium (bool): The scenario boolean value that determines whether there is a technology moratorium.
        enforce_constraints (bool): The scenario boolean value that determines if all constraints are enforced.

    Returns:
        pd.DataFrame: The modified Steel Plant DataFrame.
    """
    if lev_cost_df.empty:
        lev_cost_df = pd.DataFrame(columns=["year", "region", "technology"])
    levelized_cost_for_regions = (
        lev_cost_df.set_index(["year", "region"]).sort_index(ascending=True).copy()
    )
    levelized_cost_for_tech = (
        lev_cost_df.set_index(["year", "region", "technology"])
        .sort_index(ascending=True)
        .copy()
    )

    active_steel_plants_df = steel_plant_df[
        steel_plant_df["active_check"] == True
    ].copy()
    region_list = list(production_demand_gap_analysis.keys())
    steel_plant_cols = steel_plant_df.columns
    ng_mapper = ng_flag_mapper(steel_plant_df, country_df)
    updated_steel_plant_df = steel_plant_df.copy()

    for region in region_list:
        plants_required = production_demand_gap_analysis[region]["plants_required"]

        if plants_required > 0:
            metadata_container = []
            for _ in range(plants_required):
                new_plant_meta = new_plant_metadata(
                    plant_id_container,
                    production_demand_gap_analysis,
                    levelized_cost_for_regions,
                    active_steel_plants_df,
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

                capacity_constraint_container.update_potential_plant_switcher(
                    year, new_plant_name, new_plant_capacity, "New Plant"
                )
                capacity_constraint_container.subtract_capacity_from_balance(
                    year, new_plant_name, enforce_constraint=False
                )

                metadata_container.append(dict_entry)
                tech_choices_container.update_choice(year, new_plant_name, xcost_tech)
                # include usage for plant
                create_material_usage_dict(
                    material_usage_dict_container=material_container,
                    plant_capacities=capacity_container.return_plant_capacity(
                        year=year
                    ),
                    business_case_ref=business_case_ref,
                    plant_name=new_plant_name,
                    region=region,
                    year=year,
                    switch_technology=xcost_tech,
                    regional_scrap=regional_scrap,
                    capacity_value=new_plant_capacity,
                    override_constraint=True,
                    apply_transaction=True,
                )

            updated_steel_plant_df = pd.concat(
                [updated_steel_plant_df, pd.DataFrame(metadata_container)]
            ).reset_index(drop=True)

    return updated_steel_plant_df


def return_len_closed_plants_closed_in_year(
    steel_plant_df: pd.DataFrame, year: int
) -> int:
    return len(steel_plant_df[steel_plant_df["end_of_operation"] == year])


def return_len_inactive_plants(steel_plant_df: pd.DataFrame) -> int:
    return len(steel_plant_df[steel_plant_df["active_check"] == False])


def close_real_plants(
    production_demand_gap_analysis: pd.DataFrame,
    utilization_container: UtilizationContainerClass,
    investment_container: PlantInvestmentCycle,
    material_container: MaterialUsage,
    capacity_container: CapacityContainerClass,
    capacity_constraint_container: PlantCapacityConstraint,
    tech_choices_container: PlantChoices,
    steel_plant_df: pd.DataFrame,
    business_case_ref: dict,
    year: int,
    util_max: float,
    util_min: float,
    regional_scrap: bool,
) -> pd.DataFrame:
    """Closes plants from a Steel Plant DataFrame based on capacity constraint considerations, regional cost competitveness and plant age.

    Args:
        production_demand_gap_analysis (pd.DataFrame): A DataFrame containing the analysis of past state and current state of demand, capacity and trade data.
        utilization_container (UtilizationContainerClass): The UtilizationContainerClass Instance containing the utilization state.
        investment_container (PlantInvestmentCycle): The PlantInvestmentCycle Instance containing the investment cycle state.
        material_container (MaterialUsage): The MaterialUsage Instance containing the material usage state.
        capacity_container (CapacityContainerClass): The CapacityContainerClass Instance containing the capacity state.
        capacity_constraint_container (PlantCapacityConstraint): The PlantCapacityConstraint Instance containing the capacity constraint state.
        tech_choices_container (PlantChoices): The PlantChoices Instance containing the Technology Choices state.
        steel_plant_df (pd.DataFrame): The steel plant DataFrame.
        business_case_ref (dict): The business cases reference dictionary.
        year (int): The current model year.
        util_max (float): The maximum capacity utilization that plants are allowed to reach before having to open new plants.
        util_min (float): The minimum capacity utilization that plants are allowed to reach before having to close existing plants.
        regional_scrap (bool): The scenario boolean value that determines whether there is a regional or global scrap constraints.

    Returns:
        pd.DataFrame: The modified Steel Plant DataFrame.
    """
    investment_dict = investment_container.return_investment_dict()
    plant_start_years = investment_container.plant_start_years
    plant_cycle_lengths = investment_container.return_cycle_lengths()
    active_steel_plants_df = steel_plant_df[
        steel_plant_df["active_check"] == True
    ].copy()
    updated_steel_plant_df = steel_plant_df.copy()

    for region in list(production_demand_gap_analysis.keys()):
        plants_to_close = production_demand_gap_analysis[region]["plants_to_close"]
        initial_number_of_closed_plants = return_len_inactive_plants(
            updated_steel_plant_df
        )

        if plants_to_close > 0:
            initial_capacity = production_demand_gap_analysis[region]["capacity"]
            new_capacity = production_demand_gap_analysis[region]["new_total_capacity"]
            min_capacity_to_close = -production_demand_gap_analysis[region][
                "new_capacity_required"
            ]
            assert round(new_capacity, TRADE_ROUNDING_NUMBER) == round(
                initial_capacity - min_capacity_to_close, TRADE_ROUNDING_NUMBER
            ), f"Region: region | target_capacity: {new_capacity} | initial_capacity: {initial_capacity} | min_capacity_to_close: {min_capacity_to_close} -> {production_demand_gap_analysis[region]}"
            production_dict_value = production_demand_gap_analysis[region][
                "new_utilized_capacity"
            ]
            potential_plants_to_close = return_plants_from_region(
                active_steel_plants_df, region
            )
            initial_utilization = production_demand_gap_analysis[region][
                "new_utilization"
            ]
            closed_plants_prior = return_len_closed_plants_closed_in_year(
                updated_steel_plant_df, year
            )
            actual_plants_to_close = []
            actual_closed_plants = 0
            capacity_removed = 0

            while capacity_removed <= min_capacity_to_close:
                plant_to_close = return_oldest_plant(
                    investment_dict,
                    tech_choices_container.output_records_to_df("choice"),
                    plant_start_years,
                    plant_cycle_lengths,
                    year,
                    potential_plants_to_close,
                )
                potential_plants_to_close.remove(plant_to_close)
                plant_capacity = capacity_container.return_plant_capacity(
                    year, plant_to_close
                )
                actual_closed_plants += 1
                capacity_removed += plant_capacity
                actual_plants_to_close.append(plant_to_close)

            for plant_to_close in actual_plants_to_close:
                idx_close = updated_steel_plant_df.index[
                    updated_steel_plant_df["plant_name"] == plant_to_close
                ].tolist()[0]
                updated_steel_plant_df.loc[idx_close, "status"] = "decomissioned"
                updated_steel_plant_df.loc[idx_close, "end_of_operation"] = year
                updated_steel_plant_df.loc[idx_close, "active_check"] = False
                tech_choices_container.update_choice(
                    year, plant_to_close, "Close plant"
                )
                plant_tech = tech_choices_container.get_choice(year, plant_to_close)
                plant_capacity = capacity_container.return_plant_capacity(
                    year, plant_to_close
                )
                capacity_constraint_container.remove_plant_from_waiting_list(
                    year, plant_to_close
                )
                # remove usage for plant
                create_material_usage_dict(
                    material_usage_dict_container=material_container,
                    plant_capacities=capacity_container.return_plant_capacity(
                        year=year
                    ),
                    business_case_ref=business_case_ref,
                    plant_name=plant_to_close,
                    region=region,
                    year=year,
                    switch_technology=plant_tech,
                    regional_scrap=regional_scrap,
                    capacity_value=plant_capacity,
                    override_constraint=True,
                    apply_transaction=True,
                    negative_amount=True,
                )

            capacity_removal_actual_minus_indicative = (
                capacity_removed - min_capacity_to_close
            )
            closed_plants_post = return_len_closed_plants_closed_in_year(
                updated_steel_plant_df, year
            )
            new_number_of_closed_plants = return_len_inactive_plants(
                updated_steel_plant_df
            )
            new_total_capacity = initial_capacity - capacity_removed
            new_utilization = production_dict_value / new_total_capacity
            utilization_container.update_region(year, region, new_utilization)
            production_demand_gap_analysis[region][
                "new_total_capacity"
            ] = new_total_capacity
            production_demand_gap_analysis[region]["new_utilization"] = new_utilization

            assert round(capacity_removed, TRADE_ROUNDING_NUMBER) >= round(
                min_capacity_to_close, TRADE_ROUNDING_NUMBER
            ), f"{region}: Capacity Removed {capacity_removed} is less than Indicative Capacity Removed {min_capacity_to_close}"
            assert round(initial_capacity, TRADE_ROUNDING_NUMBER) >= round(
                new_total_capacity, TRADE_ROUNDING_NUMBER
            ), f"{region}: New Capacity {new_total_capacity} is greater than Old Capacity {initial_capacity}"
            assert round(initial_utilization, UTILIZATION_ROUNDING_NUMBER) <= round(
                new_utilization, UTILIZATION_ROUNDING_NUMBER
            ), f"{region}: Initial Utilization {initial_utilization} is smaller than the New Utilization {new_utilization}"
            assert (
                round(util_min, UTILIZATION_ROUNDING_NUMBER)
                <= round(new_utilization, UTILIZATION_ROUNDING_NUMBER)
                <= round(util_max, UTILIZATION_ROUNDING_NUMBER)
            ), f"{region}: utilization {new_utilization} is out of bounds -> capacity_removed: {capacity_removed} | capacity removal gap: {capacity_removal_actual_minus_indicative} | {production_demand_gap_analysis[region]}"
            assert (
                new_number_of_closed_plants > initial_number_of_closed_plants
            ), f"Closed Plants not being updated -> plants_to_close: {plants_to_close} | initial_active_checks: {initial_number_of_closed_plants} | new_active_checks: {new_number_of_closed_plants} | plants: {join_list_as_string(actual_plants_to_close)}"
            assert (
                closed_plants_prior + len(actual_plants_to_close) == closed_plants_post
            ), f"Closed plants prior: {closed_plants_prior} | Actual closed plants to add: {len(actual_plants_to_close)} | Closed plants post: {closed_plants_post}"

    return updated_steel_plant_df


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
    capacity_constraint_container: PlantCapacityConstraint,
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
    investment_cycle_randomness: bool = False,
    util_max: float = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    util_min: float = CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
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
        capacity_constraint_container (PlantCapacityConstraint): The PlantCapacityConstraint Instance containing the capacity constraint state.
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
        util_max (float, optional): The maximum capacity utilization that plants are allowed to reach before having to open new plants. Defaults to CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION.
        util_min (float, optional): The minimum capacity utilization that plants are allowed to reach before having to close existing plants. Defaults to CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION.

    Returns:
        pd.DataFrame: The initial DataFrame passed to the function with the plants that have been opened and close updated.
    """

    logger.info(f"Running open and close decisions for {year}")
    updated_steel_plant_df = steel_plant_df.copy()
    active_steel_plants_df = steel_plant_df[
        steel_plant_df["active_check"] == True
    ].copy()
    capacity_container.map_capacities(active_steel_plants_df, year)

    if trade_scenario:
        logger.info(f"Starting the trade flow for {year}")
        production_demand_gap_analysis = trade_flow(
            market_container=market_container,
            utilization_container=utilization_container,
            capacity_container=capacity_container,
            steel_demand_df=steel_demand_df,
            variable_cost_df=variable_costs_df,
            plant_df=active_steel_plants_df,
            capex_dict=capex_dict,
            tech_choices_ref=tech_choices_container.return_choices(),
            year=year,
            util_min=util_min,
            util_max=util_max,
        )
    else:
        logger.info(f"Starting the non-trade flow for {year}")
        production_demand_gap_analysis = production_demand_gap(
            steel_demand_df=steel_demand_df,
            capacity_container=capacity_container,
            utilization_container=utilization_container,
            year=year,
            util_max=util_max,
            util_min=util_min,
        )

    updated_steel_plant_df = open_real_plants(
        production_demand_gap_analysis,
        plant_id_container,
        material_container,
        capacity_container,
        capacity_constraint_container,
        tech_choices_container,
        lev_cost_df,
        updated_steel_plant_df,
        tech_availability,
        country_df,
        business_case_ref,
        year,
        regional_scrap,
        tech_moratorium,
        enforce_constraints,
    )

    updated_steel_plant_df = close_real_plants(
        production_demand_gap_analysis,
        utilization_container,
        investment_container,
        material_container,
        capacity_container,
        capacity_constraint_container,
        tech_choices_container,
        updated_steel_plant_df,
        business_case_ref,
        year,
        util_max,
        util_min,
        regional_scrap,
    )

    test_utilization_values(
        utilization_container, production_demand_gap_analysis, year, util_min, util_max
    )
    production_demand_gap_analysis_df = create_and_test_market_df(
        production_demand_gap_analysis, year, test_df=True
    )
    market_container.store_results(
        year, production_demand_gap_analysis_df, "market_results"
    )

    new_active_plants = updated_steel_plant_df[
        updated_steel_plant_df["active_check"] == True
    ].copy()
    capacity_container.map_capacities(new_active_plants, year)
    regional_capacities = capacity_container.return_regional_capacity(year)
    global_demand = steel_demand_getter(
        steel_demand_df, year=year, metric="crude", region="World"
    )
    utilization_container.calculate_world_utilization(
        year, regional_capacities, global_demand
    )
    global_production = sum(
        regional_capacities.values()
    ) * utilization_container.get_utilization_values(year, region="World")
    logger.info(
        f"Balanced Supply Demand results for {year}: Demand: {global_demand :0.2f}  | Production: {global_production :0.2f}"
    )
    new_open_plants = return_modified_plants(new_active_plants, year, "open")
    investment_container.add_new_plants(
        new_open_plants["plant_name"],
        new_open_plants["start_of_operation"],
        investment_cycle_randomness,
    )
    return updated_steel_plant_df
