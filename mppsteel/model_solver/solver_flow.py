"""Main solving script for deciding investment decisions."""

import pandas as pd
from tqdm import tqdm

from typing import Iterable, Union
from mppsteel.utility.dataframe_utility import extend_df_years

from mppsteel.model_solver.solver_summary import tech_capacity_splits, utilization_mapper
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.plant_classes.plant_container_class import PlantIdContainer
from mppsteel.plant_classes.plant_investment_cycle_class import PlantInvestmentCycle
from mppsteel.plant_classes.capacity_constraint_class import PlantCapacityConstraint
from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    return_pkl_paths,
    serialize_file
)
from mppsteel.utility.location_utility import create_country_mapper
from mppsteel.config.model_config import (
    MEGATON_TO_KILOTON_FACTOR,
    MODEL_YEAR_END,
    MODEL_YEAR_RANGE,
    MODEL_YEAR_START,
    YEARS_TO_SKIP_FOR_SOLVER,
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS,
    MAIN_REGIONAL_SCHEMA,
    PROJECT_PATH
)

from mppsteel.config.reference_lists import (
    RESOURCE_CONTAINER_REF,
)
from mppsteel.model_solver.solver_flow_helpers import read_and_format_tech_availability
from mppsteel.model_solver.material_usage_class import return_current_usage
from mppsteel.data_load_and_format.steel_plant_formatter import create_active_check_col
from mppsteel.model_solver.tco_and_abatement_optimizer import subset_presolver_df
from mppsteel.plant_classes.plant_choices_class import PlantChoices
from mppsteel.plant_classes.capacity_container_class import CapacityContainerClass
from mppsteel.model_solver.market_container_class import MarketContainerClass

from mppsteel.plant_classes.regional_utilization_class import (
    UtilizationContainerClass, create_wsa_2020_utilization_dict
)
from mppsteel.model_solver.material_usage_class import (
    MaterialUsage,
    create_material_usage_dict
)
from mppsteel.model_solver.plant_open_close_flow import open_close_plants
from mppsteel.model_solver.solver_flow_helpers import (
    active_check_results,
    create_solver_entry_dict,
    get_current_technology,
    resort_primary_switchers,
    return_best_tech,
    return_initial_tech,
    split_primary_plant_switchers,
)
from mppsteel.data_load_and_format.country_reference import country_df_formatter
from mppsteel.data_preprocessing.levelized_cost import generate_levelized_cost_results
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


class ChooseTechnologyInput:
    @classmethod
    def get_steel_demand_default(cls) -> pd.DataFrame:
        """Returns the default steel demand dataframe."""
        return pd.DataFrame(
            [["Africa", 0.0, "2020", "Average", "Scrap availability", []]],
            columns=["region", "value", "year", "scenario", "metric", "country_code"],
        ).set_index(["year", "scenario", "metric"])


    def __init__(
        self,
        *,
        original_plant_df: pd.DataFrame = pd.DataFrame(
            [["STE02421", "Africa", True, 0.0, "status", "name", 2019, "F", 0, "DEU", 2020]],
            columns=["plant_id", "rmi_region", "active_check", "plant_capacity",
                    "status", "plant_name", "start_of_operation", "primary_capacity",
                    "cheap_natural_gas", "country_code", "end_of_operation"]
        ),
        year_range: Iterable[int] = [],
        tech_moratorium: bool = False,
        trade_active: bool = False,
        enforce_constraints: bool = False,
        regional_scrap_constraint: bool = False,
        investment_cycle_randomness: bool = False,
        plant_investment_cycle_container: PlantInvestmentCycle = PlantInvestmentCycle(),
        variable_costs_regional: pd.DataFrame = pd.DataFrame(),
        country_ref: pd.DataFrame = pd.DataFrame(),
        rmi_mapper: dict[str, str] = {},
        country_ref_f: pd.DataFrame = pd.DataFrame([], columns=["country_code"]),
        bio_constraint_model: pd.DataFrame = pd.DataFrame(
            [[2020, 0.0]], columns=["year", "value"]
        ).set_index("year"),
        co2_constraint: pd.DataFrame = pd.DataFrame(
            [[0.0, 2020, "Steel CO2 use market"]], columns=["Value", "Year", "Metric"]
        ),
        ccs_constraint: pd.DataFrame = pd.DataFrame(
            [[2020, "Global", 0.0]], columns=["year", "region", "value"]
        ).set_index(["year", "region"]),
        steel_demand_df: pd.DataFrame = pd.DataFrame(),
        tech_availability: pd.DataFrame = pd.DataFrame(),
        ta_dict: dict[str, int] = {},
        capex_dict: dict[str, pd.DataFrame] = {},
        business_case_ref: dict[tuple[str, str], float] = {},
        green_premium_timeseries: pd.DataFrame = pd.DataFrame(),
        tco_summary_data: pd.DataFrame = pd.DataFrame(),
        tco_slim: pd.DataFrame = pd.DataFrame(),
        levelized_cost: pd.DataFrame = pd.DataFrame(),
        steel_plant_abatement_switches: pd.DataFrame = pd.DataFrame(),
        abatement_slim: pd.DataFrame = pd.DataFrame(),
        scenario_dict: dict[str, any] = {},
        wsa_dict: dict[str, float] = {},
        model_year_range: Iterable[int] = range(2020, 2021),
    ):
        self.original_plant_df = original_plant_df
        self.year_range = year_range
        self.tech_moratorium = tech_moratorium
        self.trade_active = trade_active
        self.enforce_constraints = enforce_constraints
        self.regional_scrap_constraint = regional_scrap_constraint
        self.investment_cycle_randomness = investment_cycle_randomness
        self.plant_investment_cycle_container = plant_investment_cycle_container
        self.variable_costs_regional = variable_costs_regional
        self.country_ref = country_ref
        self.rmi_mapper = rmi_mapper
        self.country_ref_f = country_ref_f
        self.bio_constraint_model = bio_constraint_model
        self.co2_constraint = co2_constraint
        self.ccs_constraint = ccs_constraint
        if steel_demand_df.empty:
            self.steel_demand_df = ChooseTechnologyInput.get_steel_demand_default()
        else:
            self.steel_demand_df = steel_demand_df
        self.tech_availability = tech_availability
        self.ta_dict = ta_dict
        self.capex_dict = capex_dict
        self.business_case_ref = business_case_ref
        self.green_premium_timeseries = green_premium_timeseries
        self.tco_summary_data = tco_summary_data
        self.tco_slim = tco_slim
        self.levelized_cost = levelized_cost
        self.steel_plant_abatement_switches = steel_plant_abatement_switches
        self.abatement_slim = abatement_slim
        self.scenario_dict = scenario_dict
        self.wsa_dict = wsa_dict
        self.model_year_range = model_year_range

    @classmethod
    def from_filesystem(
        cls, scenario_dict, pkl_paths, year_range=MODEL_YEAR_RANGE
    ):
        tech_moratorium = scenario_dict["tech_moratorium"]
        trade_active = scenario_dict["trade_active"]
        enforce_constraints = scenario_dict["enforce_constraints"]
        regional_scrap_constraint = scenario_dict["regional_scrap_constraint"]
        investment_cycle_randomness = scenario_dict["investment_cycle_randomness"]

        _, intermediate_path, _ = return_pkl_paths(scenario_name=scenario_dict["scenario_name"], paths=pkl_paths)

        original_plant_df = read_pickle_folder(
            PROJECT_PATH / PKL_DATA_FORMATTED, "steel_plants_processed", "df"
        )
        plant_investment_cycle_container = read_pickle_folder(
            PROJECT_PATH / PKL_DATA_FORMATTED, "plant_investment_cycle_container", "df"
        )
        variable_costs_regional = read_pickle_folder(
            PROJECT_PATH / intermediate_path, "variable_costs_regional", "df"
        )
        country_ref = read_pickle_folder(
            PROJECT_PATH / PKL_DATA_IMPORTS, "country_ref", "df"
        )
        rmi_mapper = create_country_mapper(path=str(PROJECT_PATH / PKL_DATA_IMPORTS))
        country_ref_f = country_df_formatter(country_ref)
        bio_constraint_model = read_pickle_folder(
            PROJECT_PATH / intermediate_path, "bio_constraint_model_formatted", "df"
        )
        co2_constraint = read_pickle_folder(
            PROJECT_PATH / PKL_DATA_IMPORTS, "ccs_co2", "df"
        )
        co2_constraint = extend_df_years(co2_constraint, "Year", MODEL_YEAR_END)
        ccs_constraint = read_pickle_folder(
            PROJECT_PATH / intermediate_path, "ccs_constraints_model_formatted", "df"
        )
        steel_demand_df = read_pickle_folder(
            PROJECT_PATH / intermediate_path, "regional_steel_demand_formatted", "df"
        )
        tech_availability_raw = read_pickle_folder(
            PROJECT_PATH / PKL_DATA_IMPORTS, "tech_availability", "df"
        )
        ta_dict = dict(
            zip(
                tech_availability_raw["Technology"],
                tech_availability_raw["Year available from"],
            )
        )
        tech_availability = read_and_format_tech_availability(tech_availability_raw)
        capex_dict = read_pickle_folder(
            PROJECT_PATH / PKL_DATA_FORMATTED, "capex_dict", "dict"
        )
        business_case_ref = read_pickle_folder(
            PROJECT_PATH / PKL_DATA_FORMATTED, "business_case_reference", "df"
        )
        green_premium_timeseries = read_pickle_folder(
            PROJECT_PATH / intermediate_path, "green_premium_timeseries", "df"
        ).set_index("year")
        tco_summary_data = read_pickle_folder(
            PROJECT_PATH / intermediate_path, "tco_summary_data", "df"
        )
        tco_slim = subset_presolver_df(tco_summary_data, subset_type="tco_summary")
        levelized_cost = read_pickle_folder(
            PROJECT_PATH / intermediate_path, "levelized_cost_standardized", "df"
        )
        levelized_cost["region"] = levelized_cost["country_code"].apply(
            lambda x: rmi_mapper[x]
        )
        steel_plant_abatement_switches = read_pickle_folder(
            PROJECT_PATH / intermediate_path, "emissivity_abatement_switches", "df"
        )
        abatement_slim = subset_presolver_df(
            steel_plant_abatement_switches, subset_type="abatement"
        )
        wsa_dict = create_wsa_2020_utilization_dict(utilization_cap=1)
        model_year_range = MODEL_YEAR_RANGE
        return cls(
            original_plant_df=original_plant_df,
            year_range=year_range,
            tech_moratorium=tech_moratorium,
            trade_active=trade_active,
            enforce_constraints=enforce_constraints,
            regional_scrap_constraint=regional_scrap_constraint,
            investment_cycle_randomness=investment_cycle_randomness,
            plant_investment_cycle_container=plant_investment_cycle_container,
            variable_costs_regional=variable_costs_regional,
            country_ref=country_ref,
            rmi_mapper=rmi_mapper,
            country_ref_f=country_ref_f,
            bio_constraint_model=bio_constraint_model,
            co2_constraint=co2_constraint,
            ccs_constraint=ccs_constraint,
            steel_demand_df=steel_demand_df,
            tech_availability=tech_availability,
            ta_dict=ta_dict,
            capex_dict=capex_dict,
            business_case_ref=business_case_ref,
            green_premium_timeseries=green_premium_timeseries,
            tco_summary_data=tco_summary_data,
            tco_slim=tco_slim,
            levelized_cost=levelized_cost,
            steel_plant_abatement_switches=steel_plant_abatement_switches,
            abatement_slim=abatement_slim,
            scenario_dict=scenario_dict,
            wsa_dict=wsa_dict,
            model_year_range=model_year_range,
        )


def choose_technology_core(cti: ChooseTechnologyInput) -> dict:
    """Function containing the entire solver decision logic flow.
    1) In each year, the solver splits the plants non-switchers and switchers (secondary EAF plants and primary plants).
    2) The solver extracts the prior year technology of the non-switchers and assumes this is the current technology of the switchers.
    3) The material usage of the non-switching and secondary EAF plants is subtracted from the constraints and the remainder is then left over for the remaining switching plants.
    4) Plants are opened or closed according to the Demand for that year, the open and closing logic (potentially including trade). Which changes the capacity constraints.
    5) All switching plants are then sent through the `return_best_tech` function that decides the best technology depending on the switch type (main cycle or transitional switch).
    6) All results are saved to a dictionary which is outputted at the end of the year loop.

    Args:
        scenario_dict (int): Model Scenario settings.
    Returns:
        dict: A dictionary containing the best technology resuls. Organised as [year][plant][best tech].
    """

    logger.info("Creating Steel plant df")
    scenario_dict = cti.scenario_dict
    tech_moratorium = cti.tech_moratorium
    trade_scenario = cti.trade_active
    enforce_constraints = cti.enforce_constraints
    regional_scrap = cti.regional_scrap_constraint
    investment_cycle_randomness = cti.investment_cycle_randomness
    original_plant_df = cti.original_plant_df
    PlantInvestmentCycleContainer = cti.plant_investment_cycle_container
    variable_costs_regional = cti.variable_costs_regional
    country_ref = cti.country_ref
    rmi_mapper = cti.rmi_mapper
    country_ref_f = cti.country_ref_f
    bio_constraint_model = cti.bio_constraint_model
    co2_constraint = cti.co2_constraint
    ccs_constraint = cti.ccs_constraint
    steel_demand_df = cti.steel_demand_df
    tech_availability = cti.tech_availability
    ta_dict = cti.ta_dict
    capex_dict = cti.capex_dict
    business_case_ref = cti.business_case_ref
    green_premium_timeseries = cti.green_premium_timeseries
    tco_summary_data = cti.tco_summary_data
    tco_slim = cti.tco_slim
    levelized_cost = cti.levelized_cost
    steel_plant_abatement_switches = cti.steel_plant_abatement_switches
    abatement_slim = cti.abatement_slim
    wsa_dict = cti.wsa_dict
    model_year_range = cti.model_year_range

    # Initialize plant container
    PlantIDC = PlantIdContainer()
    PlantIDC.add_steel_plant_ids(original_plant_df)
    year_start_df = original_plant_df.copy()

    # Instantiate Trade Container
    market_container = MarketContainerClass()
    region_list = year_start_df[MAIN_REGIONAL_SCHEMA].unique()
    market_container.full_instantiation(model_year_range, region_list)

    # Utilization & Capacity Containers
    UtilizationContainer = UtilizationContainerClass()
    region_list = list(wsa_dict.keys())
    UtilizationContainer.initiate_container(
        year_range=model_year_range, region_list=region_list
    )
    CapacityContainer = CapacityContainerClass()
    CapacityContainer.instantiate_container(model_year_range)
    CapacityContainer.set_average_plant_capacity(original_plant_df)

    # Initialize the Material Usage container
    resource_models = {
        "biomass": bio_constraint_model,
        "scrap": steel_demand_df,
        "co2": co2_constraint,
        "ccs": ccs_constraint,
    }
    MaterialUsageContainer = MaterialUsage()
    MaterialUsageContainer.initiate_years_and_regions(
        model_year_range, resource_list=resource_models.keys(), region_list=region_list
    )

    for resource in resource_models:
        MaterialUsageContainer.load_constraint(resource_models[resource], resource)

    # Plant Choices
    PlantChoiceContainer = PlantChoices()
    PlantChoiceContainer.initiate_container(model_year_range)
    # Plant Constraint
    PlantCapacityConstraintContainer = PlantCapacityConstraint()
    PlantCapacityConstraintContainer.instantiate_container(model_year_range)
    # Investment Cycles
    for year in tqdm(model_year_range, total=len(model_year_range), desc="Years"):
        year_start_df["active_check"] = year_start_df.apply(
            create_active_check_col, year=year, axis=1
        )
        active_plant_df = year_start_df[year_start_df["active_check"] == True].copy()
        inactive_year_start_df = year_start_df[
            year_start_df["active_check"] == False
        ].copy()
        CapacityContainer.map_capacities(active_plant_df, year)
        world_capacity = CapacityContainer.get_world_capacity_sum(year)
        PlantCapacityConstraintContainer.update_capacity_turnover_limit(year, world_capacity)
        PlantCapacityConstraintContainer.update_capacity_balance(year)
        logger.info(
            f"Number of active (inactive) plants in {year}: {len(active_plant_df)} ({len(inactive_year_start_df)})"
        )

        for resource in resource_models:
            MaterialUsageContainer.set_year_balance(year, resource, region_list)

        # Assign initial technologies for plants in the first year
        logger.info(f"Running investment decisions for {year}")
        if year == MODEL_YEAR_START:
            logger.info(f"Loading initial technology choices for {year}")
            for row in active_plant_df.itertuples():
                PlantChoiceContainer.update_choice(
                    year, row.plant_name, row.initial_technology
                )
            UtilizationContainer.assign_year_utilization(MODEL_YEAR_START, wsa_dict)

        # Exceptions for plants in plants database that are scheduled to open later, to have their prior technology as their previous choice
        for row in inactive_year_start_df.itertuples():
            if row.start_of_operation == year + 1:
                PlantChoiceContainer.update_choice(
                    year, row.plant_name, row.initial_technology
                )

        all_active_plant_names = active_plant_df["plant_name"].copy()
        plant_capacities_dict = CapacityContainer.return_plant_capacity(year=year)
        switchers = PlantInvestmentCycleContainer.return_plant_switchers(
            all_active_plant_names, year, "combined"
        )
        non_switchers = list(set(all_active_plant_names).difference(switchers))
        switchers_df = (
            active_plant_df.set_index(["plant_name"]).drop(non_switchers).reset_index()
        ).copy()
        switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        non_switchers_df = (
            active_plant_df.set_index(["plant_name"]).drop(switchers).reset_index()
        ).copy()
        non_switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
        logger.info(f"-- Assigning usage for exisiting plants")

        # skip first year
        if year in YEARS_TO_SKIP_FOR_SOLVER:
            pass
        else:
            # check resource allocation for non-switchers
            for row in non_switchers_df.itertuples():
                plant_name = row.plant_name
                plant_region = row.rmi_region
                plant_capacity = row.plant_capacity / MEGATON_TO_KILOTON_FACTOR
                current_tech = ""
                year_founded = PlantInvestmentCycleContainer.plant_start_years[plant_name]

                if (year == MODEL_YEAR_START) or (year == year_founded):
                    current_tech = row.initial_technology
                else:
                    current_tech = PlantChoiceContainer.get_choice(year - 1, plant_name)
                create_solver_entry_dict(
                    PlantChoiceContainer, year, plant_name, 
                    current_tech, current_tech, "not a switch year", 
                    update_record=True, update_choice=True
                )

                create_material_usage_dict(
                    material_usage_dict_container=MaterialUsageContainer,
                    plant_capacities=plant_capacities_dict,
                    business_case_ref=business_case_ref,
                    plant_name=plant_name,
                    region=plant_region,
                    year=year,
                    switch_technology=current_tech,
                    regional_scrap=regional_scrap,
                    capacity_value=plant_capacity,
                    override_constraint=True,
                    apply_transaction=True
                )

        scrap_usage = return_current_usage(
            non_switchers,
            PlantChoiceContainer.return_choices(year),
            plant_capacities_dict,
            business_case_ref,
            RESOURCE_CONTAINER_REF["scrap"],
        )
        logger.info(
            f"Scrap usage | Non-Switchers: {scrap_usage: 0.2f} | Count: {len(non_switchers)}"
        )

        # check resource allocation for EAF secondary capacity
        secondary_eaf_switchers = switchers_df[
            switchers_df["primary_capacity"] == "N"
        ].copy()
        secondary_eaf_switchers_plants = secondary_eaf_switchers["plant_name"].unique()

        for row in secondary_eaf_switchers.itertuples():
            plant_name = row.plant_name
            plant_capacity = row.plant_capacity / MEGATON_TO_KILOTON_FACTOR
            plant_region = row.rmi_region
            create_solver_entry_dict(
                PlantChoiceContainer, year, plant_name, 
                "EAF", "EAF", "Secondary capacity is always EAF", 
                update_record=True, update_choice=True
            )

            create_material_usage_dict(
                material_usage_dict_container=MaterialUsageContainer,
                plant_capacities=plant_capacities_dict,
                business_case_ref=business_case_ref,
                plant_name=plant_name,
                region=plant_region,
                year=year,
                switch_technology="EAF",
                regional_scrap=regional_scrap,
                capacity_value=plant_capacity,
                override_constraint=True,
                apply_transaction=True
            )

        scrap_usage = return_current_usage(
            secondary_eaf_switchers_plants,
            PlantChoiceContainer.return_choices(year),
            plant_capacities_dict,
            business_case_ref,
            RESOURCE_CONTAINER_REF["scrap"],
        )
        logger.info(
            f"Scrap usage | Switchers - Secondary EAF: {scrap_usage: 0.2f} | Count: {len(secondary_eaf_switchers_plants)}"
        )
        logger.info(
            f"Scrap usage | Amount remaining for switchers/new plants: {MaterialUsageContainer.get_current_balance(year, 'scrap'): 0.2f}"
        )
        # skip first year
        if year in YEARS_TO_SKIP_FOR_SOLVER:
            capacity_adjusted_df = active_plant_df.copy()
            regional_capacities = CapacityContainer.return_regional_capacity(year)
            global_demand = steel_demand_getter(
                steel_demand_df, year=year, metric="crude", region="World"
            )
            UtilizationContainer.calculate_world_utilization(
                year, regional_capacities, global_demand
            )
        else:
            # Run open/close capacity
            capacity_adjusted_df = open_close_plants(
                steel_demand_df=steel_demand_df,
                steel_plant_df=active_plant_df,
                country_df=country_ref_f,
                lev_cost_df=levelized_cost,
                business_case_ref=business_case_ref,
                tech_availability=tech_availability,
                variable_costs_df=variable_costs_regional,
                capex_dict=capex_dict,
                capacity_container=CapacityContainer,
                capacity_constraint_container=PlantCapacityConstraintContainer,
                utilization_container=UtilizationContainer,
                material_container=MaterialUsageContainer,
                tech_choices_container=PlantChoiceContainer,
                plant_id_container=PlantIDC,
                market_container=market_container,
                investment_container=PlantInvestmentCycleContainer,
                year=year,
                trade_scenario=trade_scenario,
                tech_moratorium=tech_moratorium,
                regional_scrap=regional_scrap,
                enforce_constraints=enforce_constraints,
                investment_cycle_randomness=investment_cycle_randomness
            )
            capacity_adjusted_active_plants = capacity_adjusted_df[
                capacity_adjusted_df["active_check"] == True
            ].copy()
            all_active_plant_names = capacity_adjusted_active_plants["plant_name"].copy()
            plant_capacities_dict = CapacityContainer.return_plant_capacity(year=year)
            switchers = PlantInvestmentCycleContainer.return_plant_switchers(
                all_active_plant_names, year, "combined"
            )
            non_switchers = list(set(all_active_plant_names).difference(switchers))
            switchers_df = (
                capacity_adjusted_active_plants.set_index(["plant_name"])
                .drop(non_switchers)
                .reset_index()
            ).copy()
            switchers_df.rename({"index": "plant_name"}, axis=1, inplace=True)
            switchers_df = switchers_df.sample(frac=1)
            logger.info(f"-- Running investment decisions for Non Switching Plants")

            primary_switchers_df = switchers_df[
                switchers_df["primary_capacity"] == "Y"
            ].copy()

            # Shuffle rows
            primary_switchers_df = primary_switchers_df.sample(frac=1)
            waiting_list_dict = PlantCapacityConstraintContainer.potential_plant_switchers[year]
            if len(waiting_list_dict) > 0:
                primary_switchers_df = resort_primary_switchers(primary_switchers_df, waiting_list_dict)

            # Split up primary switchers into groups
            closed_plants_current_techs, new_open_plants, main_cycle_plants, trans_switch_plants = split_primary_plant_switchers(
                primary_switchers_df, PlantInvestmentCycleContainer, PlantChoiceContainer, year
            )

            # CASE 1: CLOSED PLANTS
            for plant_name in tqdm(closed_plants_current_techs, total=len(closed_plants_current_techs), desc="Closed Plants"):
                current_tech = closed_plants_current_techs[plant_name]
                create_solver_entry_dict(
                    PlantChoiceContainer, year, plant_name, 
                    closed_plants_current_techs[plant_name], "Close plant", 
                    "Plant was already closed", update_record=True, update_choice=True
                )

            # CASE 2: NEW PLANTS
            for plant_name in tqdm(new_open_plants, total=len(new_open_plants), desc="New Open Plants"):
                current_tech = new_open_plants[plant_name]
                create_solver_entry_dict(
                    PlantChoiceContainer, year, plant_name, 
                    current_tech, current_tech, "New plant founding year", 
                    update_record=True, update_choice=True
                )

            # CASE 3: MAIN CYCLE PLANTS
            for plant_name in tqdm(main_cycle_plants, total=len(main_cycle_plants), desc="Main Cycle Plants"):
                current_tech = main_cycle_plants[plant_name]["current_tech"]
                best_choice_tech = return_best_tech(
                    tco_reference_data=tco_slim,
                    abatement_reference_data=abatement_slim,
                    business_case_ref=business_case_ref,
                    variable_costs_df=variable_costs_regional,
                    green_premium_timeseries=green_premium_timeseries,
                    tech_availability=tech_availability,
                    tech_avail_from_dict=ta_dict,
                    plant_capacities=plant_capacities_dict,
                    scenario_dict=scenario_dict,
                    investment_container=PlantInvestmentCycleContainer,
                    plant_choice_container=PlantChoiceContainer,
                    capacity_constraint_container=PlantCapacityConstraintContainer,
                    year=year,
                    plant_name=plant_name,
                    region=main_cycle_plants[plant_name]["region"],
                    country_code=main_cycle_plants[plant_name]["country_code"],
                    base_tech=current_tech,
                    transitional_switch_mode=False,
                    material_usage_dict_container=MaterialUsageContainer,
                )
                switch_type_entry = "No change in main investment cycle year" if best_choice_tech == current_tech else "Regular change in investment cycle year"

                create_solver_entry_dict(
                    PlantChoiceContainer, year, plant_name,
                    current_tech, best_choice_tech, switch_type_entry,
                    update_record=True, update_choice=True
                )

            # CASE 4: TRANSITIONARY SWITCH PLANTS
            if scenario_dict["transitional_switch"]:
                for plant_name in tqdm(trans_switch_plants, total=len(trans_switch_plants), desc="Trans Switch Plants"):
                    current_tech = trans_switch_plants[plant_name]["current_tech"]
                    best_choice_tech = return_best_tech(
                        tco_reference_data=tco_slim,
                        abatement_reference_data=abatement_slim,
                        business_case_ref=business_case_ref,
                        variable_costs_df=variable_costs_regional,
                        green_premium_timeseries=green_premium_timeseries,
                        tech_availability=tech_availability,
                        tech_avail_from_dict=ta_dict,
                        plant_capacities=plant_capacities_dict,
                        scenario_dict=scenario_dict,
                        investment_container=PlantInvestmentCycleContainer,
                        plant_choice_container=PlantChoiceContainer,
                        capacity_constraint_container=PlantCapacityConstraintContainer,
                        year=year,
                        plant_name=plant_name,
                        region=trans_switch_plants[plant_name]["region"],
                        country_code=trans_switch_plants[plant_name]["country_code"],
                        base_tech=current_tech,
                        transitional_switch_mode=True,
                        material_usage_dict_container=MaterialUsageContainer,
                    )
                    if best_choice_tech != current_tech:
                        PlantInvestmentCycleContainer.adjust_cycle_for_transitional_switch(
                            plant_name, year
                        )
                    switch_type_entry = "Transitional switch in off-cycle investment year" if best_choice_tech != current_tech else "No change during off-cycle investment year"
                    create_solver_entry_dict(
                        PlantChoiceContainer, year, plant_name, 
                        current_tech, best_choice_tech, switch_type_entry, 
                        update_record=True, update_choice=True
                    )

            # CASE 5: CAPACITY CONSTRAINED PLANTS
            capacity_constrained_plants = PlantCapacityConstraintContainer.return_waiting_list(year)
            PlantCapacityConstraintContainer.move_waiting_list_plants_to_next_year(PlantInvestmentCycleContainer, year)
            PlantCapacityConstraintContainer.test_capacity_constraint(year)
            assert set(capacity_constrained_plants).issubset(primary_switchers_df["plant_name"].to_list()), "Not all plants in the waiting list are in the current year's active plants"
            initial_tech_ref = primary_switchers_df[["plant_name", "initial_technology"]].set_index("plant_name").to_dict()
            for plant_name in capacity_constrained_plants:
                year_founded = PlantInvestmentCycleContainer.plant_start_years[plant_name]
                initial_tech = return_initial_tech(initial_tech_ref["initial_technology"], plant_name)
                current_tech = get_current_technology(
                    PlantChoiceContainer, year, plant_name, year_founded, initial_tech
                )
                create_solver_entry_dict(
                    PlantChoiceContainer, year, plant_name,
                    current_tech, current_tech, "Investment year deferred due to Capacity Constraint",
                    update_record=True, update_choice=False
                )
            PlantCapacityConstraintContainer.print_capacity_summary(year)

        year_start_df = pd.concat(
            [capacity_adjusted_df, inactive_year_start_df]
        ).reset_index(drop=True)
        MaterialUsageContainer.print_year_summary(year, regional_scrap=regional_scrap)

    final_steel_plant_df = year_start_df.copy()
    active_check_results_dict = active_check_results(
        final_steel_plant_df, model_year_range
    )

    PlantCapacityConstraintContainer.waiting_list_limit_checker()
    production_demand_analysis = market_container.output_trade_calculations_to_df("market_results")
    full_trade_summary = market_container.output_trade_calculations_to_df("merge_trade_summary", steel_demand_df)
    material_usage_results = MaterialUsageContainer.output_results_to_df()
    investment_dict = PlantInvestmentCycleContainer.return_investment_dict()
    plant_cycle_length_mapper = PlantInvestmentCycleContainer.return_cycle_lengths()
    investment_df = PlantInvestmentCycleContainer.create_investment_df()
    tech_choice_dict = PlantChoiceContainer.return_choices()
    tech_choice_records = PlantChoiceContainer.output_records_to_df("choice")
    tech_rank_records = PlantChoiceContainer.output_records_to_df("rank")
    regional_capacity_results = CapacityContainer.return_regional_capacity()
    plant_capacity_results = CapacityContainer.return_plant_capacity()
    utilization_results = UtilizationContainer.get_utilization_values()
    constraints_summary = MaterialUsageContainer.output_constraints_summary(model_year_range)

    return {
        "tech_choice_dict": tech_choice_dict,
        "tech_choice_records": tech_choice_records,
        "tech_rank_records": tech_rank_records,
        "plant_result_df": final_steel_plant_df,
        "active_check_results_dict": active_check_results_dict,
        "investment_cycle_ref_result": investment_df,
        "investment_dict_result": investment_dict,
        "plant_cycle_length_mapper_result": plant_cycle_length_mapper,
        "regional_capacity_results": regional_capacity_results,
        "plant_capacity_results": plant_capacity_results,
        "utilization_results": utilization_results,
        "production_demand_analysis": production_demand_analysis,
        "full_trade_summary": full_trade_summary,
        "material_usage_results": material_usage_results,
        "constraints_summary": constraints_summary,
    }


def choose_technology(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> dict:
    """Function containing the entire solver decision logic flow.
    1) In each year, the solver splits the plants non-switchers and switchers (secondary EAF plants and primary plants).
    2) The solver extracts the prior year technology of the non-switchers and assumes this is the current technology of the switchers.
    3) The material usage of the non-switching and secondary EAF plants is subtracted from the constraints and the remainder is then left over for the remaining switching plants.
    4) Plants are opened or closed according to the Demand for that year, the open and closing logic (potentially including trade). Which changes the capacity constraints.
    5) All switching plants are then sent through the `return_best_tech` function that decides the best technology depending on the switch type (main cycle or transitional switch).
    6) All results are saved to a dictionary which is outputted at the end of the year loop.

    Args:
        scenario_dict (int): Model Scenario settings.
        pkl_paths (dict): A dict from runtime containing any custom pkl_paths.
    Returns:
        dict: A dictionary containing the best technology resuls. Organised as [year][plant][best tech].
    """
    return choose_technology_core(ChooseTechnologyInput.from_filesystem(
        scenario_dict=scenario_dict, pkl_paths=pkl_paths))

def create_levelized_cost_actuals(results_dict: dict, scenario_dict: dict, pkl_paths: Union[dict, None] = None):
    # Create Levelized cost results
    rmi_mapper = create_country_mapper(path=str(PROJECT_PATH / PKL_DATA_IMPORTS))
    tech_capacity_df = tech_capacity_splits(
        steel_plants = results_dict["plant_result_df"],
        tech_choices = results_dict["tech_choice_dict"],
        capacity_dict = results_dict["plant_capacity_results"],
        active_check_results_dict = results_dict["active_check_results_dict"]
    )
    tech_capacity_df["region"] = tech_capacity_df["country_code"].apply(
        lambda x: rmi_mapper[x]
    )
    tech_capacity_df["cuf"] = tech_capacity_df.apply(
        utilization_mapper, utilization_results=results_dict["utilization_results"], axis=1
    )
    levelized_cost_results = generate_levelized_cost_results(
        scenario_dict=scenario_dict,
        pkl_paths=pkl_paths,
        steel_plant_df=tech_capacity_df,
        standard_plant_ref=False
    )
    return levelized_cost_results


@timer_func
def main_solver_flow(scenario_dict: dict, pkl_paths: Union[dict, None] = None, serialize: bool = False, model_run: str = "") -> dict:
    """Initiates the complete solver flow and serializes the outputs. Tracks all technology choices and plant changes.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        pkl_paths (Union[dict, None], optional): A dictionary containing custom pickle paths. Defaults to {}.
        serialize (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.
        model_run (str, optional): The run of the model to customize pkl folder paths. Defaults to "".

    Returns:
        dict: A dictionary containing the best technology results and the resultant steel plants. tech_choice_dict is organised as year: plant: best tech.
    """
    _, intermediate_path, final_path = return_pkl_paths(scenario_dict["scenario_name"], pkl_paths, model_run)

    results_dict = choose_technology(scenario_dict=scenario_dict, pkl_paths=pkl_paths)

    levelized_cost_results = create_levelized_cost_actuals(results_dict=results_dict, scenario_dict=scenario_dict, pkl_paths=pkl_paths)

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(
            results_dict["tech_choice_dict"], intermediate_path, "tech_choice_dict"
        )
        serialize_file(
            results_dict["tech_choice_records"],
            intermediate_path,
            "tech_choice_records",
        )
        serialize_file(
            results_dict["tech_rank_records"],
            intermediate_path,
            "tech_rank_records",
        )
        serialize_file(
            results_dict["plant_result_df"], intermediate_path, "plant_result_df"
        )
        serialize_file(
            results_dict["active_check_results_dict"],
            intermediate_path,
            "active_check_results_dict",
        )
        serialize_file(
            results_dict["investment_cycle_ref_result"],
            intermediate_path,
            "investment_cycle_ref_result",
        )
        serialize_file(
            results_dict["investment_dict_result"],
            intermediate_path,
            "investment_dict_result",
        )
        serialize_file(
            results_dict["plant_cycle_length_mapper_result"],
            intermediate_path,
            "plant_cycle_length_mapper_result",
        )
        serialize_file(
            results_dict["regional_capacity_results"],
            intermediate_path,
            "regional_capacity_results",
        )
        serialize_file(
            results_dict["plant_capacity_results"],
            intermediate_path,
            "plant_capacity_results",
        )
        serialize_file(
            results_dict["utilization_results"],
            intermediate_path,
            "utilization_results",
        )
        serialize_file(
            results_dict["production_demand_analysis"],
            intermediate_path,
            "production_demand_analysis",
        )
        serialize_file(
            results_dict["full_trade_summary"],
            intermediate_path,
            "full_trade_summary",
        )
        serialize_file(
            results_dict["material_usage_results"],
            intermediate_path,
            "material_usage_results",
        )
        serialize_file(
            results_dict["constraints_summary"],
            intermediate_path,
            "constraints_summary",
        )
        serialize_file(
            levelized_cost_results, final_path, "levelized_cost_results"
        )
    return results_dict
