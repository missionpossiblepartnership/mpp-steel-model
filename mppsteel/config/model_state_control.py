"""Class to manage the implementation of the state controller class."""
from mppsteel.config.model_grouping import (
    add_currency_rates_to_scenarios,
    data_preprocessing_scenarios, 
    full_flow, 
    full_model_iteration_run, 
    full_multiple_run_flow, 
    generic_data_preprocessing_only, 
    get_inputted_scenarios, 
    graphs_only, 
    half_model_run, 
    model_presolver, 
    model_results_phase, 
    multi_run_full_function, 
    multi_run_half_function, 
    multi_run_multi_scenario, 
    outputs_only, 
    results_and_output, 
    scenario_model_run, 
    total_opex_calculations
)
from mppsteel.data_load_and_format.data_import import load_import_data
from mppsteel.data_load_and_format.pe_model_formatter import format_pe_data
from mppsteel.data_load_and_format.steel_plant_formatter import steel_plant_processor
from mppsteel.data_preprocessing.emissions_reference_tables import (
    generate_emissions_flow,
)
from mppsteel.data_preprocessing.investment_cycles import investment_cycle_flow
from mppsteel.data_preprocessing.variable_plant_cost_archetypes import (
    generate_variable_plant_summary,
)
from mppsteel.data_preprocessing.levelized_cost import generate_levelized_cost_results
from mppsteel.model_solver.solver_flow import main_solver_flow
from mppsteel.data_preprocessing.tco_abatement_switch import (
    tco_presolver_reference,
    abatement_presolver_reference,
)
from mppsteel.model_results.production import production_results_flow
from mppsteel.model_results.cost_of_steelmaking import (
    generate_cost_of_steelmaking_results,
)
from mppsteel.model_results.global_metaresults import metaresults_flow
from mppsteel.model_results.investments import investment_results
from mppsteel.config.mypy_config_settings import MYPY_SCENARIO_TYPE
from mppsteel.utility.function_timer_utility import TIME_CONTAINER
from mppsteel.utility.file_handling_utility import (
    create_folders_if_nonexistant,
    get_scenario_pkl_path
)
from mppsteel.multi_run_module.multiple_runs import join_scenario_data
from mppsteel.config.reference_lists import (
    ITERATION_FILES_TO_AGGREGATE, 
    SCENARIO_SETTINGS_TO_ITERATE
)
from distributed import Client
from mppsteel.config.model_config import (
    DEFAULT_NUMBER_OF_RUNS,
    FOLDERS_TO_CHECK_IN_ORDER,
)
from mppsteel.config.model_scenarios import (
    BATCH_ITERATION_SCENARIOS,
    MAIN_SCENARIO_RUNS,
    DEFAULT_SCENARIO,
    SCENARIO_SETTINGS,
    SCENARIO_OPTIONS,
    ABATEMENT_SCENARIO,
    BAU_SCENARIO,
    CARBON_COST,
    TECH_MORATORIUM,
)
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)

class ModelStateControl:
    """A class to manage the model at runtime.
    """
    def __init__(self, args, timestamp: str, create_folder: bool = True) -> None:
        self.initialize_args(args, timestamp, create_folder)

    def initialize_args(self, args, timestamp: str, create_folder: bool = True) -> None:
        self.scenario_args = DEFAULT_SCENARIO
        self.number_of_runs = DEFAULT_NUMBER_OF_RUNS
        self.timestamp = timestamp
        self.initial_args_management(args)
        self.set_scenario_name()
        self.set_path()
        if create_folder:
            self.folder_creation()

        logger.info("Created Model State Control Instance")

    def set_scenario_name(self) -> None:
        self.scenario_name = str(self.scenario_args["scenario_name"])
        self.model_output_folder = f"{self.scenario_name} {self.timestamp}"

    def set_path(self) -> None:
        self.paths = {
            "intermediate_path": get_scenario_pkl_path(self.scenario_name, "intermediate"),
            "final_path": get_scenario_pkl_path(self.scenario_name, "final")
        }

    def folder_creation(self) -> None:
        create_folders_if_nonexistant(FOLDERS_TO_CHECK_IN_ORDER)
        create_folders_if_nonexistant([self.paths["intermediate_path"], self.paths["final_path"]])

    def initial_args_management(self, args) -> None:
        logger.info("""Establishing base args...""")
        # Manage localhost app for modin operations
        if (
            args.multi_run_half
            or args.multi_run_full
            or args.multi_run_multi_scenario
            or args.model_iterations_run
        ):
            client = Client()

        if args.number_of_runs:
            self.number_of_runs = int(args.number_of_runs)

        if args.choose_scenario:
            if args.choose_scenario in SCENARIO_OPTIONS.keys():
                logger.info(f"CORRECT SCENARIO CHOSEN: {args.choose_scenario}")
                scenario_args = SCENARIO_OPTIONS[args.choose_scenario]
            else:
                scenario_name_options = list(SCENARIO_OPTIONS.keys())
                logger.info(
                    f"INVALID SCENARIO INPUT: {args.choose_scenario}, please choose from {scenario_name_options}"
                )

        if args.custom_scenario:
            logger.info("Including custom parameter inputs.")
            scenario_args = get_inputted_scenarios(
                scenario_options=SCENARIO_SETTINGS, default_scenario=scenario_args
            )
            self.scenario_args = scenario_args
            self.scenario_name = str(scenario_args["scenario_name"])
        

        self.scenario_args = add_currency_rates_to_scenarios(self.scenario_args)

    def parse_multiprocessing_scenarios(self, args) -> None:
        logger.info("""Parsing args for multiprocessing scenario runs...""")
        if args.main_scenarios:
            logger.info(f"Running {MAIN_SCENARIO_RUNS} scenario options")
            full_multiple_run_flow(
                scenario_name=self.scenario_name, 
                main_scenario_runs=MAIN_SCENARIO_RUNS, 
                timestamp=self.timestamp
            )

        if args.multi_run_multi_scenario:
            logger.info(f"Running {MAIN_SCENARIO_RUNS} scenario options, {self.number_of_runs} times")
            multi_run_multi_scenario(
                main_scenario_runs=MAIN_SCENARIO_RUNS, 
                number_of_runs=self.number_of_runs, 
                timestamp=self.timestamp
            )

        if args.model_iterations_run:
            logger.info(f"Running {BATCH_ITERATION_SCENARIOS} scenarios, by iterating the following scenarios {SCENARIO_SETTINGS_TO_ITERATE}")
            full_model_iteration_run(
                batch_iteration_scenarios=BATCH_ITERATION_SCENARIOS,
                files_to_aggregate=ITERATION_FILES_TO_AGGREGATE,
                scenario_setting_to_iterate=SCENARIO_SETTINGS_TO_ITERATE, #["green_premium_scenario",]
                timestamp=self.timestamp
            )

        if args.multi_run_full:
            multi_run_full_function(
                scenario_dict=self.scenario_args, 
                number_of_runs=self.number_of_runs,
                timestamp=self.timestamp
            )

        if args.multi_run_half:
            multi_run_half_function(
                scenario_dict=self.scenario_args, 
                number_of_runs=self.number_of_runs,
                timestamp=self.timestamp
            )

        if args.join_final_data:
            join_scenario_data(
                scenario_options=MAIN_SCENARIO_RUNS,
                new_folder=True,
                timestamp=self.timestamp,
                final_outputs_only=True,
            )

    def parse_regular_scenario_runs(self, args) -> None:
        logger.info("""Checking for regular (single scenario) scenario runs...""")

        if args.full_model:
            full_flow(
                scenario_dict=self.scenario_args,
                new_folder=True,
                output_folder=self.model_output_folder,
                pkl_paths=None
            )

        if args.solver:
            main_solver_flow(
                scenario_dict=self.scenario_args, 
                serialize=True
            )

        if args.output:
            outputs_only(
                scenario_dict=self.scenario_args,
                new_folder=True,
                output_folder=self.model_output_folder,
            )

        if args.scenario_model_run:
            scenario_model_run(
                scenario_dict=self.scenario_args,
                pkl_paths=None,
                new_folder=True,
                output_folder=self.model_output_folder,
            )

        if args.half_model_run:
            half_model_run(
                scenario_dict=self.scenario_args,
                output_folder=self.model_output_folder
            )

        if args.data_import:
            load_import_data(serialize=True)

        if args.preprocessing:
            data_preprocessing_scenarios(scenario_dict=self.scenario_args)

        if args.presolver:
            model_presolver(scenario_dict=self.scenario_args)

        if args.results:
            model_results_phase(scenario_dict=self.scenario_args)

        if args.generic_preprocessing:
            generic_data_preprocessing_only(scenario_dict=self.scenario_args)

        if args.variable_costs:
            generate_variable_plant_summary(
                scenario_dict=self.scenario_args, serialize=True
            )

        if args.levelized_cost:
            generate_levelized_cost_results(
                scenario_dict=self.scenario_args,
                pkl_paths=None,
                serialize=True,
                standard_plant_ref=True,
            )

        if args.results_and_output:
            results_and_output(
                scenario_dict=self.scenario_args,
                new_folder=True,
                output_folder=self.model_output_folder,
            )

        if args.graphs:
            graphs_only(
                scenario_dict=self.scenario_args,
                new_folder=True,
                output_folder=self.model_output_folder,
            )

        if args.total_opex:
            total_opex_calculations(scenario_dict=self.scenario_args)

        if args.production:
            production_results_flow(self.scenario_args, pkl_paths=None, serialize=True)

        if args.cos:
            generate_cost_of_steelmaking_results(
                self.scenario_args, pkl_paths=None, serialize=True
            )

        if args.metaresults:
            metaresults_flow(self.scenario_args, pkl_paths=None, serialize=True)

        if args.investment:
            investment_results(scenario_dict=self.scenario_args)

        if args.tco:
            tco_presolver_reference(self.scenario_args, pkl_paths=None, serialize=True)

        if args.abatement:
            abatement_presolver_reference(self.scenario_args, pkl_paths=None, serialize=True)

        if args.emissivity:
            generate_emissions_flow(scenario_dict=self.scenario_args, serialize=True)

        if args.investment_cycles:
            investment_cycle_flow(scenario_dict=self.scenario_args, serialize=True)

        if args.pe_models:
            format_pe_data (scenario_dict=self.scenario_args)

        if args.steel_plants:
            steel_plant_processor(scenario_dict=self.scenario_args)

        time_container = TIME_CONTAINER.return_time_container()
        logger.info(time_container)

    def parse_runtime_args(self, args) -> None:
        self.parse_multiprocessing_scenarios(args)
        self.parse_regular_scenario_runs(args)
