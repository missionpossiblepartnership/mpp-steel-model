"""Model flow functions for the main script"""
import argparse

from mppsteel.utility.utils import stdout_query, get_currency_rate
from mppsteel.utility.file_handling_utility import (
    pickle_to_csv,
    create_folder_if_nonexist,
)
from mppsteel.utility.log_utility import get_logger

from mppsteel.data_loading.data_import import load_data
from mppsteel.data_loading.reg_steel_demand_formatter import get_steel_demand
from mppsteel.minimodels.timeseries_generator import generate_timeseries
from mppsteel.data_loading.standardise_business_cases import standardise_business_cases
from mppsteel.data_loading.business_case_tests import (
    create_bc_test_df,
    test_all_technology_business_cases,
)
from mppsteel.data_loading.pe_model_formatter import format_pe_data
from mppsteel.data_loading.steel_plant_formatter import steel_plant_processor
from mppsteel.data_loading.country_reference import create_country_ref
from mppsteel.data_loading.data_interface import (
    create_capex_opex_dict,
    generate_preprocessed_emissions_data,
)
from mppsteel.model.emissions_reference_tables import generate_emissions_flow
from mppsteel.model.capex_switching import create_capex_timeseries
from mppsteel.model.investment_cycles import investment_cycle_flow
from mppsteel.model.variable_plant_cost_archetypes import (
    generate_variable_plant_summary,
)
from mppsteel.model.solver import solver_flow
from mppsteel.model.tco_abatement_switch import (
    tco_presolver_reference,
    abatement_presolver_reference,
)
from mppsteel.results.production import production_results_flow
from mppsteel.results.cost_of_steelmaking import generate_cost_of_steelmaking_results
from mppsteel.results.global_metaresults import metaresults_flow
from mppsteel.results.investments import investment_results
from mppsteel.graphs.graph_production import create_graphs

from mppsteel.model_config import (
    MODEL_YEAR_END,
    OUTPUT_FOLDER,
    PKL_DATA_FINAL,
    PKL_DATA_INTERMEDIATE,
    BC_TEST_FOLDER,
)

logger = get_logger("Main Model Code")


def stdout_question(
    count_iter: int, scenario_type: str, scenario_options: dict, default_dict: dict
) -> str:
    return f"""
    Scenario Option {count_iter+1}/{len(scenario_options)}: {scenario_type}
    Default value: {default_dict[scenario_type]}.
    To keep default, leave blank and press ENTER, else enter a different value from the options presented.
    ---> Options {scenario_options[scenario_type]}
    """


def get_inputted_scenarios(scenario_options: dict, default_scenario: dict) -> dict:
    inputted_scenario_args = {}
    for count, scenario in enumerate(scenario_options.keys()):
        question = stdout_question(count, scenario, scenario_options, default_scenario)
        inputted_scenario_args[scenario] = stdout_query(
            question, default_scenario[scenario], scenario_options[scenario]
        )
    return inputted_scenario_args


def add_currency_rates_to_scenarios(scenario_dict: dict) -> dict:
    scenario_dict["eur_usd"] = get_currency_rate("eur", "usd")
    scenario_dict["usd_eur"] = get_currency_rate("usd", "eur")
    return scenario_dict


# Model phasing
def data_import_stage() -> None:
    load_data(serialize=True)
    get_steel_demand(serialize=True)
    format_pe_data(serialize=True)
    standardise_business_cases(serialize=True)
    create_country_ref(serialize=True)


def data_preprocessing_phase(scenario_dict: dict) -> None:
    generate_timeseries(serialize=True, scenario_dict=scenario_dict)
    steel_plant_processor(serialize=True, remove_non_operating_plants=True)
    create_capex_opex_dict(serialize=True)
    generate_preprocessed_emissions_data(serialize=True)
    generate_emissions_flow(scenario_dict=scenario_dict, serialize=True)
    create_capex_timeseries(serialize=True)
    investment_cycle_flow(serialize=True)
    generate_variable_plant_summary(scenario_dict, serialize=True)


def model_presolver(scenario_dict: dict) -> None:
    tco_presolver_reference(scenario_dict, serialize=True)
    abatement_presolver_reference(scenario_dict, serialize=True)


def model_calculation_phase(scenario_dict: dict) -> None:
    solver_flow(scenario_dict, year_end=MODEL_YEAR_END, serialize=True)


def model_results_phase(scenario_dict: dict) -> None:
    production_results_flow(scenario_dict, serialize=True)
    generate_cost_of_steelmaking_results(scenario_dict, serialize=True)
    metaresults_flow(scenario_dict, serialize=True)
    investment_results(scenario_dict, serialize=True)


def model_outputs_phase(new_folder: bool = False, timestamp: str = "") -> None:
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f"{OUTPUT_FOLDER}/{timestamp}"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath

    # Save Intermediate Pickle Files
    pickle_to_csv(save_path, PKL_DATA_INTERMEDIATE, "steel_plants_processed")
    pickle_to_csv(save_path, PKL_DATA_INTERMEDIATE, "capex_switching_df")
    pickle_to_csv(save_path, PKL_DATA_INTERMEDIATE, "calculated_emissivity_combined")
    pickle_to_csv(save_path, PKL_DATA_INTERMEDIATE, "emissivity_abatement_switches")
    pickle_to_csv(save_path, PKL_DATA_INTERMEDIATE, "tco_summary")
    pickle_to_csv(save_path, PKL_DATA_INTERMEDIATE, "tco_reference_data")

    # Save Final Pickle Files
    pkl_files = [
        "production_resource_usage",
        "production_emissions",
        "global_metaresults",
        "investment_results",
    ]
    for pkl_file in pkl_files:
        pickle_to_csv(save_path, PKL_DATA_FINAL, pkl_file)


def model_graphs_phase(new_folder: bool = False, timestamp: str = "") -> None:
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f"{OUTPUT_FOLDER}/{timestamp}/graphs"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    create_graphs(save_path)


# Group phases
def data_import_refresh() -> None:
    data_import_stage()


def data_preprocessing_refresh(scenario_dict: dict) -> None:
    data_preprocessing_phase(scenario_dict)


def data_import_and_preprocessing_refresh(scenario_dict: dict) -> None:
    data_import_stage()
    data_preprocessing_phase(scenario_dict)


def tco_and_abatement_calculations(scenario_dict: dict) -> None:
    model_presolver(scenario_dict)


def scenario_batch_run(
    scenario_dict: dict, dated_output_folder: bool, timestamp: str
) -> None:
    data_preprocessing_phase(scenario_dict)
    model_presolver(scenario_dict)
    model_calculation_phase(scenario_dict)
    model_results_phase(scenario_dict)
    model_outputs_phase(dated_output_folder, timestamp)


def half_model_run(
    scenario_dict: dict, dated_output_folder: bool, timestamp: str
) -> None:
    model_calculation_phase(scenario_dict)
    model_results_phase(scenario_dict)
    model_outputs_phase(dated_output_folder, timestamp)
    model_graphs_phase(dated_output_folder, timestamp)


def results_and_output(
    scenario_dict: dict, dated_output_folder: bool, timestamp: str
) -> None:
    model_results_phase(scenario_dict)
    model_outputs_phase(dated_output_folder, timestamp)
    model_graphs_phase(dated_output_folder, timestamp)


def outputs_only(dated_output_folder: bool, timestamp: str) -> None:
    model_outputs_phase(dated_output_folder, timestamp)
    model_graphs_phase(dated_output_folder, timestamp)


def graphs_only(timestamp: str, dated_output_folder: bool) -> None:
    model_graphs_phase(dated_output_folder, timestamp)


def full_flow(scenario_dict: dict, dated_output_folder: bool, timestamp: str) -> None:
    data_import_and_preprocessing_refresh(scenario_dict)
    model_presolver(scenario_dict)
    half_model_run(scenario_dict, dated_output_folder, timestamp)


def business_case_tests(
    new_folder: bool = False, timestamp: str = "", create_test_df: bool = True
) -> None:
    save_path = BC_TEST_FOLDER
    if new_folder:
        folder_filepath = f"{BC_TEST_FOLDER}/{timestamp}"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    if create_test_df:
        create_bc_test_df(serialize=True)
    test_all_technology_business_cases(save_path)


def generate_minimodels(scenario_dict: dict) -> None:
    generate_timeseries(serialize=True, scenario_dict=scenario_dict)


def tco_switch_reference(scenario_dict: dict) -> None:
    tco_presolver_reference(scenario_dict, serialize=True)


def abatement_switch_reference(scenario_dict: dict) -> None:
    abatement_presolver_reference(scenario_dict, serialize=True)


def production_flow(scenario_dict: dict) -> None:
    production_results_flow(scenario_dict, serialize=True)


def investment_flow(scenario_dict: dict) -> None:
    investment_results(scenario_dict, serialize=True)


def get_emissivity() -> None:
    generate_emissions_flow(False)


parser = argparse.ArgumentParser(
    description="The MPP Python Steel Model Command Line Interface", add_help=False
)
parser.add_argument(
    "-q",
    "--custom_scenario",
    action="store_true",
    help="Adds custom scenario inputs to the model",
)
parser.add_argument(
    "-c",
    "--choose_scenario",
    action="store",
    help="Runs a single fixed scenario to the model that you can specify by name",
)
parser.add_argument(
    "-a",
    "--all_scenarios",
    action="store_true",
    help="Runs all fixed scenarios in the model",
)
parser.add_argument(
    "-f", "--full_model", action="store_true", help="Runs the complete model flow"
)  # full_flow
parser.add_argument(
    "-s", "--solver", action="store_true", help="Runs the solver scripts directly"
)  # data_import_and_preprocessing_refresh
parser.add_argument(
    "-p",
    "--preprocessing",
    action="store_true",
    help="Runs the preprocessing scripts directly",
)  # data_preprocessing_refresh
parser.add_argument(
    "-o", "--output", action="store_true", help="Runs the output scripts directly"
)  # outputs_only
parser.add_argument(
    "-h",
    "--half_model",
    action="store_true",
    help="Runs the half model sctips scripts directly",
)  # half_model_run
parser.add_argument(
    "-i",
    "--data_import",
    action="store_true",
    help="Runs the data import scripts scripts directly",
)  # data_import_refresh
parser.add_argument(
    "-d",
    "--data_refresh",
    action="store_true",
    help="Runs the data refresh scripts directly",
)  # data_import_and_preprocessing_refresh
parser.add_argument(
    "-r",
    "--results",
    action="store_true",
    help="Runs the model results scripts directly",
)  # model_results_phase
parser.add_argument(
    "-b",
    "--business_cases",
    action="store_true",
    help="Runs the business cases script directly",
)  # standardise_business_cases
parser.add_argument(
    "-v",
    "--variable_costs",
    action="store_true",
    help="Runs the variable costs sumary script directly",
)  # generate_variable_plant_summary
parser.add_argument(
    "-t",
    "--results_and_output",
    action="store_true",
    help="Runs the results and output scripts directly",
)  # results_and_output
parser.add_argument(
    "-g", "--graphs", action="store_true", help="Runs the graph output script directly"
)  # graphs_only
parser.add_argument(
    "-n",
    "--minimodels",
    action="store_true",
    help="Runs the minimodels script directly",
)  # generate_minimodels
parser.add_argument(
    "-w",
    "--production",
    action="store_true",
    help="Runs the production script directly",
)  # production_flow
parser.add_argument(
    "-e",
    "--investment",
    action="store_true",
    help="Runs the investments script directly",
)  # investment_flow
parser.add_argument(
    "-x", "--ta", action="store_true", help="Runs the tco and abatement scripts only"
)  # tco_and_abatement_calculations
parser.add_argument(
    "-y", "--tco", action="store_true", help="Runs the tco script only"
)  # tco_switch_reference
parser.add_argument(
    "-z", "--abatement", action="store_true", help="Runs the abatament script only"
)  # abatement_switch_reference
parser.add_argument(
    "-j", "--emissivity", action="store_true", help="Runs the emissivity script only"
)  # get_emissivity
