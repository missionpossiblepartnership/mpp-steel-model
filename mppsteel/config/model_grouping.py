"""Model flow functions for the main script"""
import argparse

from datetime import datetime

import pandas as pd

from mppsteel.utility.utils import stdout_query, get_currency_rate
from mppsteel.utility.file_handling_utility import (
    pickle_to_csv,
    create_folder_if_nonexist,
    get_scenario_pkl_path,
    create_folders_if_nonexistant,
    read_pickle_folder,
    serialize_file,
)

from mppsteel.utility.log_utility import get_logger

from mppsteel.data_load_and_format.data_import import load_data
from mppsteel.data_load_and_format.reg_steel_demand_formatter import get_steel_demand
from mppsteel.data_preprocessing.timeseries_generator import generate_timeseries
from mppsteel.data_load_and_format.pe_model_formatter import format_pe_data
from mppsteel.data_load_and_format.steel_plant_formatter import steel_plant_processor
from mppsteel.data_load_and_format.data_interface import (
    create_capex_opex_dict,
    create_business_case_reference,
    generate_preprocessed_emissions_data,
)
from mppsteel.data_preprocessing.emissions_reference_tables import (
    generate_emissions_flow,
)
from mppsteel.data_preprocessing.capex_switching import create_capex_timeseries
from mppsteel.data_preprocessing.investment_cycles import investment_cycle_flow
from mppsteel.data_preprocessing.variable_plant_cost_archetypes import (
    generate_variable_plant_summary,
)
from mppsteel.data_preprocessing.levelized_cost import generate_levelized_cost_results
from mppsteel.model_solver.solver import solver_flow
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
from mppsteel.model_results.green_capacity_ratio import generate_gcr_df
from mppsteel.model_graphs.graph_production import (
    create_graphs,
    create_combined_scenario_graphs,
)

from mppsteel.config.model_config import (
    PKL_DATA_FORMATTED,
    PKL_FOLDER,
    USD_TO_EUR_CONVERSION_DEFAULT,
    OUTPUT_FOLDER,
    INTERMEDIATE_RESULT_PKL_FILES,
    FINAL_RESULT_PKL_FILES,
)
from mppsteel.config.model_scenarios import SCENARIO_OPTIONS

logger = get_logger(__name__)


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


def add_currency_rates_to_scenarios(scenario_dict: dict, live: bool = False) -> dict:
    eur_to_usd = 1 / USD_TO_EUR_CONVERSION_DEFAULT
    usd_to_eur = USD_TO_EUR_CONVERSION_DEFAULT
    if live:
        eur_to_usd = get_currency_rate("eur", "usd")
        usd_to_eur = get_currency_rate("usd", "eur")

    scenario_dict["eur_to_usd"] = eur_to_usd
    scenario_dict["usd_to_eur"] = usd_to_eur

    return scenario_dict


# Model phasing
def data_import_stage() -> None:
    load_data(serialize=True)


def data_preprocessing_generic() -> None:
    steel_plant_processor(serialize=True)
    create_capex_opex_dict(serialize=True)
    create_capex_timeseries(serialize=True)
    create_business_case_reference(serialize=True)
    investment_cycle_flow(serialize=True)


def data_preprocessing_scenarios(scenario_dict: dict) -> None:
    get_steel_demand(scenario_dict=scenario_dict, serialize=True)
    generate_timeseries(scenario_dict=scenario_dict, serialize=True)
    format_pe_data(scenario_dict=scenario_dict, serialize=True, standardize_units=True)
    generate_preprocessed_emissions_data(serialize=True)
    generate_emissions_flow(scenario_dict=scenario_dict, serialize=True)
    generate_variable_plant_summary(scenario_dict, serialize=True)
    generate_levelized_cost_results(scenario_dict=scenario_dict, serialize=True, standard_plant_ref=True)
    tco_presolver_reference(scenario_dict, serialize=True)
    abatement_presolver_reference(scenario_dict, serialize=True)


def investment_cycles() -> None:
    investment_cycle_flow(serialize=True)


def model_presolver(scenario_dict: dict) -> None:
    tco_presolver_reference(scenario_dict, serialize=True)
    abatement_presolver_reference(scenario_dict, serialize=True)


def scenario_preprocessing_phase(scenario_dict: dict) -> None:
    data_preprocessing_scenarios(scenario_dict)
    model_presolver(scenario_dict)


def model_results_phase(scenario_dict: dict) -> None:
    production_results_flow(scenario_dict, serialize=True)
    investment_results(scenario_dict, serialize=True)
    metaresults_flow(scenario_dict, serialize=True)
    generate_cost_of_steelmaking_results(scenario_dict, serialize=True)
    generate_gcr_df(scenario_dict, serialize=True)


def model_outputs_phase(
    scenario_dict: dict, new_folder: bool = False, output_folder: str = ""
) -> None:
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f"{OUTPUT_FOLDER}/{output_folder}"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath

    # Save Intermediate Pickle Files
    intermediate_path = get_scenario_pkl_path(
        scenario_dict["scenario_name"], "intermediate"
    )
    final_path = get_scenario_pkl_path(scenario_dict["scenario_name"], "final")

    pickle_to_csv(save_path, PKL_DATA_FORMATTED, "capex_switching_df", reset_index=True)

    # Save Final Pickle Files
    for pkl_file in INTERMEDIATE_RESULT_PKL_FILES:
        pickle_to_csv(save_path, intermediate_path, pkl_file)

    for pkl_file in FINAL_RESULT_PKL_FILES:
        pickle_to_csv(save_path, final_path, pkl_file)


def join_scenario_data(
    scenario_options: list,
    new_folder: bool = True,
    timestamp: str = "",
    final_outputs_only: bool = True,
):
    logger.info(f"Joining the Following Scenario Data {scenario_options}")
    combined_ouptut_pkl_folder = f"{PKL_FOLDER}/combined_output"
    create_folder_if_nonexist(combined_ouptut_pkl_folder)
    output_save_path = OUTPUT_FOLDER
    output_folder_graphs = f"{output_save_path}/graphs"
    output_folder_name = f"combined_output {timestamp}"
    if new_folder:
        output_folder_filepath = f"{OUTPUT_FOLDER}/{output_folder_name}"
        output_folder_graphs = f"{output_folder_filepath}/graphs"
        create_folder_if_nonexist(output_folder_filepath)
        create_folder_if_nonexist(output_folder_graphs)
        output_save_path = output_folder_filepath
        output_save_path_graphs = output_folder_graphs

    if not final_outputs_only:
        for output_file in INTERMEDIATE_RESULT_PKL_FILES:
            output_container = []
            for scenario_name in scenario_options:
                path = get_scenario_pkl_path(scenario_name, "intermediate")
                output_container.append(read_pickle_folder(path, output_file, "df"))

            combined_output = pd.concat(output_container).reset_index(drop=True)
            serialize_file(combined_output, combined_ouptut_pkl_folder, output_file)
            combined_output.to_csv(f"{output_save_path}/{output_file}.csv", index=False)

    for output_file in FINAL_RESULT_PKL_FILES:
        output_container = []
        for scenario_name in scenario_options:
            path = get_scenario_pkl_path(scenario_name, "final")
            output_container.append(read_pickle_folder(path, output_file, "df"))

        combined_output = pd.concat(output_container).reset_index(drop=True)
        serialize_file(combined_output, combined_ouptut_pkl_folder, output_file)
        combined_output.to_csv(f"{output_save_path}/{output_file}.csv", index=False)

    create_combined_scenario_graphs(filepath=output_save_path_graphs)


def model_graphs_phase(
    scenario_dict: dict, new_folder: bool = False, model_output_folder: str = ""
) -> None:
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f"{OUTPUT_FOLDER}/{model_output_folder}/graphs"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    create_graphs(filepath=save_path, scenario_dict=scenario_dict)


# Group phases
def data_import_refresh() -> None:
    data_import_stage()


def data_preprocessing_refresh(scenario_dict: dict) -> None:
    data_preprocessing_generic()
    data_preprocessing_scenarios(scenario_dict)


def data_import_and_preprocessing_refresh() -> None:
    data_import_stage()
    data_preprocessing_generic()


def tco_and_abatement_calculations(scenario_dict: dict) -> None:
    model_presolver(scenario_dict)


def scenario_batch_run(scenario: str, dated_output_folder: bool) -> None:
    # create new folders for path
    intermediate_path = get_scenario_pkl_path(scenario, "intermediate")
    final_path = get_scenario_pkl_path(scenario, "final")
    create_folders_if_nonexistant([intermediate_path, final_path])
    # Set up scenario and metadata
    scenario_args = SCENARIO_OPTIONS[scenario]
    scenario_args = add_currency_rates_to_scenarios(scenario_args)
    timestamp = datetime.today().strftime("%d-%m-%y %H-%M")
    model_output_folder = f"{scenario} {timestamp}"

    # Model run
    scenario_model_run(scenario_args, dated_output_folder, model_output_folder)


def scenario_model_run(
    scenario_dict: dict, dated_output_folder: bool, model_output_folder: str
) -> None:
    scenario_preprocessing_phase(scenario_dict)
    solver_flow(scenario_dict, serialize=True)
    model_results_phase(scenario_dict)
    model_outputs_phase(scenario_dict, dated_output_folder, model_output_folder)
    model_graphs_phase(scenario_dict, dated_output_folder, model_output_folder)


def results_and_output(
    scenario_dict: dict, dated_output_folder: bool, model_output_folder: str
) -> None:
    model_results_phase(scenario_dict)
    model_outputs_phase(scenario_dict, dated_output_folder, model_output_folder)
    model_graphs_phase(scenario_dict, dated_output_folder, model_output_folder)


def outputs_only(
    scenario_dict: dict, dated_output_folder: bool, model_output_folder: str
) -> None:
    model_outputs_phase(scenario_dict, dated_output_folder, model_output_folder)
    model_graphs_phase(scenario_dict, dated_output_folder, model_output_folder)


def graphs_only(
    scenario_dict: dict, model_output_folder: str, dated_output_folder: bool
) -> None:
    model_graphs_phase(scenario_dict, dated_output_folder, model_output_folder)


def full_flow(
    scenario_dict: dict, dated_output_folder: bool, model_output_folder: str
) -> None:
    data_import_and_preprocessing_refresh()
    scenario_model_run(scenario_dict, dated_output_folder, model_output_folder)


def generate_minimodels(scenario_dict: dict) -> None:
    generate_timeseries(scenario_dict, serialize=True)


def tco_switch_reference(scenario_dict: dict) -> None:
    tco_presolver_reference(scenario_dict, serialize=True)


def abatement_switch_reference(scenario_dict: dict) -> None:
    abatement_presolver_reference(scenario_dict, serialize=True)


def production_flow(scenario_dict: dict) -> None:
    production_results_flow(scenario_dict, serialize=True)


def cos_flow(scenario_dict: dict) -> None:
    generate_cost_of_steelmaking_results(scenario_dict, serialize=True)


def global_metaresults_flow(scenario_dict: dict):
    metaresults_flow(scenario_dict, serialize=True)


def investment_flow(scenario_dict: dict) -> None:
    investment_results(scenario_dict, serialize=True)


def get_emissivity(scenario_dict: dict) -> None:
    generate_emissions_flow(scenario_dict, serialize=True)


def lcost_flow(scenario_dict: dict) -> None:
    generate_levelized_cost_results(scenario_dict=scenario_dict, serialize=True, standard_plant_ref=True)


def gcr_flow(scenario_dict: dict) -> None:
    generate_gcr_df(scenario_dict, serialize=True)


parser = argparse.ArgumentParser(
    description="The MPP Python Steel Model Command Line Interface", add_help=False
)

### THESE ARGUMENTS ARE FOR MAIN MODEL FLOWS: RUNNING SECTIONS OF THE MODEL IN FULL
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
    "--main_scenarios",
    action="store_true",
    help="Runs specified scenarios using multiprocessing using scenario_batch_run",
)  # scenario_batch_run
parser.add_argument(
    "-f", "--full_model", action="store_true", help="Runs the complete model flow"
)  # full_flow


### THESE ARGUMENTS ARE FOR DEVELOPMENT PRUPORSES: RUNNING SECTIONS OF THE MODEL IN ISOLATION
parser.add_argument(
    "-s", "--solver", action="store_true", help="Runs the solver scripts directly"
)  # solver_flow
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
    "--scenario_model_run",
    action="store_true",
    help="Runs the complete scenario adjusted scripts directly",
)  # scenario_model_run
parser.add_argument(
    "-i",
    "--data_import",
    action="store_true",
    help="Runs the data import scripts scripts directly",
)  # data_import_refresh
parser.add_argument(
    "-d",
    "--presolver",
    action="store_true",
    help="Runs the model_presolver scripts directly",
)  # model_presolver
parser.add_argument(
    "-r",
    "--results",
    action="store_true",
    help="Runs the model results scripts directly",
)  # model_results_phase
parser.add_argument(
    "-b",
    "--generic_preprocessing",
    action="store_true",
    help="Runs the data_preprocessing_generic script directly",
)  # generic_preprocessing
parser.add_argument(
    "-v",
    "--variable_costs",
    action="store_true",
    help="Runs the variable costs summary script directly",
)  # generate_variable_plant_summary
parser.add_argument(
    "-l",
    "--levelized_cost",
    action="store_true",
    help="Runs the levelized cost script directly",
)  # lcost_flow
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
    "-u",
    "--cos",
    action="store_true",
    help="Runs the cost of steelmaking script directly",
)  # cos_flow
parser.add_argument(
    "-k",
    "--metaresults",
    action="store_true",
    help="Runs the global metaresults script directly",
)  # cost of steelmaking
parser.add_argument(
    "-x",
    "--join_final_data",
    action="store_true",
    help="Joins final data sets from different scenarios",
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
parser.add_argument(
    "-m",
    "--green_replacement_ratio",
    action="store_true",
    help="Runs the gcr_flow script only",
)  # gcr_flow
