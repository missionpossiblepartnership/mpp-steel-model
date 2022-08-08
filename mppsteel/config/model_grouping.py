"""Model flow functions for the main script"""

from typing import Union
from datetime import datetime

from mppsteel.utility.utils import stdout_query, get_currency_rate
from mppsteel.utility.file_handling_utility import (
    pickle_to_csv,
    create_folder_if_nonexist,
    get_scenario_pkl_path,
    create_folders_if_nonexistant,
    return_pkl_paths
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
from mppsteel.data_preprocessing.carbon_tax_reference import generate_carbon_tax_reference
from mppsteel.data_preprocessing.total_opex_reference import generate_total_opex_cost_reference
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
from mppsteel.model_results.green_capacity_ratio import generate_gcr_df
from mppsteel.model_graphs.graph_production import (
    create_graphs,
)

from mppsteel.config.model_config import (
    DATETIME_FORMAT,
    PKL_DATA_FORMATTED,
    USD_TO_EUR_CONVERSION_DEFAULT,
    OUTPUT_FOLDER,
    INTERMEDIATE_RESULT_PKL_FILES,
    FINAL_RESULT_PKL_FILES
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

#Hello
# Model phasing
def data_import_stage() -> None:
    load_data(serialize=True)


def data_preprocessing_generic_1() -> None:
    create_capex_opex_dict(serialize=True)
    create_capex_timeseries(serialize=True)
    create_business_case_reference(serialize=True)

def data_preprocessing_generic_2(scenario_dict):
    steel_plant_processor(scenario_dict=scenario_dict, serialize=True)
    investment_cycle_flow(scenario_dict=scenario_dict, serialize=True)
    generate_preprocessed_emissions_data(serialize=True)

def data_preprocessing_scenarios(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    get_steel_demand(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_timeseries(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True)
    format_pe_data(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True, standardize_units=True)
    generate_emissions_flow(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_variable_plant_summary(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_carbon_tax_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_total_opex_cost_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_levelized_cost_results(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True, standard_plant_ref=True)
    tco_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    abatement_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)

def total_opex_calculations(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    generate_variable_plant_summary(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_carbon_tax_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_total_opex_cost_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    generate_levelized_cost_results(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True, standard_plant_ref=True)
    tco_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def investment_cycles() -> None:
    investment_cycle_flow(serialize=True)


def model_presolver(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    tco_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    abatement_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def scenario_preprocessing_phase(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    data_preprocessing_scenarios(scenario_dict, pkl_paths=pkl_paths)

# model_run (str, optional): The run of the model to customize pkl folder paths. Defaults to "".
def model_results_phase(scenario_dict: dict, pkl_paths: Union[dict, None] = None, model_run: str = "") -> None:
    production_results_flow(scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run)
    investment_results(scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run)
    metaresults_flow(scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run)
    generate_cost_of_steelmaking_results(scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run)
    generate_gcr_df(scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=model_run)


def model_outputs_phase(
    scenario_dict: dict, pkl_paths: Union[dict, None] = None, new_folder: bool = False, output_folder: str = ""
) -> None:
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f"{OUTPUT_FOLDER}/{output_folder}"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath

    # Save Intermediate Pickle Files
    _, intermediate_path, final_path = return_pkl_paths(scenario_name=scenario_dict["scenario_name"], paths=pkl_paths)

    pickle_to_csv(save_path, PKL_DATA_FORMATTED, "capex_switching_df", reset_index=True)

    # Save Final Pickle Files
    for pkl_file in INTERMEDIATE_RESULT_PKL_FILES:
        pickle_to_csv(save_path, intermediate_path, pkl_file)

    for pkl_file in FINAL_RESULT_PKL_FILES:
        pickle_to_csv(save_path, final_path, pkl_file)


def model_graphs_phase(
    scenario_dict: dict, pkl_paths: Union[dict, None] = None, new_folder: bool = False, model_output_folder: str = ""
) -> None:
    save_path = OUTPUT_FOLDER
    if new_folder:
        folder_filepath = f"{OUTPUT_FOLDER}/{model_output_folder}/graphs"
        create_folder_if_nonexist(folder_filepath)
        save_path = folder_filepath
    create_graphs(filepath=save_path, scenario_dict=scenario_dict, pkl_paths=pkl_paths)


# Group phases
def data_import_refresh() -> None:
    data_import_stage()


def data_preprocessing_refresh(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    data_preprocessing_generic_1()
    data_preprocessing_generic_2(scenario_dict)
    data_preprocessing_scenarios(scenario_dict, pkl_paths=pkl_paths)


def data_import_and_preprocessing_refresh(scenario_dict) -> None:
    data_import_stage()
    data_preprocessing_generic_1()
    data_preprocessing_generic_2(scenario_dict)


def tco_and_abatement_calculations(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    model_presolver(scenario_dict, pkl_paths=pkl_paths)


def scenario_batch_run(scenario_dict: dict, dated_output_folder: bool = False, iteration_run: bool = False, include_outputs: bool = True) -> None:
    scenario_name = scenario_dict["scenario_name"]
    # create new folders for path
    intermediate_path = get_scenario_pkl_path(scenario=scenario_name, pkl_folder_type="intermediate", iteration_run=iteration_run)
    final_path = get_scenario_pkl_path(scenario=scenario_name, pkl_folder_type="final", iteration_run=iteration_run)
    pkl_paths = {"intermediate_path": intermediate_path, "final_path": final_path}
    create_folders_if_nonexistant([intermediate_path, final_path])
    # Set up scenario and metadata
    scenario_args = dict(scenario_dict)
    scenario_args = add_currency_rates_to_scenarios(scenario_args)
    timestamp = datetime.now().strftime(DATETIME_FORMAT)
    model_output_folder = f"{scenario_name} {timestamp}"

    # Model run
    scenario_model_run(
        scenario_dict=scenario_args,
        pkl_paths=pkl_paths,
        dated_output_folder=dated_output_folder,
        model_output_folder=model_output_folder,
        include_outputs=include_outputs
    )


def scenario_model_run(
    scenario_dict: dict, pkl_paths: dict, dated_output_folder: bool, model_output_folder: str, include_outputs: bool = True
) -> None:
    scenario_preprocessing_phase(scenario_dict, pkl_paths=pkl_paths)
    main_solver_flow(scenario_dict, pkl_paths=pkl_paths, serialize=True)
    model_results_phase(scenario_dict, pkl_paths=pkl_paths)
    if include_outputs:
        model_outputs_phase(scenario_dict, pkl_paths=pkl_paths, dated_output_folder=dated_output_folder, model_output_folder=model_output_folder)
        model_graphs_phase(scenario_dict, pkl_paths=pkl_paths, dated_output_folder=dated_output_folder, model_output_folder=model_output_folder)


def results_and_output(
    scenario_dict: dict, pkl_paths: dict, dated_output_folder: bool, model_output_folder: str
) -> None:
    model_results_phase(scenario_dict, pkl_paths=pkl_paths)
    model_outputs_phase(scenario_dict, pkl_paths=pkl_paths, dated_output_folder=dated_output_folder, model_output_folder=model_output_folder)
    model_graphs_phase(scenario_dict, pkl_paths=pkl_paths, dated_output_folder=dated_output_folder, model_output_folder=model_output_folder)


def outputs_only(
    scenario_dict: dict, pkl_paths: dict, dated_output_folder: bool, model_output_folder: str
) -> None:
    model_outputs_phase(scenario_dict, pkl_paths, dated_output_folder, model_output_folder)
    model_graphs_phase(scenario_dict, pkl_paths, dated_output_folder, model_output_folder)


def graphs_only(
    scenario_dict: dict, pkl_paths: dict, model_output_folder: str, dated_output_folder: bool
) -> None:
    model_graphs_phase(scenario_dict, pkl_paths, dated_output_folder, model_output_folder)


def full_flow(
    scenario_dict: dict, pkl_paths: dict, dated_output_folder: bool, model_output_folder: str
) -> None:
    data_import_and_preprocessing_refresh(scenario_dict)
    scenario_model_run(scenario_dict, pkl_paths=pkl_paths, dated_output_folder=dated_output_folder, model_output_folder=model_output_folder)


def generate_minimodels(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    generate_timeseries(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def tco_switch_reference(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    tco_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def abatement_switch_reference(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    abatement_presolver_reference(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def production_flow(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    production_results_flow(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def cos_flow(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    generate_cost_of_steelmaking_results(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def global_metaresults_flow(scenario_dict: dict, pkl_paths: Union[dict, None] = None):
    metaresults_flow(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def investment_flow(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    investment_results(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def get_emissivity(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    generate_emissions_flow(scenario_dict, pkl_paths=pkl_paths, serialize=True)


def lcost_flow(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    generate_levelized_cost_results(scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True, standard_plant_ref=True)


def gcr_flow(scenario_dict: dict, pkl_paths: Union[dict, None] = None) -> None:
    generate_gcr_df(scenario_dict, pkl_paths=pkl_paths, serialize=True)
