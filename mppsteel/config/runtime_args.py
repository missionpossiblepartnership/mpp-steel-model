"""Runtime arguments for the model"""

import argparse

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
parser.add_argument(
    "-n",
    "--number_of_runs",
    action="store",
    help="The number of runs that the model should run for a multi-model run",
)

### THESE ARGUMENTS ARE FOR DEVELOPMENT PRUPORSES: RUNNING SECTIONS OF THE MODEL IN ISOLATION
parser.add_argument(
    "-s", "--solver", action="store_true", help="Runs the solver scripts directly"
)  # main_solver_flow
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
)  # load_import_data
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
)  # generate_levelized_cost_results
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
    "--total_opex",
    action="store_true",
    help="Runs the total_opex_calculations script grouping",
)  # total_opex_calculations
parser.add_argument(
    "-w",
    "--production",
    action="store_true",
    help="Runs the production script directly",
)  # production_results_flow
parser.add_argument(
    "-e",
    "--investment",
    action="store_true",
    help="Runs the investments script directly",
)  # investment_results
parser.add_argument(
    "-u",
    "--cos",
    action="store_true",
    help="Runs the cost of steelmaking script directly",
)  # generate_cost_of_steelmaking_results
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
)  # model_presolver
parser.add_argument(
    "-y", "--tco", action="store_true", help="Runs the tco script only"
)  # tco_presolver_reference
parser.add_argument(
    "-z", "--abatement", action="store_true", help="Runs the abatament script only"
)  # abatement_presolver_reference
parser.add_argument(
    "-j", "--emissivity", action="store_true", help="Runs the emissivity script only"
)  # generate_emissions_flow
parser.add_argument(
    "--investment_cycles",
    action="store_true",
    help="Runs script to create investment cycles",
)  # investment_cycles
parser.add_argument("--pe_models", action="store_true", help="Runs the PE Model script")
parser.add_argument(
    "--multi_run_full",
    action="store_true",
    help="Runs a full model scenario multiple times (from solver stage) and stores aggregated results",
)
parser.add_argument(
    "--multi_run_half",
    action="store_true",
    help="Runs a half model scenario multiple times (from solver stage) and stores aggregated results",
)
parser.add_argument(
    "--half_model_run",
    action="store_true",
    help="Runs a half model (from solver stage onwards) and stores results",
)
parser.add_argument(
    "--multi_run_multi_scenario",
    action="store_true",
    help="Runs the model multiple times for multiple scenarios",
)
parser.add_argument(
    "--steel_plants", action="store_true", help="Runs the steel plant formatter"
)  # steel_plant_processor

parser.add_argument(
    "--model_iterations_run",
    action="store_true",
    help="Runs the multiple iterations of each scenario",
)
