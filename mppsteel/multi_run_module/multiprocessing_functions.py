"""Script containing functions that include multiprocessing"""

import multiprocessing as mp
import time
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Sized, Union, Sequence
from mppsteel.config.mypy_config_settings import MYPY_SCENARIO_TYPE, MYPY_SCENARIO_TYPE_DICT


from mppsteel.model_solver.solver_flow import main_solver_flow
from mppsteel.config.model_grouping import model_results_phase
from mppsteel.utility.file_handling_utility import generate_files_to_path_dict

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def create_pool(processes_to_run: Sized) -> mp.pool.Pool:
    """Creates a Multiprocessing pool with the number of processess based on the length of processes_to_run.

    Args:
        processes_to_run (Sized): The number of processes to run.

    Returns:
        mp.Pool: A parameterized Multiprocessing pool.
    """
    n_cores = mp.cpu_count()
    virtual_cores = len(processes_to_run)
    logger.info(f"{n_cores} cores detected, creating {virtual_cores} virtual cores")
    return mp.Pool(processes=virtual_cores)


def multiprocessing_scenarios_single_run(
    scenario_options: Sequence[MutableMapping],
    func: Callable,
    dated_output_folder: bool = False,
    iteration_run: bool = False,
    include_outputs: bool = True,
) -> None:
    """Multiprocessing function that creates a pool to run a function for multiple scenarios at once.

    Args:
        scenario_options (Sequence[MutableMapping]): Dictionary containing the scenarios you want to run.
        func (Callable): Function to run for each scenario in scenario_options.
        dated_output_folder (bool, optional): Boolean flag to determine if the create a new folder for the results. Defaults to False.
        iteration_run (bool, optional): Boolean flag to determine whether there will be multiple iterations for each scenario. Defaults to False.
        include_outputs (bool, optional): _description_. Defaults to True.
    """
    pool = create_pool(scenario_options)
    workers = [
        pool.apply_async(
            func,
            kwds=dict(
                scenario_dict=scenario_dict,
                dated_output_folder=dated_output_folder,
                iteration_run=iteration_run,
                include_outputs=include_outputs,
            ),
        )
        for scenario_dict in scenario_options
    ]
    for async_pool in workers:
        async_pool.get()
    pool.close()
    pool.join()


def multiprocessing_scenarios_preprocessing(
    scenario_options: MYPY_SCENARIO_TYPE_DICT, preprocessing_function: Callable
) -> None:
    """Multiprocessing function that creates a pool to run a function for multiple scenarios at once.

    Args:
        scenario_options (MutableMapping): Dictionary containing the scenarios you want to run.
        preprocessing_function (Callable): Function containing the logic you want to run for each scenario in scenario_options.
    """
    # POOL 1: PREPROCESSING
    pool = create_pool(scenario_options.keys())
    workers = [
        pool.apply_async(
            preprocessing_function, kwds=dict(scenario_dict=scenario_options[scenario])
        )
        for scenario in scenario_options
    ]
    for async_pool in workers:
        async_pool.get()
    pool.close()
    pool.join()

def core_run_function(
    scenario_dict: MutableMapping[str, Any], model_run: str, pkl_paths: Union[dict, None] = None
) -> None:
    """Function to run the section of the model that runs multiple times.

    Args:
        scenario_dict (MutableMapping): The scenario to run.
        model_run (str): The number of the scenario run.
        pkl_paths (Union[dict, None], optional): The path where the multiple runs will be stored. Defaults to None.
    """
    # Create folders where the multiple runs will be saved.
    generate_files_to_path_dict(
        scenarios=[
            scenario_dict["scenario_name"],
        ],
        pkl_paths=pkl_paths,
        model_run=model_run,
        create_path=True,
    )
    main_solver_flow(
        scenario_dict=scenario_dict,
        pkl_paths=pkl_paths,
        serialize=True,
        model_run=model_run,
    )
    model_results_phase(scenario_dict, pkl_paths=pkl_paths, model_run=model_run)


def async_error_handler(e):
    print("error")
    logger.info(dir(e), "\n")
    logger.info(f"-->{e.__cause__}<--")


def multi_run_function(run_range: range, scenario_dict: MutableMapping) -> None:
    """Multiprocessing function that uses a pool to run the model multiple times.

    Args:
        run_range (range): A range representing the number of times to run the model.
        scenario_dict (MutableMapping): The scenario you want to run multiple times.
    """
    pool = create_pool(run_range)
    for model_run in run_range:
        pool.apply_async(
            core_run_function,
            kwds=dict(
                scenario_dict=scenario_dict,
                model_run=str(model_run),
                pkl_paths=None,
            ),
            error_callback=async_error_handler,
        )
    pool.close()
    pool.join()
