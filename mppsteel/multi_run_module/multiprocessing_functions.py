"""Script containing functions that include multiprocessing"""

import math
import multiprocessing as mp
from typing import Callable, Iterable, List, Union
from mppsteel.config.model_config import MAX_TASKS_PER_MP_CHILD


from mppsteel.model_solver.solver_flow import main_solver_flow
from mppsteel.config.model_grouping import model_results_phase
from mppsteel.utility.file_handling_utility import generate_files_to_path_dict

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


def create_pool(processes_to_run: Iterable):
    n_cores = mp.cpu_count()
    virtual_cores = len(processes_to_run)
    logger.info(f"{n_cores} cores detected, creating {virtual_cores} virtual cores")
    return mp.Pool(processes=virtual_cores)


def multiprocessing_scenarios_single_run(
    scenario_options: List[dict], func: Callable, dated_output_folder: bool = False, 
    iteration_run: bool = False, include_outputs: bool = True
) -> None:
    pool = create_pool(scenario_options)
    workers = [
        pool.apply_async(
            func, 
            kwds=dict(
                scenario_dict=scenario_dict, 
                dated_output_folder=dated_output_folder, 
                iteration_run=iteration_run, 
                include_outputs=include_outputs
            )
        ) for scenario_dict in scenario_options
    ]
    for async_pool in workers:
        async_pool.get()
    pool.close()
    pool.join()


def multiprocessing_scenarios_preprocessing(
    scenario_options: dict,
    preprocessing_function: Callable
) -> None:
    # POOL 1: PREPROCESSING
    pool = create_pool(scenario_options.keys())
    workers = [
        pool.apply_async(
            preprocessing_function, 
            kwds=dict(
                scenario_dict=scenario_options[scenario]
            )
        ) for scenario in scenario_options
    ]
    for async_pool in workers:
        async_pool.get()
    pool.close()
    pool.join()


def manager_run(
    process_function: Callable, process_kwargs_function: Callable, 
    process_iterable_dict: dict, filename: str = "", 
):
    logger.info(f"Combining scenario data into one for {filename}")
    with mp.Manager() as manager:
        L = manager.list()
        processes = []
        for iter in process_iterable_dict:
            p = mp.Process(
                target=process_function, 
                kwargs=process_kwargs_function(L, filename, iter),   
            )
            p.start()
            processes.append(p)
        
        for p in processes:
            p.join()

        return list(L)

def core_run_function(scenario_dict: dict, model_run: int, pkl_paths: Union[dict, None] = None):
    generate_files_to_path_dict(
        [scenario_dict["scenario_name"],], pkl_paths, model_run, create_path=True
    )
    main_solver_flow(
        scenario_dict=scenario_dict, pkl_paths=pkl_paths, serialize=True, model_run=str(model_run)
    )
    model_results_phase(
        scenario_dict, pkl_paths=pkl_paths, model_run=str(model_run)
    )

def async_error_handler(e):
    print('error')
    logger.info(dir(e), "\n")
    logger.info(f"-->{e.__cause__}<--")


def multi_run_function(run_range: range, scenario_dict: dict):
    pool = create_pool(run_range)
    for model_run in run_range:
        pool.apply_async(
            core_run_function, 
            kwds=dict(
                scenario_dict=scenario_dict,
                model_run=model_run,
                pkl_paths=None,
            ),
            error_callback=async_error_handler
        )
    pool.close()
    pool.join()
