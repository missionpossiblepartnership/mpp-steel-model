"""Module that determines functionality for opening and closing plants"""

import math
from typing import Union
from typing import Tuple, Union

import pandas as pd
import numpy as np
from tqdm import tqdm

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import return_furnace_group
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.data_loading.reg_steel_demand_formatter import extend_steel_demand

from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    PKL_DATA_IMPORTS,
    PKL_DATA_INTERMEDIATE,
    INVESTMENT_CYCLE_DURATION_YEARS,
    INVESTMENT_OFFCYCLE_BUFFER_TOP,
    INVESTMENT_OFFCYCLE_BUFFER_TAIL
)

from mppsteel.config.model_scenarios import TECH_SWITCH_SCENARIOS, SOLVER_LOGICS

from mppsteel.config.reference_lists import (
    SWITCH_DICT,
    TECHNOLOGY_STATES,
    FURNACE_GROUP_DICT,
    TECH_MATERIAL_CHECK_DICT,
    RESOURCE_CONTAINER_REF,
    TECHNOLOGY_PHASES,
)

from mppsteel.data_loading.data_interface import load_materials, load_business_cases

from mppsteel.model.solver_constraints import (
    tech_availability_check,
    read_and_format_tech_availability,
    plant_tech_resource_checker,
    create_plant_capacities_dict,
    material_usage_per_plant,
    load_resource_usage_dict,
)

from mppsteel.model.tco_and_abatement_optimizer import get_best_choice
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger("Plant Opening and Closing")

def plant_closure_check(
    utilization_rate: float, cutoff: float, current_tech: str
) -> str:
    """Function that checks whether a plant in a given region should close.

    Args:
        utilization_rate (float): _description_
        cutoff (float): _description_
        current_tech (str): _description_

    Returns:
        str: _description_
    """
    return "Close plant" if utilization_rate < cutoff else current_tech

