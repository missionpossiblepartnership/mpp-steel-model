"""Script for establishing capex switching values"""

# For Data Manipulation
import pandas as pd

# For logger
from utils import get_logger, read_pickle_folder, serialise_file

from model_config import PKL_FOLDER

# Create logger
logger = get_logger('Capex Switching')

brownfield_capex = read_pickle_folder(PKL_FOLDER, 'brownfield_capex')


print(brownfield_capex)