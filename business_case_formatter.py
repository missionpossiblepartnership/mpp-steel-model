"""Minimodel for calculating Hydrogen prices"""
# For system level operations
from collections import namedtuple

# For Data Manipulation
import pandas as pd
import numpy as np

# For logger and units dict
from utils import get_logger, read_pickle_folder, serialize_df

from model_config import PKL_FOLDER

# Create logger
logger = get_logger('Business Case Formatter')

business_cases = read_pickle_folder(PKL_FOLDER, 'business_cases')

business_cases = business_cases.melt(id_vars=['Section','Process','Process Detail','Step','Unit', 'Material Category'], var_name='Technology')
print(business_cases.head())