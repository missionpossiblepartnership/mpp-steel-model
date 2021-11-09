"""Utility library for functions used throughout the module"""
import logging
import pickle
import sys
import os

from logging.handlers import TimedRotatingFileHandler

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = "MPP_STEEL_LOGFILE.log"

def get_console_handler():
    """Formats the log for console output

    Returns:
        StreamHandler: A formatted stream handler
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler

def get_file_handler():
    """Formats the log for file output

    Returns:
        [type]: A formatted file handler
    """
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler

def get_logger(logger_name, create_logfile: bool = False):
    """Creates a log object that can be outputted to file or std output.

    Args:
        logger_name ([type]): Defines the name of the log based on the user input.
        create_logfile (bool, optional): Determines whether to create a logfile. Defaults to False.

    Returns:
        [type]: [description]
    """
    generic_logger = logging.getLogger(logger_name)
    generic_logger.setLevel(logging.DEBUG) # better to have too much log than not enough
    generic_logger.addHandler(get_console_handler())
    if create_logfile:
        generic_logger.addHandler(get_file_handler())
    generic_logger.propagate = False # rarely necessary to propagate the error up to parent
    return generic_logger

logger = get_logger('Utils')


def read_pickle_folder(data_path: str):
    """Reads a path where pickle files are stores and saves them to a dictionary

    Args:
        data_path (str): A path in the repository where pickle files are stored

    Returns:
        [dict]: A dictionary with keys based on the file names without the extension.
    """
    logger.info(f'||| Loading pickle files from path {data_path}')
    new_data_dict = {}
    for pkl_file in os.listdir(data_path):
        logger.info(f'|||| Loading {pkl_file}')
        with open(fr'{data_path}/{pkl_file}', 'rb') as f:
            new_data_dict[pkl_file.split('.')[0]] = pickle.load(f)

    return new_data_dict