"""Utility Script for logger"""

import logging
import sys

from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

from mppsteel.model_config import LOG_PATH

LOG_FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def get_console_handler():
    """Formats the log for console output

    Returns:
        StreamHandler: A formatted stream handler
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(LOG_FORMATTER)
    return console_handler


def get_file_handler():
    """Formats the log for file output

    Returns:
        [type]: A formatted file handler
    """
    today_time = datetime.today().strftime('%y%m%d_%H%M%S')
    log_filepath = f"{LOG_PATH}/mppsteel_{today_time}.log"
    file_handler = TimedRotatingFileHandler(log_filepath, when="midnight")
    file_handler.setFormatter(LOG_FORMATTER)
    return file_handler


def get_logger(logger_name, create_logfile: bool = True):
    """Creates a log object that can be outputted to file or std output.

    Args:
        logger_name ([type]): Defines the name of the log based on the user input.
        create_logfile (bool, optional): Determines whether to create a logfile. Defaults to False.

    Returns:
        [type]: [description]
    """
    generic_logger = logging.getLogger(logger_name)
    generic_logger.setLevel(
        logging.DEBUG
    )  # better to have too much log than not enough
    generic_logger.addHandler(get_console_handler())
    if create_logfile:
        generic_logger.addHandler(get_file_handler())
    generic_logger.propagate = (
        False  # rarely necessary to propagate the error up to parent
    )
    return generic_logger
