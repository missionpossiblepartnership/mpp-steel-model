"""Runs the data loading scripts"""

from datetime import datetime
from mppsteel.config.model_state_control import ModelStateControl

from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    DATETIME_FORMAT
)

from mppsteel.config.runtime_args import parser

logger = get_logger(__name__)

if __name__ == "__main__":
    args = parser.parse_args()
    timestamp = datetime.now().strftime(DATETIME_FORMAT)
    model_state_control = ModelStateControl(args=args, timestamp=timestamp)
    model_state_control.parse_runtime_args(args)
