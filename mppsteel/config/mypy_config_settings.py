"""Contains types for mypy"""
from typing import (
    Any,
    Dict, 
    MutableMapping, 
    Mapping, 
    List, 
    Sequence, 
    Iterable, 
    Sized,
    Union
)

MYPY_SCENARIO_TYPE = MutableMapping[str, Union[bool, str]]
MYPY_SCENARIO_TYPE_OR_NONE = Union[MYPY_SCENARIO_TYPE, None]
MYPY_PKL_PATH_OPTIONAL = Union[MutableMapping[str, str], None]
MYPY_DICT_STR_LIST = Dict[str, list]
MYPY_DICT_STR_DICT = Dict[str, dict]