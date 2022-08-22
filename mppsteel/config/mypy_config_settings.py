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
    Tuple,
    Union
)

MYPY_SCENARIO_ENTRY_TYPE = Union[bool, str, float]
MYPY_SCENARIO_TYPE = MutableMapping[str, MYPY_SCENARIO_ENTRY_TYPE]
MYPY_SCENARIO_TYPE_DICT = Dict[str, MYPY_SCENARIO_TYPE]
MYPY_SCENARIO_TYPE_OR_NONE = Union[MYPY_SCENARIO_TYPE, None]
MYPY_PKL_PATH_OPTIONAL = Union[MutableMapping[str, str], None]
MYPY_DICT_STR_LIST = Dict[str, list]
MYPY_DICT_STR_DICT = Dict[str, dict]
MYPY_NUMERICAL = Union[int, float]
MYPY_NUMERICAL_SEQUENCE = Sequence[MYPY_NUMERICAL]
MYPY_NUMERICAL_AND_RANGE = List[Union[int, float, range]]
MYPY_SCENARIO_SETTINGS_SEQUENCE = Dict[str, Sequence]
MYPY_SCENARIO_SETTINGS_TUPLE = Dict[str, Tuple[Union[float, int], Union[float, int]]]
MYPY_SCENARIO_SETTINGS_DICT = Dict[str, Dict[str, Union[int, float]]]
MYPY_STR_DICT = Dict[str, str]
MYPY_DOUBLE_STR_DICT = MutableMapping[str, MYPY_STR_DICT]
