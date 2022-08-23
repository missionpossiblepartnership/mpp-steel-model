"""Script for the PlantInvestmentCycle class."""

from typing import Dict, List
import pandas as pd

from mppsteel.config.model_config import (
    MODEL_YEAR_END,
    INVESTMENT_CYCLE_DURATION_YEARS,
)
from mppsteel.config.mypy_config_settings import MYPY_NUMERICAL_AND_RANGE

from mppsteel.utility.log_utility import get_logger
from mppsteel.plant_classes.plant_investment_cycle_helpers import (
    add_off_cycle_investment_years,
    adjust_cycles_for_first_year,
    adjust_transitional_switch_in_investment_cycle,
    calculate_investment_years,
    create_investment_cycle_reference,
    extract_tech_plant_switchers,
    increment_investment_cycle_year,
    return_cycle_length,
    return_switch_type,
)


logger = get_logger(__name__)


class PlantInvestmentCycle:
    """Class for managing the the investment cycles for plants."""

    def __init__(self) -> None:
        self.plant_names: List[str] = []
        self.plant_start_years: Dict[str, int] = {}
        self.plant_investment_cycle_length: Dict[str, int] = {}
        self.plant_cycles: Dict[str, MYPY_NUMERICAL_AND_RANGE] = {}
        self.plant_cycles_with_off_cycle: Dict[str, MYPY_NUMERICAL_AND_RANGE] = {}

    def instantiate_plants(
        self,
        plant_names: list,
        plant_start_years: list,
        investment_cycle_randomness: bool,
    ) -> None:
        self.plant_names = plant_names
        start_year_dict = dict(zip(plant_names, plant_start_years))
        for plant_name in self.plant_names:
            self.plant_start_years[plant_name] = start_year_dict[plant_name]
            self.plant_investment_cycle_length[plant_name] = return_cycle_length(
                INVESTMENT_CYCLE_DURATION_YEARS, investment_cycle_randomness
            )
            self.plant_cycles[plant_name] = calculate_investment_years(
                self.plant_start_years[plant_name],
                self.plant_investment_cycle_length[plant_name],
            )
            self.plant_cycles_with_off_cycle[
                plant_name
            ] = add_off_cycle_investment_years(self.plant_cycles[plant_name])
        self.plant_cycles_with_off_cycle = adjust_cycles_for_first_year(
            self.plant_cycles_with_off_cycle
        )

    def add_new_plants(
        self,
        plant_names: list,
        plant_start_years: list,
        investment_cycle_randomness: bool,
    ) -> None:
        new_dict = dict(zip(plant_names, plant_start_years))
        for plant_name in plant_names:
            self.plant_names.append(plant_name)
            self.plant_start_years[plant_name] = new_dict[plant_name]
            self.plant_investment_cycle_length[plant_name] = return_cycle_length(
                INVESTMENT_CYCLE_DURATION_YEARS, investment_cycle_randomness
            )
            self.plant_cycles[plant_name] = calculate_investment_years(
                self.plant_start_years[plant_name],
                self.plant_investment_cycle_length[plant_name],
            )
            self.plant_cycles_with_off_cycle[
                plant_name
            ] = add_off_cycle_investment_years(self.plant_cycles[plant_name])

    def adjust_cycle_for_transitional_switch(
        self, plant_name: str, rebase_year: int
    ) -> None:
        new_cycle = adjust_transitional_switch_in_investment_cycle(
            self.plant_cycles_with_off_cycle[plant_name], rebase_year
        )
        self.plant_cycles_with_off_cycle[plant_name] = new_cycle

    def adjust_cycle_for_deferred_investment(
        self, plant_name: str, rebase_year: int
    ) -> None:
        new_cycle = increment_investment_cycle_year(
            self.plant_cycles_with_off_cycle[plant_name], rebase_year
        )
        self.plant_cycles_with_off_cycle[plant_name] = new_cycle

    def create_investment_df(self) -> pd.DataFrame:
        return create_investment_cycle_reference(self.plant_cycles_with_off_cycle)

    def return_plant_switch_type(self, plant_name: str, year: int) -> str:
        return return_switch_type(self.plant_cycles_with_off_cycle[plant_name], year)

    def return_investment_dict(self) -> dict:
        return self.plant_cycles_with_off_cycle

    def return_cycle_lengths(self, plant_name: str = ""):
        return (
            self.plant_investment_cycle_length[plant_name]
            if plant_name
            else self.plant_investment_cycle_length
        )

    def test_cycle_lengths(self) -> None:
        for plant_name in self.plant_cycles_with_off_cycle:
            plant_cycle: list = self.plant_cycles_with_off_cycle[plant_name]
            cycle_length: int = self.plant_investment_cycle_length[plant_name]
            range_list: List[range] = [range_object for range_object in plant_cycle if isinstance(range_object, range)]
            if (len(plant_cycle) == 1) and (len(range_list) == 1): # range object
                start_year: int = range_list[0][0]
                assert (
                    start_year + cycle_length > MODEL_YEAR_END
                ), f"Only one entry for {plant_name}. Initial year: {start_year} | Cycle length {cycle_length} | Next investment cycle {start_year + cycle_length}"

    def return_plant_switchers(
        self, active_plants: list, year: int, value_type: str
    ) -> list:
        (
            main_cycle_switchers,
            trans_cycle_switchers,
            non_switchers,
            combined_switchers,
        ) = extract_tech_plant_switchers(
            self.plant_cycles_with_off_cycle, active_plants, year
        )
        if value_type == "main cycle":
            return main_cycle_switchers
        elif value_type == "trans switch":
            return trans_cycle_switchers
        elif value_type == "no switch":
            return non_switchers
        return combined_switchers # value_type == "combined"
