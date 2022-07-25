"""Script for the CapacityConstraint class."""

from typing import Union
from mppsteel.config.model_config import (
    MAX_WAITING_LIST_YEARS,
    MODEL_YEAR_END,
    CAPACITY_CONSTRAINT_FIXED_GROWTH_RATE,
    CAPACITY_CONSTRAINT_FIXED_RATE_MTPA,
    CAPACITY_CONSTRAINT_FIXED_RATE_YEAR_RANGE
)

from collections import namedtuple
from mppsteel.plant_classes.plant_investment_cycle_class import PlantInvestmentCycle

from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)

SwitchingPlant = namedtuple(
    'SwitchingPlant', 
    ['plant_name', 'capacity', 'switch_type', 'within_constraint', 'waiting_list']
)

class PlantCapacityConstraint:
    def __init__(self) -> None:
        self.annual_capacity_turnover_limit = {}
        self.capacity_balance = {}
        self.potential_plant_switchers = {}
        self.waiting_list_counter = {}

    def instantiate_container(self, year_range: range) -> None:
        self.annual_capacity_turnover_limit = {year: 0 for year in year_range}
        self.capacity_balance = {year: 0 for year in year_range}
        self.potential_plant_switchers = {year: {} for year in year_range}

    def update_capacity_turnover_limit(self, year: int, prior_year_total_capacity: float = 0):
        if year in CAPACITY_CONSTRAINT_FIXED_RATE_YEAR_RANGE:
            self.annual_capacity_turnover_limit[year] = CAPACITY_CONSTRAINT_FIXED_RATE_MTPA
        else:
            self.annual_capacity_turnover_limit[year] = prior_year_total_capacity * (1 + CAPACITY_CONSTRAINT_FIXED_GROWTH_RATE)
            
    def update_capacity_balance(self, year: int) -> None:
        self.capacity_balance[year] = self.annual_capacity_turnover_limit[year]
        
    def update_potential_plant_switcher(self, year: int, plant_name: str, plant_capacity: float, switch_type: str) -> None:
        if plant_name not in self.potential_plant_switchers[year].keys():
            self.potential_plant_switchers[year][plant_name] = SwitchingPlant(
                plant_name=plant_name, 
                capacity=plant_capacity, 
                switch_type=switch_type, 
                within_constraint=False, 
                waiting_list=True
            )
        if plant_name not in self.waiting_list_counter:
            self.waiting_list_counter[plant_name] = 0

    def subtract_capacity_from_balance(
        self, year: int, plant_name: str, enforce_constraint: bool = True
    ) -> bool:
        plant_capacity = self.potential_plant_switchers[year][plant_name].capacity
        current_plant_capacity = self.capacity_balance[year]
        if plant_capacity > current_plant_capacity and enforce_constraint:
            return False
        else:
            self.capacity_balance[year] = current_plant_capacity - plant_capacity
            self.potential_plant_switchers[year][plant_name] = self.potential_plant_switchers[year][plant_name]._replace(within_constraint = True, waiting_list = False)
            return True

    def remove_plant_from_waiting_list(self, year: int, plant_name: str) -> None:
        if plant_name in self.potential_plant_switchers[year]:
            self.potential_plant_switchers[year][plant_name] = self.potential_plant_switchers[year][plant_name]._replace(waiting_list = False)

    def return_waiting_list(self, year: int) -> list:
        return [plant_name for plant_name in self.potential_plant_switchers[year] if self.potential_plant_switchers[year][plant_name].waiting_list]

    def return_capcity_switch_plants(self, year: int) -> list:
        return [plant_name for plant_name in self.potential_plant_switchers[year] if self.potential_plant_switchers[year][plant_name].within_constraint]

    def move_waiting_list_plants_to_next_year(self, plant_investment_cycle_container: PlantInvestmentCycle, year: int) -> None:
        if year < MODEL_YEAR_END:
            waiting_list = self.return_waiting_list(year)
            for plant_name in waiting_list:
                self.potential_plant_switchers[year + 1][plant_name] = self.potential_plant_switchers[year][plant_name]
                plant_investment_cycle_container.adjust_cycle_for_deferred_investment(plant_name, year)
                self.waiting_list_counter[plant_name] += 1

    def return_potential_switchers(self, year: int, plant_name: str = "") -> Union[dict, SwitchingPlant]:
        return self.potential_plant_switchers[year][plant_name] if plant_name else self.potential_plant_switchers[year]

    def test_capacity_constraint(self, year):
        capacity_balance = self.capacity_balance[year]
        assert capacity_balance >= 0, f"Capacity balance is less than zero: {capacity_balance: .2f}"

    def waiting_list_limit_checker(self) -> None:
        waiting_list_dict = self.waiting_list_counter
        burst_limit_dict = {key: val for key,val in waiting_list_dict.items() if val > MAX_WAITING_LIST_YEARS}
        if burst_limit_dict:
            logger.info(f"Some plants burst the waiting list limit of {MAX_WAITING_LIST_YEARS} years -> {burst_limit_dict}")
        else:
            logger.info(f"No plants burst the limit of {MAX_WAITING_LIST_YEARS} years. Max waiting list time is {max(waiting_list_dict.values())} years.")

    def print_capacity_summary(self, year: int) -> None:
        capacity_balance = self.capacity_balance[year]
        waiting_list = self.return_waiting_list(year)
        switched_plants = self.return_capcity_switch_plants(year)
        logger.info(
            f"Capacity Balance for {year} is {capacity_balance: .2f}. {len(switched_plants)} Plants switched their capacity to a different technology. There are {len(waiting_list)} plants in the waiting_list."
        )
