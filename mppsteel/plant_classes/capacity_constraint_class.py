"""Script for the CapacityConstraint class."""

from mppsteel.config.model_config import (
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
    def __init__(self):
        self.annual_capacity_turnover_limit = {}
        self.capacity_balance = {}
        self.potential_plant_switchers = {}
        self.waiting_list = {}

    def initiate_capacity_limit_and_balance(self, year_range: range):
        self.annual_capacity_turnover_limit = {year: 0 for year in year_range}
        self.capacity_balance = {year: 0 for year in year_range}
        self.potential_plant_switchers = {year: {} for year in year_range}
        
        
    def update_capacity_turnover_limit(self, year: int, prior_year_total_capacity: float = 0):
        if year in CAPACITY_CONSTRAINT_FIXED_RATE_YEAR_RANGE:
            self.annual_capacity_turnover_limit[year] = CAPACITY_CONSTRAINT_FIXED_RATE_MTPA
        else:
            self.annual_capacity_turnover_limit[year] = prior_year_total_capacity * (1 + CAPACITY_CONSTRAINT_FIXED_GROWTH_RATE)
            
    def update_capacity_balance(self, year: int):
        self.capacity_balance[year] = self.annual_capacity_turnover_limit[year]
        
    def update_potential_plant_switcher(self, year: int, plant_name: str, plant_capacity: float, switch_type: str):
        if not isinstance(self.potential_plant_switchers[year][plant_name], SwitchingPlant):
            self.potential_plant_switchers[year][plant_name] = SwitchingPlant(plant_name, plant_capacity, switch_type, False, True)
        
    def subtract_capacity_from_balance(self, plant_investment_cycle_container: PlantInvestmentCycle, year: int, plant_name: str):
        plant_capacity = self.potential_plant_switchers[year][plant_name].capacity
        current_plant_capacity = self.capacity_balance[year]
        if plant_capacity > current_plant_capacity:
            plant_investment_cycle_container.adjust_cycle_for_deferred_investment(plant_name, year)
        else:
            # else condition
            self.capacity_balance[year] = current_plant_capacity - plant_capacity
            self.potential_plant_switchers[year][plant_name].within_constraint = True
            self.potential_plant_switchers[year][plant_name].waiting_list = False
        
    def move_waiting_list_plants_to_next_year(self, year):
        for plant_name in self.potential_plant_switchers[year]:
            if self.potential_plant_switchers[year][plant_name].waiting_list == True and year < MODEL_YEAR_END:
                self.potential_plant_switchers[year + 1][plant_name] = self.potential_plant_switchers[year][plant_name]

    # Also update cases in solver
    # Need priority ranking -> 1) New plants 2) Main switchers 3) Transitional switchers.
