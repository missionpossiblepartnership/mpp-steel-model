"""Class and functions to manage Material Usage"""

import itertools
from typing import Sequence

import pandas as pd

from mppsteel.config.model_config import (
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    SCRAP_CONSTRAINT_TOLERANCE_FACTOR,
)
from mppsteel.config.mypy_config_settings import MYPY_NUMERICAL
from mppsteel.config.reference_lists import RESOURCE_CONTAINER_REF
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


class MaterialUsage:
    """Description
    Class to manage how resource constraints are handled.

    Important Points
    1) Only resources that have constraints are tracked in this class
    2) There are several attributes that interact to manage the resource consumptions
    3) There are states that usage might exceed the constraint - Not by error. The class has functionality to manage these cases.

    Main Class Attributes
        Constraint: The amount of the reosurce available. In the form [model_type][year] -> constraint value
        Usage: The amount of the reosurce that has been used. Should be lower than the constraint. In the form [model_type][year] -> usage value
        Balance: The amount of the reosurce still available for the year. Should be lower or equal to the constraint. In the form [model_type][year] -> constraint value
        Resources: The list of resources to track.
    """

    def __init__(self):
        self.constraint = {}
        self.usage = {}
        self.balance = {}
        self.results = []
        self.resources = ["biomass", "scrap", "ccs", "co2"]

    def initiate_years_and_regions(
        self, year_range: range, resource_list: Sequence, region_list: Sequence
    ):
        for year in year_range:
            self.usage[year] = {resource: 0 for resource in resource_list}
            self.balance[year] = {resource: 0 for resource in resource_list}
            self.usage[year]["scrap"] = {region: 0 for region in region_list}
            self.balance[year]["scrap"] = {region: 0 for region in region_list}

    def load_constraint(self, model: pd.DataFrame, model_type: str):
        if model_type == "biomass":
            self.constraint[model_type] = create_biomass_constraint(model)
        elif model_type == "scrap":
            self.constraint[model_type] = create_scrap_constraints(model)
        elif model_type == "ccs":
            self.constraint[model_type] = create_ccs_constraint(model)
        elif model_type == "co2":
            self.constraint[model_type] = create_co2_use_constraint(model)

    def set_year_balance(self, year: int, model_type: str, region_list: list):
        if model_type == "scrap":
            for region in region_list:
                self.balance[year][model_type][region] = self.constraint[model_type][
                    year
                ][region]
        else:
            self.balance[year][model_type] = self.constraint[model_type][year]

    def get_current_balance(self, year: int, model_type: str, region: str = ""):
        if model_type == "scrap":
            if region:
                return self.balance[year][model_type][region]
            else:
                return sum(self.balance[year][model_type].values())
        return self.balance[year][model_type]

    def get_current_usage(self, year: int, model_type: str):
        if model_type == "scrap":
            return sum(self.usage[year][model_type].values())
        return self.usage[year][model_type]

    def record_results(self, dict_entry: dict):
        self.results.append(dict_entry)

    def print_year_summary(self, year: int, regional_scrap: bool):
        for model_type in self.resources:
            if model_type == "scrap":
                constraint: MYPY_NUMERICAL = sum(
                    self.constraint[model_type][year].values()
                )
                usage: MYPY_NUMERICAL = sum(self.usage[year][model_type].values())
                balance: MYPY_NUMERICAL = sum(self.balance[year][model_type].values())
            else:
                constraint = self.constraint[model_type][year]
                usage = self.usage[year][model_type]
                balance = self.balance[year][model_type]
            pct_used: MYPY_NUMERICAL = 100
            pct_remaining: MYPY_NUMERICAL = 0
            try:
                pct_used = (usage / constraint) * 100
                pct_remaining = (balance / constraint) * 100
            except ZeroDivisionError:
                pct_used = 100
                pct_remaining = 0
            logger.info(
                f"""{model_type.upper()} USAGE SUMMARY {year}  -> Constraint: {constraint :0.4f} | Usage: {usage :0.4f} ({pct_used :0.1f}%) | Balance: {balance :0.4f} ({pct_remaining :0.1f}%)"""
            )
            if (model_type == "scrap") and regional_scrap:
                limit_bursting_regions = {
                    region: round(self.balance[year][model_type][region], 2)
                    for region in self.balance[year][model_type]
                    if self.balance[year][model_type][region] < 0
                }
                limit_keeping_regions = {
                    region: round(self.balance[year][model_type][region], 2)
                    for region in self.balance[year][model_type]
                    if self.balance[year][model_type][region] >= 0
                }
                logger.info(
                    f"SCRAP USAGE SUMMARY {year} - Scrap Limit Bursting Regions -> {limit_bursting_regions}"
                )
                logger.info(
                    f"SCRAP USAGE SUMMARY {year} - Scrap Limit Keeping Regions -> {limit_keeping_regions}"
                )

    def output_constraints_summary(self, year_range: range):
        results = []
        for year, model_type in itertools.product(year_range, self.resources):
            if model_type == "scrap":
                constraint = sum(self.constraint[model_type][year].values())
                usage = sum(self.usage[year][model_type].values())
                balance = sum(self.balance[year][model_type].values())
            else:
                constraint = self.constraint[model_type][year]
                usage = self.usage[year][model_type]
                balance = self.balance[year][model_type]
            entry = {
                "resource": model_type,
                "year": year,
                "constraint": constraint,
                "usage": usage,
                "balance": balance,
            }
            results.append(entry)
        return (
            pd.DataFrame(results)
            .set_index(["resource", "year"])
            .sort_index(ascending=True)
        )

    def output_results_to_df(self):
        return pd.DataFrame(self.results)

    def constraint_transaction(
        self,
        year: int,
        model_type: str,
        amount: float,
        region: str = None,
        override_constraint: bool = False,
        apply_transaction: bool = True,
        regional_scrap: bool = False,
    ):
        def function_to_apply_transaction(
            self,
            current_balance: float,
            current_usage: float,
            current_balance_regional: float,
            current_usage_regional: float,
        ) -> None:
            if model_type == "scrap" and regional_scrap:
                self.balance[year][model_type][region] = current_balance - amount
                self.usage[year][model_type][region] = current_usage + amount
            elif model_type == "scrap" and not regional_scrap:
                self.balance[year][model_type][region] = (
                    current_balance_regional - amount
                )
                self.usage[year][model_type][region] = current_usage_regional + amount
            else:
                self.balance[year][model_type] = current_balance - amount
                self.usage[year][model_type] = current_usage + amount
            return None

        # set up balance and usage
        current_balance_regional = 0
        current_usage_regional = 0
        if model_type == "scrap" and regional_scrap:
            current_balance = self.balance[year][model_type][region]
            current_usage = self.usage[year][model_type][region]

        elif model_type == "scrap" and not regional_scrap:
            current_balance_regional = self.balance[year][model_type][region]
            current_usage_regional = self.usage[year][model_type][region]
            current_balance = sum(self.balance[year][model_type].values())
            current_usage = sum(self.usage[year][model_type].values())

        else:
            current_balance = self.balance[year][model_type]
            current_usage = self.usage[year][model_type]

        # CASE 1: Apply and override
        if apply_transaction and override_constraint:
            function_to_apply_transaction(
                self,
                current_balance,
                current_usage,
                current_balance_regional,
                current_usage_regional,
            )
            return True

        # CASE 2: Apply, but don't override
        elif apply_transaction and not override_constraint:
            # check that an amount is required but the constraint is insufficient
            if (amount > 0) and (current_balance < amount):
                return False
            else:
                function_to_apply_transaction(
                    self,
                    current_balance,
                    current_usage,
                    current_balance_regional,
                    current_usage_regional,
                )
                return True

        # CASE 3: Don't apply, but and override
        elif not apply_transaction and override_constraint:
            return True

        # CASE 4: Don't apply or override
        elif not apply_transaction and not override_constraint:
            if (amount > 0) and (current_balance < amount):
                return False
            return True


def create_material_usage_dict(
    material_usage_dict_container: MaterialUsage,
    plant_capacities: dict,
    business_case_ref: dict,
    plant_name: str,
    region: str,
    year: int,
    switch_technology: str,
    regional_scrap: bool,
    capacity_value: float = None,
    override_constraint: bool = False,
    apply_transaction: bool = False,
    negative_amount: bool = False,
) -> dict:
    """Creates a material checking dictionary that contains checks on every resource that has a constraint.
    The function will assign True if the resource passes the constraint check and False if the resource doesn't pass this constraint.


    Args:
        material_usage_dict_container (MaterialUsage): The MaterialUsage Instance containing the material consumption state.
        plant_capacities (dict): A dictionary of plant names and capacity values.
        business_case_ref (dict): The Business Cases of resourse usage.
        plant_name (str): The name of the plant.
        region (str): The region of the plant.
        year (int): The current model cycle year.
        switch_technology (str): The potential switch technology.
        capacity_value (float, optional): The capacity of the plant (if the value is availabile, otherwise to be found in `plant_capacities`). Defaults to None.
        override_constraint (bool, optional): Boolean flag to determine whether the current constraint should be overwritten. Defaults to False.
        apply_transaction (bool, optional): Boolean flag to determine whether the transaction against the constraint should be fulfilled. If false, the constraint will be left unmodified. Defaults to False.
        negative_amount (bool, optional): Boolean flag to determine if tthe transaction amount should be multiplied by -1. Defaults to False.
    Returns:
        dict: A dictionary that has the resource as a key and a boolean check as the value.
    """
    material_check_container = {}
    for resource in RESOURCE_CONTAINER_REF:
        projected_usage = return_projected_usage(
            plant_name,
            switch_technology,
            plant_capacities,
            business_case_ref,
            RESOURCE_CONTAINER_REF[resource],
            capacity_value=capacity_value,
        )
        if negative_amount:
            projected_usage = projected_usage * -1
        material_check_container[
            resource
        ] = material_usage_dict_container.constraint_transaction(
            year=year,
            model_type=resource,
            amount=projected_usage,
            region=region,
            override_constraint=override_constraint,
            apply_transaction=apply_transaction,
            regional_scrap=regional_scrap,
        )
    return material_check_container


def create_co2_use_constraint(model: pd.DataFrame) -> dict:
    """Creates a dictionary of years as keys and constraint amounts as values (in Mt CO2) for CO2 Use.

    Args:
        model (pd.DataFrame): The CO2 Use model.

    Returns:
        dict: A dictionary of the CO2 use constraints.
    """
    return (
        model[model["Metric"] == "Steel CO2 use market"][["Value", "Year"]]
        .set_index(["Year"])
        .to_dict()["Value"]
    )


def create_ccs_constraint(model: pd.DataFrame) -> dict:
    """Creates a dictionary of years as keys and constraint amounts as values (in Mt CO2) for CCS.

    Args:
        model (pd.DataFrame): The CCS model.

    Returns:
        dict: A dictionary of the CCS constraints.
    """
    return model.swaplevel().loc["Global"][["value"]].to_dict()["value"]


def create_biomass_constraint(model: pd.DataFrame) -> dict:
    """Creates a dictionary of years as keys and constraint amounts as values (in GJ Energy) for Biomass.

    Args:
        model (pd.DataFrame): The Biomass model.

    Returns:
        dict: A dictionary of the Biomass constraints.
    """
    return model[["value"]].to_dict()["value"]


def create_scrap_constraints(model: pd.DataFrame) -> dict:
    """Creates a multilevel dictionary of years as keys and region[values] amounts as values (in Mt Scrap) for Scrap.

    Args:
        model (pd.DataFrame): The Scrap model.

    Returns:
        dict: A multilevel dictionary of the Scrap constraints.
    """
    rsd = model[model["region"] != "World"].copy()
    rsd = rsd[rsd.index.get_level_values("metric") == "Scrap availability"]
    rsd = rsd[["region", "value"]].reset_index().set_index(["year", "region"]).copy()
    rsd["value"] = rsd["value"] * (1 + SCRAP_CONSTRAINT_TOLERANCE_FACTOR)
    return {
        year: rsd.loc[year].to_dict()["value"] for year in rsd.index.get_level_values(0)
    }


def return_projected_usage(
    plant_name: str,
    technology: str,
    capacities_dict: dict,
    business_case_ref: dict,
    materials: list,
    capacity_value: float = None,
) -> float:
    """Returns the project usage for a specific plant given its capacity and `technology` for resources specified in `materials`.

    Args:
        plant_name (str): The name of the plant.
        technology (str): Tech plant's (potential or actual) technology
        capacities_dict (dict): Dictionary containing the capacities of each plant.
        business_case_ref (dict): The business case reference dictionary.
        materials (list): The materials to summ usage for.
        capacity_value (float, optional): A specific capacity value of the plant, if not already contained in capacities_dict. Defaults to None.

    Returns:
        float: The sum of the usage across the materials specified in `materials`
    """
    # Mt or Gj
    capacity_value_final = (
        capacity_value if capacity_value else capacities_dict[plant_name]
    )
    return sum(
        [
            business_case_ref[(technology, material)]
            * (
                capacity_value_final
                * CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION
            )
            for material in materials
        ]
    )


def return_current_usage(
    plant_list: list,
    technology_choices: dict,
    capacities_dict: dict,
    business_case_ref: dict,
    materials: list,
) -> float:
    """Returns the project usage for a a list of plants in `plant_list` given their `technology_choices` and capacities for resources specified in `materials`.

    Args:
        plant_list (list): The names of the plants.
        technology_choices (dict): The plants' technology choices.
        capacities_dict (dict): The plants' capacities.
        business_case_ref (dict): The business case reference dictionary.
        materials (list): The materials to summ usage for.

    Returns:
        float: The sum of the usage across the plants and materials specified in `materials`
    """
    usage_sum = []
    for material, plant_name in list(itertools.product(materials, plant_list)):
        capacity = capacities_dict[plant_name]
        consumption_rate = business_case_ref[(technology_choices[plant_name], material)]
        utilization = CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION
        usage_sum.append(consumption_rate * (capacity * utilization))
    return sum(usage_sum)
