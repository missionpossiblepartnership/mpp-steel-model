"""Classes to manage Capacity & Utilization"""

from copy import deepcopy
import itertools
from typing import Tuple
import pandas as pd
import numpy as np

from mppsteel.config.model_config import (
    IMPORT_DATA_PATH,
    MEGATON_TO_KILOTON_FACTOR,
    PKL_DATA_IMPORTS,
    PKL_DATA_FORMATTED,
    MAIN_REGIONAL_SCHEMA,
    PROJECT_PATH,
    TRADE_ROUNDING_NUMBER,
)
from mppsteel.config.reference_lists import RESOURCE_CONTAINER_REF
from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.utility.file_handling_utility import extract_data, read_pickle_folder
from mppsteel.model_solver.solver_constraints import (
    create_biomass_constraint,
    create_ccs_constraint,
    create_co2_use_constraint,
    create_scrap_constraints,
    return_projected_usage,
    tech_availability_check,
)
from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


class PlantChoices:
    """Description
    Class to manage the state of each plant's technology choices.

    Main Class attributes
        choices: Keeps track of each plants choice in every year. A dictionary in the form [year][plant_name] -> technology
        records: A list of DataFrames that record why certain technologies were chosen or not chosen. The list can be outputted to a combined DataFrame.
        active_check: A dictionary that keeps track of whether a plant is active or not. A dictionary in the form [year][plant_name] -> boolean check
    """

    def __init__(self):
        self.choices = {}
        self.choice_records = []
        self.rank_records = []
        self.active_check = {}

    def initiate_container(self, year_range: range):
        for year in year_range:
            self.choices[year] = {}
            self.active_check[year] = {}

    def update_choice(self, year: int, plant: str, tech: str):
        self.choices[year][plant] = tech
        if tech != "Close plant":
            self.active_check[year][plant] = True
        if tech == "Close plant":
            self.active_check[year][plant] = False

    def remove_choice(self, year: int, plant: str):
        del self[year][plant]

    def get_choice(self, year: int, plant: str):
        return self.choices[year][plant]

    def return_choices(self, year: int = None):
        return self.choices[year] if year else self.choices

    def return_nans(self, year: int):
        return [
            plant for plant in self.choices[year] if pd.isna(self.choices[year][plant])
        ]

    def update_records(self, record_type: str, df_entry: pd.DataFrame):
        if record_type == "choice":
            self.choice_records.append(df_entry)
        elif record_type == "rank":
            self.rank_records.append(df_entry)

    def output_records_to_df(self, record_type: str):
        if record_type == "choice":
            df = pd.DataFrame(self.choice_records).reset_index(drop=True)
            return df.drop_duplicates(keep='last')
        elif record_type == "rank":
            df = pd.DataFrame(self.rank_records).reset_index(drop=True)
            df = df[~df.index.duplicated(keep="first")]
            return combine_tech_ranks(df)

def combine_tech_ranks(tr_df: pd.DataFrame):
    container = [pd.concat(tr_df.values[count]) for count in range(len(tr_df.values))]
    if len(container) == 0:
        return pd.DataFrame(columns=["year", "start_tech"])
    df = pd.concat(container)
    return df.sort_values(by=['year','start_tech'], ascending=True).reset_index()

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
        self, year_range: range, resource_list: list, region_list: list
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
                constraint = sum(self.constraint[model_type][year].values())
                usage = sum(self.usage[year][model_type].values())
                balance = sum(self.balance[year][model_type].values())
            else:
                constraint = self.constraint[model_type][year]
                usage = self.usage[year][model_type]
                balance = self.balance[year][model_type]
            pct_used = 100
            pct_remaining = 0
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
        regional_scrap: bool = False
    ):

        def function_to_apply_transaction(
            self,
            current_balance: float,
            current_usage: float,
            current_balance_regional: float,
            current_usage_regional: float
        ) -> None:
            if model_type == "scrap" and regional_scrap:
                self.balance[year][model_type][region] = current_balance - amount
                self.usage[year][model_type][region] = current_usage + amount
            elif model_type == "scrap" and not regional_scrap:
                self.balance[year][model_type][region] = current_balance_regional - amount
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
                self, current_balance, current_usage, current_balance_regional, current_usage_regional)
            return True

        # CASE 2: Apply, but don't override
        elif apply_transaction and not override_constraint:
            # check that an amount is required but the constraint is insufficient
            if (amount > 0) and (current_balance < amount):
                return False
            else:
                function_to_apply_transaction(
                    self, current_balance, current_usage, current_balance_regional, current_usage_regional)
                return True

        # CASE 3: Don't apply, but and override
        elif not apply_transaction and override_constraint:
            return True

        # CASE 4: Don't apply or override
        elif not apply_transaction and not override_constraint:
            if (amount > 0) and (current_balance < amount):
                return False
            return True

class CapacityContainerClass:
    """Description
    Class for maintaining the state of each plant's capacity.

    Main Class Attributes
        At the plant level: `plant_capacities` in the form [year][plant_name] -> total capacity value
        At the region level:
            `regional_capacities_agg` in the form [year][region] -> total regional capacity value
            `regional_capacities_avg` in the form [year][region] -> average regional capacity value
    """

    def __init__(self):
        self.plant_capacities = {}
        self.regional_capacities_agg = {}

    def instantiate_container(self, year_range: range):
        self.plant_capacities = {year: 0 for year in year_range}
        self.regional_capacities_agg = {year: 0 for year in year_range}
        self.regional_capacities_avg = {year: 0 for year in year_range}

    def map_capacities(self, plant_df: pd.DataFrame, year: int):
        # Map capacities of plants that are still active for aggregates, else use averages
        plant_capacity_dict, regional_capacity_dict = create_annual_capacity_dict(
            plant_df, as_mt=True
        )
        self.plant_capacities[year] = plant_capacity_dict
        self.regional_capacities_agg[year] = regional_capacity_dict

    def set_average_plant_capacity(self, original_plant_df: pd.DataFrame):
        self.average_plant_capacity = create_average_plant_capacity(
            original_plant_df, as_mt=True
        )

    def return_regional_capacity(self, year: int = None, region: str = None):
        capacity_dict = self.regional_capacities_agg
        if region and not year:
            # return a year value time series for a region
            return {
                year_val: capacity_dict[year_val][region] for year_val in capacity_dict
            }

        if year and not region:
            # return all regions for single year
            return capacity_dict[year]

        if year and region:
            # return single value
            return capacity_dict[year][region]

        # return all years and regions
        return capacity_dict

    def update_region(self, year: int, region: str, value: float):
        self.utilization_container[year][region] = value

    def return_avg_capacity_value(self):
        return self.average_plant_capacity

    def get_world_capacity_sum(self, year: int):
        return sum(list(self.regional_capacities_agg[year].values()))

    def return_plant_capacity(self, year: int = None, plant: str = None):
        if plant and not year:
            # return a year valye time series for a region
            return {
                year_val: (
                    self.plant_capacities[year_val][plant]
                    if plant in self.plant_capacities[year_val]
                    else 0
                )
                for year_val in self.plant_capacities
            }

        if year and not plant:
            # return all plants for single year
            return self.plant_capacities[year]

        if year and plant:
            # return single value
            return (
                self.plant_capacities[year][plant]
                if plant in self.plant_capacities[year]
                else 0
            )

        # return all years and regions
        return self.plant_capacities


class UtilizationContainerClass:
    """Description
    Class for managing each region's utilization rates.

    Main Class Attirbutes
        It maintains a dictionary structured as [year][region] -> value in the attribute called `utilization_container`
        World Utilization rates are treated as a weighted average of all the region's utilizatoin rates
    """

    def __init__(self):
        self.utilization_container = {}

    def initiate_container(self, year_range: range, region_list: list):
        for year in year_range:
            self.utilization_container[year] = {region: 0 for region in region_list}

    def assign_year_utilization(self, year: int, entry: dict):
        self.utilization_container[year] = entry

    def update_region(self, year: int, region: str, value: float):
        self.utilization_container[year][region] = value

    def get_average_utilization(self, year: int):
        return np.mean(self.utilization_container[year].values())

    def calculate_world_utilization(
        self, year: int, capacity_dict: dict, demand_value: float
    ):
        self.utilization_container[year]["World"] = demand_value / sum(
            capacity_dict.values()
        )

    def get_utilization_values(self, year: int = None, region: str = None):
        if region and not year:
            # return a year valye time series for a region
            return {
                year_val: self.utilization_container[year_val][region]
                for year_val in self.regional_capacities
            }

        if year and not region:
            # return all regions for single year
            return self.utilization_container[year]

        if year and region:
            # return single value
            return self.utilization_container[year][region]

        # return all years and regions
        return self.utilization_container

class MarketContainerClass:
    """Description
    Class for managing all aspects of the trade functionality.

    Important Notes
    1) All excess production above the regions demand is registered as positive number.
    2) All production deficits below the regional demand is registered as a negative number.

    Main Class Attributes
        It maintains a trade container as an attirbute called `trade_container`
        It also maintains more detail on the years transactions as a dictionary of DataFrames called `market_results`
    """

    def __init__(self):
        self.trade_container = {}
        self.market_results = {}
        self.account_dictionary = {
            "all": [ "regional_demand_minus_imports", "exports", "imports"],
            "trade": ["exports", "imports"],
            "consumption": ["regional_demand_minus_imports", "imports"],
            "production": ["regional_demand_minus_imports", "exports"]
        }

    def __repr__(self):
        return "Trade Container"

    def __str__(self):
        return "Trade Container Class"

    def initiate_years(self, year_range: range):
        self.trade_container = {year: {} for year in year_range}
        self.market_results = {year: {} for year in year_range}
        self.regional_competitiveness = {year: {} for year in year_range}

    def initiate_regions(self, region_list: list):
        production_account = {
            key: 0 for key in self.account_dictionary["all"]
        }
        for year in self.trade_container:
            self.trade_container[year] = {region: deepcopy(production_account) for region in region_list}

    def return_container(self):
        return self.trade_container

    def full_instantiation(self, year_range: range, region_list: list):
        self.initiate_years(year_range)
        self.initiate_regions(region_list)

    def return_market_entry(self, regional_demand_minus_imports: float, import_value: float, export_value: float) -> dict:
        return {
            "regional_demand_minus_imports": regional_demand_minus_imports,
            "import_value": import_value,
            "export_value": export_value
        }

    def assign_market_tuple(
        self,
        year: int,
        region: str,
        market_entry: dict
    ) -> None:
        self.assign_trade_balance(year, region, "regional_demand_minus_imports", market_entry["regional_demand_minus_imports"])
        self.assign_trade_balance(year, region, "imports", market_entry["import_value"])
        self.assign_trade_balance(year, region, "exports", market_entry["export_value"])
        return None

    def trade_container_getter(self, year: int, region: str = None, account_type: str = None):
        if account_type:
            return self.trade_container[year][region][account_type]
        if region:
            return self.trade_container[year][region]
        return self.trade_container[year]

    def return_trade_balance(self, year: int, region: str, account_type: str) -> float:
        imports = self.trade_container[year][region]["imports"]
        exports = self.trade_container[year][region]["exports"]
        regional_demand_minus_imports = self.trade_container[year][region]["regional_demand_minus_imports"]
        if account_type == "trade":
            return exports - imports
        elif account_type == "all":
            return regional_demand_minus_imports + exports + imports
        elif account_type == "consumption":
            return regional_demand_minus_imports + imports
        elif account_type == "production":
            return regional_demand_minus_imports + exports

    def trade_container_aggregator(self, year: int, agg_type: bool, region: str = None) -> float:
        if region:
            return self.return_trade_balance(year, region, agg_type)
        container = [self.return_trade_balance(year, region, agg_type) for region in self.trade_container[year].keys()]
        return sum(container)

    def list_regional_types(self, year: int, account_type: bool) -> list:
        return [region for region in self.trade_container[year] if self.trade_container[year][region][account_type] > 0]

    def check_if_trade_balance(self, year: int, rounding_factor: int = 3) -> list:
        balance_list = []
        for region in self.trade_container[year]:
            imports = self.trade_container[year][region]["imports"]
            exports = self.trade_container[year][region]["exports"]
            if round(exports, rounding_factor) - round(imports, rounding_factor) == 0:
                balance_list.append(region)
        return balance_list
    
    def return_current_account_balance(self, year: int, region: str, account_type: str):
        return self.trade_container[year][region][account_type]

    def assign_trade_balance(self, year: int, region: str, account_type: str, value: float) -> None:
        current_value = self.return_current_account_balance(year, region, account_type)
        self.trade_container[year][region][account_type] = current_value + value
        return None

    def store_results(self, year: int, results_df: pd.DataFrame, store_type: str):
        if store_type == "market_results":
            self.market_results[year] = results_df
        elif store_type == "competitiveness":
            self.regional_competitiveness[year] = results_df

    def return_results(self, year: int, store_type: str):
        if store_type == "market_results":
            return self.market_results[year]
        elif store_type == "competitiveness":
            return self.regional_competitiveness[year]

    def create_trade_balance_summary(self, demand_df: pd.DataFrame):
        def map_demand(row, demand_df: pd.DataFrame):
            return steel_demand_getter(demand_df, row.year, "crude", region=row.region)
        market_dict = self.trade_container
        df = pd.DataFrame.from_dict(
            {(i,j): market_dict[i][j] for i in market_dict.keys() for j in market_dict[i].keys()},
            orient='index'
        ).rename_axis(index=['year', 'region'])
        df.reset_index(inplace=True)
        df["demand"] = df.apply(map_demand, demand_df=demand_df, axis=1)
        df["trade_balance"] = df["exports"] - df["imports"]
        df["result_validity_check"] = round(df["demand"] - df["regional_demand_minus_imports"] - df["exports"] + df["exports"] - df["imports"], TRADE_ROUNDING_NUMBER) == 0
        column_order = ["year", "region", "demand", "imports", "exports", "regional_demand_minus_imports", "trade_balance"]
        return df[column_order]

    def output_trade_calculations_to_df(self, store_type: str, demand_df: pd.DataFrame = pd.DataFrame()):
        market_result_list = [df for df in self.market_results.values() if isinstance(df, pd.DataFrame)]
        competitiveness_list = [df for df in self.regional_competitiveness.values() if isinstance(df, pd.DataFrame)]
        if not demand_df.empty:
            trade_account_df = self.create_trade_balance_summary(demand_df)
        if store_type == "market_results":
            return pd.concat(market_result_list, axis=1)
        if store_type == "competitiveness":
            return pd.concat(competitiveness_list, axis=1)
        if store_type == "trade_account":
            return trade_account_df
        if store_type == "merge_trade_summary" and not demand_df.empty:
            competitiveness_df = pd.concat(competitiveness_list, axis=0)
            return merge_competitiveness_with_trade_account(competitiveness_df, trade_account_df)


def merge_competitiveness_with_trade_account(competitiveness_df: pd.DataFrame, trade_account_df: pd.DataFrame) -> pd.DataFrame:
    competitiveness_df.rename({"rmi_region": "region"}, axis=1, inplace=True)
    competitiveness_df.set_index(["year", "region"], inplace=True)
    trade_account_df.set_index(["year", "region"], inplace=True)
    return competitiveness_df.join(trade_account_df).reset_index()

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
    negative_amount: bool = False
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
            regional_scrap=regional_scrap
        )
    return material_check_container


def apply_constraints(
    business_case_ref: dict,
    plant_capacities: dict,
    material_usage_dict_container: MaterialUsage,
    combined_available_list: list,
    year: int,
    plant_name: str,
    region: str,
    base_tech: str,
    regional_scrap: bool,
    override_constraint: bool,
    apply_transaction: bool
):
    # Constraints checks
    new_availability_list = []
    for switch_technology in combined_available_list:
        material_check_container = create_material_usage_dict(
            material_usage_dict_container,
            plant_capacities,
            business_case_ref,
            plant_name,
            region,
            year,
            switch_technology,
            regional_scrap,
            override_constraint=override_constraint,
            apply_transaction=apply_transaction
        )
        if all(material_check_container.values()):
            new_availability_list.append(switch_technology)
        failure_resources = [
            resource
            for resource in material_check_container
            if not material_check_container[resource]
        ]

        result = "PASS" if all(material_check_container.values()) else "FAIL"

        entry = {
            "plant": plant_name,
            "region": region,
            "start_technology": base_tech,
            "switch_technology": switch_technology,
            "year": year,
            "assign_case": "pre-existing plant",
            "result": result,
            "failure_resources": failure_resources,
            "pass_boolean_check": material_check_container,
        }
        material_usage_dict_container.record_results(entry)
    return new_availability_list


def apply_constraints_for_min_cost_tech(
    business_case_ref: dict,
    plant_capacities_dict: dict,
    tech_availability: pd.DataFrame,
    material_usage_dict_container: MaterialUsage,
    combined_available_list: list,
    plant_capacity: float,
    tech_moratorium: bool,
    regional_scrap: bool,
    year: int,
    plant_name: str,
    region: str,
) -> list:
    """Subsets a list of technologies according to whether they pass certain availability checks for TRL>8 and resource availability.

    Args:
        business_case_ref (dict): The Business Cases of resourse usage.
        plant_capacities_dict (dict): A dictionary of plant names and capacity values.
        tech_availability (pd.DataFrame): A DataFrame of technology availability.
        material_usage_dict_container (MaterialUsage): The MaterialUsage Instance containing the material consumption state.
        combined_available_list (list): The initial technology availabilty list.
        plant_capacity (float): A capacity of the plant
        tech_moratorium (bool): Scenario boolean flag that determines if there is a tehcnology moratorium.
        year (int): The current model cycle year
        plant_name (str): The name of the plant.
        region (str): The region of the plant.

    Returns:
        list: The subsetted list.
    """
    new_availability_list = []
    # Constraints checks
    combined_available_list = [
        tech
        for tech in combined_available_list
        if tech_availability_check(
            tech_availability, tech, year, tech_moratorium=tech_moratorium
        )
    ]
    for technology in combined_available_list:
        material_check_container = create_material_usage_dict(
            material_usage_dict_container,
            plant_capacities_dict,
            business_case_ref,
            plant_name,
            region,
            year,
            technology,
            regional_scrap,
            plant_capacity,
            override_constraint=False,
            apply_transaction=False
        )

        if all(material_check_container.values()):
            new_availability_list.append(technology)

        failure_resources = [
            resource
            for resource in material_check_container
            if material_check_container[resource]
        ]

        result = "PASS" if all(material_check_container.values()) else "FAIL"

        entry = {
            "plant": plant_name,
            "region": region,
            "start_technology": "none",
            "switch_technology": technology,
            "year": year,
            "assign_case": "new plant",
            "result": result,
            "failure_resources": failure_resources,
            "pass_boolean_check": material_check_container,
        }

        material_usage_dict_container.record_results(entry)
    return new_availability_list


def create_regional_capacity_dict(
    plant_df: pd.DataFrame, rounding: int = 3, as_avg: bool = False, as_mt: bool = False
) -> dict:
    """Creates a regional capacity dictionary.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        rounding (int, optional): The rounding factor for the capacity values. Defaults to 3.
        as_avg (bool, optional): Optionally returns the average capacity value instead of the aggregate value. Defaults to False.
        as_mt (bool, optional): Boolean flag that optionall converts the capacity value from Kilotons to Megatons. Defaults to False.

    Returns:
        dict: A dictionary containing regions as keys and capacity values as values.
    """
    logger.info("Deriving average plant capacity statistics")
    plant_df_c = plant_df.set_index(["active_check"]).loc[True].copy()
    df = plant_df_c[[MAIN_REGIONAL_SCHEMA, "plant_capacity"]].groupby(
        [MAIN_REGIONAL_SCHEMA]
    )
    if as_avg:
        df = df.mean().round(rounding).reset_index()
    else:
        df = df.sum().round(rounding).reset_index()
    dict_obj = dict(zip(df[MAIN_REGIONAL_SCHEMA], df["plant_capacity"]))
    if as_mt:
        return {
            region: value / MEGATON_TO_KILOTON_FACTOR
            for region, value in dict_obj.items()
        }
    return dict_obj


def create_average_plant_capacity(
    plant_df: pd.DataFrame, rounding: int = 3, as_mt: bool = False
) -> float:
    """Generates an average plant capacity value across all plants and regions to use as a reference.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        rounding (int, optional): Rounding factor to use for the final values. Defaults to 3.
        as_mt (bool, optional): Boolean flag to determine whether to convert to units from Kilotons to Megatons. Defaults to False.

    Returns:
        float: The average plant capacity
    """
    plant_df_c = plant_df.set_index(["active_check"]).loc[True].copy()
    capacity_sum = plant_df_c["plant_capacity"].sum()
    if as_mt:
        capacity_sum = capacity_sum / MEGATON_TO_KILOTON_FACTOR
    return round(capacity_sum / len(plant_df_c), rounding)


def create_annual_capacity_dict(
    plant_df: pd.DataFrame, as_mt: bool = False
) -> Tuple[dict, dict]:
    """Creates two dictionaries. One with the plants as keys and capacities as values. The second with regions as keys and capacities as values.

    Args:
        plant_df (pd.DataFrame): The steel plant DataFrame.
        as_mt (bool, optional): Boolean flag to determine whether to convert to units from Kilotons to Megatons. Defaults to False.

    Returns:
        Tuple[dict, dict]: A tuple of the two capacity dictionary references.
    """
    regions = plant_df[MAIN_REGIONAL_SCHEMA].unique()
    plant_capacity_dict = dict(zip(plant_df["plant_name"], plant_df["plant_capacity"]))
    regional_capacity_dict_list = {
        region: [
            plant_capacity_dict[plant_name]
            for plant_name in set(plant_capacity_dict.keys()).intersection(
                set(
                    plant_df[plant_df[MAIN_REGIONAL_SCHEMA] == region][
                        "plant_name"
                    ].unique()
                )
            )
        ]
        for region in regions
    }
    regional_capacity_dict = {
        region: sum(value_list)
        for region, value_list in regional_capacity_dict_list.items()
    }
    if as_mt:
        plant_capacity_dict = {
            plant_name: value / MEGATON_TO_KILOTON_FACTOR
            for plant_name, value in plant_capacity_dict.items()
        }
        regional_capacity_dict = {
            region: value / MEGATON_TO_KILOTON_FACTOR
            for region, value in regional_capacity_dict.items()
        }
    return plant_capacity_dict, regional_capacity_dict


def format_wsa_production_data(df: pd.DataFrame, as_dict: bool = False) -> pd.DataFrame:
    """Formats the inital WSA DataFrame in preparation to extract utilization figures.

    Args:
        df (pd.DataFrame): The initial WSA Data.
        as_dict (bool, optional): Boolean flag that determines whether the DataFrame should be returned as a dictionary. Defaults to False.

    Returns:
        pd.DataFrame: Formatted WSA Data.
    """
    logger.info("Formatting WSA production data for 2020")
    df_c = df.copy()
    df_c.columns = [col.lower() for col in df_c.columns]
    if as_dict:
        return dict(zip(df_c["region"], df_c["value"]))
    return df_c


def return_utilization(
    prod_dict: dict, cap_dict: dict, utilization_cap: float = None
) -> dict:
    """Creates a utilization dictionary based on production reference dictionary and capacity reference dictionary.
    Takes the minimum of the calculated capacity and the utilization `value cap`.

    Args:
        prod_dict (dict): The dictionary containing the actual production figures.
        cap_dict (dict): The dictionary containing the capacity values.
        value_cap (float, optional): The maximum utilization value that a plant is allowed to take. Defaults to None.

    Returns:
        dict: A dictionary with regions as keys and utilization numbers as values.
    """
    util_dict = {}
    for region in prod_dict:
        val = round(prod_dict[region] / cap_dict[region], 2)
        if utilization_cap:
            val = min(val, utilization_cap)
        util_dict[region] = val
    return util_dict


def create_wsa_2020_utilization_dict(project_dir=PROJECT_PATH, from_csv: bool = False, utilization_cap: int = 1) -> dict:
    """Creates the initial utilization dictionary for 2020 based on data from the World Steel Association (WSA).

    Returns:
        dict: A dictionary with regions as keys and utilization numbers as values.
    """
    logger.info("Creating the utilization dictionary for 2020.")
    if from_csv:
        wsa_production = extract_data(IMPORT_DATA_PATH, "WSA World Steel in Figures 2021", "xlsx", 1)
    else:
        wsa_production = read_pickle_folder(project_dir / PKL_DATA_IMPORTS, "wsa_production", "df")
    steel_plants_processed = read_pickle_folder(
        project_dir / PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
    wsa_2020_production_dict = format_wsa_production_data(wsa_production, as_dict=True)
    capacity_dict = create_regional_capacity_dict(steel_plants_processed, as_mt=True)
    return return_utilization(wsa_2020_production_dict, capacity_dict, utilization_cap=utilization_cap)
