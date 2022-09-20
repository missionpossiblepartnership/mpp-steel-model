"""Classes to manage market trade"""

from copy import deepcopy
import pandas as pd

from mppsteel.config.model_config import (
    TRADE_ROUNDING_NUMBER,
)

from mppsteel.data_load_and_format.reg_steel_demand_formatter import steel_demand_getter

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


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
        self.regional_competitiveness = {}
        self.account_dictionary = {
            "all": ["regional_demand_minus_imports", "exports", "imports"],
            "trade": ["exports", "imports"],
            "consumption": ["regional_demand_minus_imports", "imports"],
            "production": ["regional_demand_minus_imports", "exports"],
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
        production_account = {key: 0 for key in self.account_dictionary["all"]}
        for year in self.trade_container:
            self.trade_container[year] = {
                region: deepcopy(production_account) for region in region_list
            }

    def return_container(self):
        return self.trade_container

    def full_instantiation(self, year_range: range, region_list: list):
        self.initiate_years(year_range)
        self.initiate_regions(region_list)

    def return_market_entry(
        self,
        regional_demand_minus_imports: float,
        import_value: float,
        export_value: float,
    ) -> dict:
        return {
            "regional_demand_minus_imports": regional_demand_minus_imports,
            "import_value": import_value,
            "export_value": export_value,
        }

    def assign_market_tuple(self, year: int, region: str, market_entry: dict) -> None:
        self.assign_trade_balance(
            year,
            region,
            "regional_demand_minus_imports",
            market_entry["regional_demand_minus_imports"],
        )
        self.assign_trade_balance(year, region, "imports", market_entry["import_value"])
        self.assign_trade_balance(year, region, "exports", market_entry["export_value"])
        return None

    def trade_container_getter(
        self, year: int, region: str = None, account_type: str = None
    ):
        if account_type:
            return self.trade_container[year][region][account_type]
        if region:
            return self.trade_container[year][region]
        return self.trade_container[year]

    def return_trade_balance(self, year: int, region: str, account_type: str) -> float:
        imports = self.trade_container[year][region]["imports"]
        exports = self.trade_container[year][region]["exports"]
        regional_demand_minus_imports = self.trade_container[year][region][
            "regional_demand_minus_imports"
        ]
        if account_type == "trade":
            return exports - imports
        elif account_type == "all":
            return regional_demand_minus_imports + exports + imports
        elif account_type == "consumption":
            return regional_demand_minus_imports + imports
        elif account_type == "production":
            return regional_demand_minus_imports + exports
        return regional_demand_minus_imports + exports + imports  # defaults to all

    def trade_container_aggregator(
        self, year: int, agg_type: str, region: str = None
    ) -> float:
        if region:
            return self.return_trade_balance(year, region, agg_type)
        container = [
            self.return_trade_balance(year, region, agg_type)
            for region in self.trade_container[year].keys()
        ]
        return sum(container)

    def list_regional_types(self, year: int, account_type: str) -> list:
        return [
            region
            for region in self.trade_container[year]
            if round(
                self.trade_container[year][region][account_type], TRADE_ROUNDING_NUMBER
            )
            > 0
        ]

    def check_if_trade_balance(self, year: int) -> list:
        balance_list = []
        for region in self.trade_container[year]:
            imports = self.trade_container[year][region]["imports"]
            exports = self.trade_container[year][region]["exports"]
            if (
                round(exports, TRADE_ROUNDING_NUMBER)
                - round(imports, TRADE_ROUNDING_NUMBER)
                == 0
            ):
                balance_list.append(region)
        return balance_list

    def return_current_account_balance(self, year: int, region: str, account_type: str):
        return self.trade_container[year][region][account_type]

    def assign_trade_balance(
        self, year: int, region: str, account_type: str, value: float
    ) -> None:
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
            return steel_demand_getter(
                df=demand_df, year=row.year, metric="crude", region=row.region
            )

        market_dict = self.trade_container
        df = pd.DataFrame.from_dict(
            {
                (i, j): market_dict[i][j]
                for i in market_dict.keys()
                for j in market_dict[i].keys()
            },
            orient="index",
        ).rename_axis(index=["year", "region"])
        df.reset_index(inplace=True)
        df["demand"] = df.apply(map_demand, demand_df=demand_df, axis=1)
        df["trade_balance"] = df["exports"] - df["imports"]
        df["result_validity_check"] = (
            round(
                df["demand"]
                - df["regional_demand_minus_imports"]
                - df["exports"]
                + df["exports"]
                - df["imports"],
                TRADE_ROUNDING_NUMBER,
            )
            == 0
        )
        column_order = [
            "year",
            "region",
            "demand",
            "imports",
            "exports",
            "regional_demand_minus_imports",
            "trade_balance",
        ]
        return df[column_order]

    def output_trade_calculations_to_df(
        self, store_type: str, demand_df: pd.DataFrame = pd.DataFrame()
    ):
        market_result_list = [
            df for df in self.market_results.values() if isinstance(df, pd.DataFrame)
        ]
        competitiveness_list = [
            df
            for df in self.regional_competitiveness.values()
            if isinstance(df, pd.DataFrame)
        ]
        if not demand_df.empty:
            trade_account_df = self.create_trade_balance_summary(demand_df)
        if store_type == "market_results":
            return (
                pd.concat(market_result_list, axis=1)
                if market_result_list
                else pd.DataFrame()
            )
        if store_type == "competitiveness":
            return (
                pd.concat(competitiveness_list, axis=1)
                if competitiveness_list
                else pd.DataFrame()
            )
        if store_type == "trade_account":
            return trade_account_df if not trade_account_df.empty else pd.DataFrame()
        if store_type == "merge_trade_summary" and not demand_df.empty:
            competitiveness_df = (
                pd.concat(competitiveness_list, axis=0)
                if competitiveness_list
                else pd.DataFrame()
            )
            if competitiveness_df.empty and trade_account_df.empty:
                return pd.DataFrame()
            elif competitiveness_df.empty and not trade_account_df.empty:
                return trade_account_df
            elif not competitiveness_df.empty and trade_account_df.empty:
                return competitiveness_df
            return merge_competitiveness_with_trade_account(
                competitiveness_df, trade_account_df
            )


def merge_competitiveness_with_trade_account(
    competitiveness_df: pd.DataFrame, trade_account_df: pd.DataFrame
) -> pd.DataFrame:
    competitiveness_df.rename({"rmi_region": "region"}, axis=1, inplace=True)
    competitiveness_df.set_index(["year", "region"], inplace=True)
    trade_account_df.set_index(["year", "region"], inplace=True)
    return competitiveness_df.join(trade_account_df).reset_index()
