"""Script to determine the variable plant cost types dependent on regions."""

import itertools
import pandas as pd
from typing import Iterable, Union
from pandas.testing import assert_series_equal

from mppsteel.config.model_config import (
    PKL_DATA_FORMATTED,
    TON_TO_KILOGRAM_FACTOR,
    MODEL_YEAR_RANGE,
    PKL_DATA_IMPORTS,
    PROJECT_PATH,
    USD_TO_EUR_CONVERSION_DEFAULT,
)
from mppsteel.config.reference_lists import RESOURCE_CATEGORY_MAPPER
from mppsteel.utility.utils import cast_to_float
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    return_pkl_paths,
    serialize_file
)
from mppsteel.model_tests.df_tests import (
    test_negative_df_values,
    test_negative_list_values,
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.utility.dataframe_utility import convert_currency_col

# Create logger
logger = get_logger(__name__)


def generate_feedstock_dict(eur_to_usd_rate: float, project_dir=None) -> dict:
    """Creates a feedstock dictionary that combines all non-energy model commodities into one dictionary.
    The dictionary has a pairing of the commodity name and the price.

    Args:
        eur_to_usd (float): The rate used ot convert EUR values to USD.

    Returns:
        dict: A dictionary containing the pairing of feedstock name and price.
    """

    def standardise_units(row):
        return (
            row.Value * TON_TO_KILOGRAM_FACTOR
            if row.Metric in {"BF slag", "Other slag"}
            else row.Value
        )
    if project_dir is not None:
        feedstock_prices = read_pickle_folder(project_dir / PKL_DATA_IMPORTS, "feedstock_prices", "df")
    else:
        feedstock_prices = read_pickle_folder(PKL_DATA_IMPORTS, "feedstock_prices", "df")
    feedstock_prices = convert_currency_col(feedstock_prices, "Value", eur_to_usd_rate)
    feedstock_prices["Value"] = feedstock_prices.apply(standardise_units, axis=1)
    if project_dir is not None:
        commodities_df = read_pickle_folder(project_dir / PKL_DATA_FORMATTED, "commodities_df", "df")
    else:
        commodities_df = read_pickle_folder(PKL_DATA_FORMATTED, "commodities_df", "df")
    commodities_dict = {
        "Plastic waste": sum(
            commodities_df["netenergy_gj"] * commodities_df["implied_price"]
        )
        / commodities_df["netenergy_gj"].sum()
    }
    return {
        **commodities_dict,
        **dict(zip(feedstock_prices["Metric"], feedstock_prices["Value"])),
    }


def convert_to_category(*args, columns=["material_category", "country_code", "year"]):
    for df in args:
        for col in columns:
            df[col] = df[col].astype("category")
    return args


def merge_price_lookup_dfs(price_lookup_dfs):
    df = pd.concat(price_lookup_dfs)
    df = convert_to_category(df)[0]
    return df


class PlantVariableCostsInput:
    """
    Create a dataframe with the prices for the biofuel and bioenergy commodities.

    The input for this dataframe can be read from filesystem:
     - PlantVariableCostsInput.from_filesystem(scenario_dict, project_dir, **kwargs)
    """
    def __init__(
        self,
        *,
        product_range_year_country: list[tuple[int, str]] = [],
        resource_category_mapper: dict[str, str] = {},
        business_cases: pd.DataFrame = pd.DataFrame(
            [], columns=["technology", "material_category", "metric_type", "unit", "value"]
        ),
        power_grid_prices_ref: dict[tuple[int, str], float] = {},
        h2_prices_ref: dict[tuple[int, str], float] = {},
        bio_model_prices_ref: dict[tuple[int, str], float] = {},
        year_range: Iterable[int] = [],
        ccs_model_storage_ref: dict[str, float] = {},
        ccs_model_transport_ref: dict[str, float] = {},
        fossil_fuel_ref: dict[tuple[int, str, str], float] = {},
        static_energy_prices: pd.DataFrame = pd.DataFrame(
            [], columns=["Metric", "Year", "Value"]
        ).set_index(["Metric", "Year"]),
        feedstock_dict: dict[str, float] = {},
        country_codes: list[str] = [],
    ):
        self.product_range_year_country = product_range_year_country
        self.resource_category_mapper = resource_category_mapper
        self.business_cases = business_cases
        self.power_grid_prices_ref = power_grid_prices_ref
        self.h2_prices_ref = h2_prices_ref
        self.bio_model_prices_ref = bio_model_prices_ref
        self.year_range = year_range
        self.ccs_model_storage_ref = ccs_model_storage_ref
        self.ccs_model_transport_ref = ccs_model_transport_ref
        self.fossil_fuel_ref = fossil_fuel_ref
        self.static_energy_prices = static_energy_prices
        self.feedstock_dict = feedstock_dict
        self.country_codes = country_codes
        self._df_years_and_country_codes = None

    @classmethod
    def from_filesystem(
        cls,
        scenario_dict: dict,
        intermediate_path: str=None,
        project_dir: str=PROJECT_PATH,
        resource_category_mapper: dict=RESOURCE_CATEGORY_MAPPER,
        year_range: range=MODEL_YEAR_RANGE,
    ):

        intermediate_path = project_dir / intermediate_path
        eur_to_usd_rate = scenario_dict["eur_to_usd"]

        steel_plants = read_pickle_folder(
            project_dir / PKL_DATA_FORMATTED, "steel_plants_processed", "df"
        )

        power_grid_prices_ref = read_pickle_folder(
            intermediate_path, "power_grid_prices_ref", "df"
        )
        h2_prices_ref = read_pickle_folder(intermediate_path, "h2_prices_ref", "df")
        bio_model_prices_ref = read_pickle_folder(
            intermediate_path, "bio_model_prices_ref", "df"
        )
        ccs_model_transport_ref = read_pickle_folder(
            intermediate_path, "ccs_model_transport_ref", "df"
        )
        ccs_model_storage_ref = read_pickle_folder(
            intermediate_path, "ccs_model_storage_ref", "df"
        )
        fossil_fuel_ref = read_pickle_folder(
            intermediate_path, "fossil_fuel_ref", "df"
        )
        business_cases = read_pickle_folder(
            project_dir / PKL_DATA_FORMATTED, "standardised_business_cases", "df"
        ).reset_index()
        static_energy_prices = read_pickle_folder(
            project_dir / PKL_DATA_IMPORTS, "static_energy_prices", "df"
        )[["Metric", "Year", "Value"]]
        static_energy_prices.set_index(["Metric", "Year"], inplace=True)
        static_energy_prices["Value"] = static_energy_prices["Value"] / USD_TO_EUR_CONVERSION_DEFAULT
        feedstock_dict = generate_feedstock_dict(
            eur_to_usd_rate, project_dir=project_dir
        )
        country_codes = list(steel_plants["country_code"].unique())
        product_range_year_country = list(itertools.product(year_range, country_codes))
        return cls(
            product_range_year_country=product_range_year_country,
            resource_category_mapper=resource_category_mapper,
            business_cases=business_cases,
            power_grid_prices_ref=power_grid_prices_ref,
            h2_prices_ref=h2_prices_ref,
            bio_model_prices_ref=bio_model_prices_ref,
            year_range=year_range,
            ccs_model_storage_ref=ccs_model_storage_ref,
            ccs_model_transport_ref=ccs_model_transport_ref,
            fossil_fuel_ref=fossil_fuel_ref,
            static_energy_prices=static_energy_prices,
            feedstock_dict=feedstock_dict,
            country_codes=country_codes,
        )

    def create_df_from_years_and_country_codes(self):
        if self._df_years_and_country_codes is None:
            self._df_years_and_country_codes = pd.merge(
                pd.DataFrame(self.country_codes, columns=["country_code"]),
                pd.DataFrame(self.year_range, columns=["year"]),
                how="cross",
            )
        return self._df_years_and_country_codes.copy()

    def get_power_grid_prices(self):
        pgp_ref_list = [
            (year, cc, price)
            for (year, cc), price in self.power_grid_prices_ref.items()
        ]
        df = pd.DataFrame(pgp_ref_list, columns=("year", "country_code", "price"))
        df["material_category"] = "Electricity"
        return df,

    def get_hydrogen_prices(self):
        h2_ref_list = [
            (year, cc, price) for (year, cc), price in self.h2_prices_ref.items()
        ]
        df = pd.DataFrame(h2_ref_list, columns=("year", "country_code", "price"))
        df["material_category"] = "Hydrogen"
        return df,

    def get_bio_model_prices(self):
        df_mass = pd.DataFrame(
            (
                (year, country_code, price)
                for (year, country_code), price in self.bio_model_prices_ref.items()
            ),
            columns=("year", "country_code", "price"),
        )
        df_mass["material_category"] = "Biomass"
        df_methane = df_mass.copy()
        df_methane["material_category"] = "Biomethane"
        return df_mass, df_methane

    def get_store_and_transport_prices(self):
        df_year = pd.DataFrame(self.year_range, columns=["year"])
        df_storage = pd.DataFrame(
            self.ccs_model_storage_ref.items(),
            columns=("country_code", "price_storage"),
        )
        df_transport = pd.DataFrame(
            self.ccs_model_transport_ref.items(),
            columns=("country_code", "price_transport"),
        )
        df_store_trans = pd.merge(df_storage, df_transport, on=("country_code"))
        df_store_trans["price"] = (
            df_store_trans.price_storage + df_store_trans.price_transport
        )
        df_store_trans = df_store_trans.drop(
            ["price_storage", "price_transport"], axis=1
        )
        df_store_trans = df_store_trans.merge(df_year, how="cross")
        df_store_trans_captured = df_store_trans.copy()
        df_store_trans_captured["material_category"] = "Captured CO2"
        df_transport_f = df_transport.drop(["price_transport"], axis=1)
        df_transport_f["price"] = 0
        df_transport_f["material_category"] = "Used CO2"
        df_transport_f = df_transport_f.merge(df_year, how="cross")
        return df_store_trans_captured, df_transport_f

    def get_gas_prices_per_country_and_year(self, gas_type, country_codes):
        gas_prices = self.static_energy_prices.loc[gas_type].reset_index().copy()
        year_to_price = dict(zip(gas_prices.Year.values, gas_prices.Value.values))
        default_price = year_to_price[
            2026
        ]  # FIXME make this depend on the available range of years
        df_data = {"year": [], "price": []}
        for year in self.year_range:
            df_data["year"].append(year)
            df_data["price"].append(year_to_price.get(year, default_price))
        df = pd.DataFrame(df_data)
        return pd.merge(
            df, pd.DataFrame(country_codes, columns=["country_code"]), how="cross"
        )

    def get_gas_prices(self):
        return pd.DataFrame(
            ((*key, price) for key, price in self.fossil_fuel_ref.items()),
            columns=["year", "country_code", "material_category", "price"]
        ),

    def get_plastic_waste(self):
        df = self.create_df_from_years_and_country_codes()
        df["price"] = self.feedstock_dict["Plastic waste"]
        df["material_category"] = "Plastic waste"
        return df,

    def get_fossil_category_prices(self, category):
        fossil_prices = self.static_energy_prices.loc[category].reset_index().copy()
        year_to_price = dict(zip(fossil_prices.Year.values, fossil_prices.Value.values))
        default_price = year_to_price[
            2026
        ]  # FIXME make this depend on the available range of years
        data = {"year": [], "price": []}
        for year in self.year_range:
            data["year"].append(year)
            data["price"].append(year_to_price.get(year, default_price))
        df = pd.DataFrame(data)
        df["material_category"] = category
        return pd.merge(
            df, pd.DataFrame(self.country_codes, columns=["country_code"]), how="cross"
        )

    def get_fossil_fuel_prices(self):
        fossil_categories = [
            k
            for k, v in self.resource_category_mapper.items()
            if v == "Fossil Fuels" and k not in ("Natural gas", "Plastic waste", "Met coal", "Thermal coal")
        ]
        fossil_price_dfs = []
        for category in fossil_categories:
            fossil_price_dfs.append(self.get_fossil_category_prices(category))
        return fossil_price_dfs

    def get_feedstock_prices(self):
        df_iron_ore = self.create_df_from_years_and_country_codes()
        df_iron_ore["price"] = self.feedstock_dict["Iron ore"]
        df_iron_ore["material_category"] = "Iron ore"
        df_scrap = self.create_df_from_years_and_country_codes()
        df_scrap["price"] = self.feedstock_dict["Scrap"]
        df_scrap["material_category"] = "Scrap"
        df_dri = self.create_df_from_years_and_country_codes()
        df_dri["price"] = self.feedstock_dict["DRI"]
        df_dri["material_category"] = "DRI"
        return df_iron_ore, df_scrap, df_dri

    def get_other_opex(self):
        df_bf_slag = self.create_df_from_years_and_country_codes()
        df_bf_slag["price"] = self.feedstock_dict["BF slag"]
        df_bf_slag["material_category"] = "BF slag"
        df_other_slag = self.create_df_from_years_and_country_codes()
        df_other_slag["price"] = self.feedstock_dict["Other slag"]
        df_other_slag["material_category"] = "Other slag"
        return df_bf_slag, df_other_slag

    def get_steam_prices(self):
        df_steam = self.get_gas_prices_per_country_and_year("Steam", self.country_codes)
        df_steam["material_category"] = "Steam"
        return df_steam,

    def get_price_lookup_df(self):
        price_lookup_dfs = [
            *self.get_power_grid_prices(),
            *self.get_hydrogen_prices(),
            *self.get_bio_model_prices(),
            *self.get_store_and_transport_prices(),
            *self.get_gas_prices(),
            *self.get_plastic_waste(),
            *self.get_fossil_fuel_prices(),
            *self.get_feedstock_prices(),
            *self.get_other_opex(),
            *self.get_steam_prices(),
        ]
        return merge_price_lookup_dfs(price_lookup_dfs)


def build_variable_cost_df(input_data: PlantVariableCostsInput) -> pd.DataFrame:
    """
    Builds a dataframe with the variable costs of the plants for all years.
    """
    dyc = pd.DataFrame(
        input_data.product_range_year_country, columns=("year", "country_code")
    )
    emissions = set(
        [k for k, v in input_data.resource_category_mapper.items() if v == "Emissivity"]
    )
    db = input_data.business_cases.copy()
    db = db[~db.material_category.isin(emissions)]
    df = db.merge(dyc, how="cross")
    not_categorical = set(["value"])
    categorical_columns = [col for col in df.columns if col not in not_categorical]
    for col in categorical_columns:
        df[col] = df[col].astype("category")
    df["cost"] = 0.0
    return df


def plant_variable_costs_vectorized(input_data: PlantVariableCostsInput) -> pd.DataFrame:
    """
    Creates a dataframe with the variable costs of the plants.

    It's building a dataframe with the product range year country for
    all business cases and merges it with the dataframe holding
    the prices for all material categories. The resulting costs column
    is just the product between value and price or 0 if there's no price
    for the material category.

    Args:
        input_data (PlantVariableCostsInput): object holding all the input data needed.

    Returns:
        pd.DataFrame: A DataFrame containing each plant's variable costs.
    """
    df = build_variable_cost_df(input_data)
    df_prices = input_data.get_price_lookup_df()
    dm = df.merge(
        df_prices, on=("material_category", "year", "country_code"), how="left"
    )
    dm["country_code"] = dm["country_code"].astype("category")
    dm["material_category"] = dm["material_category"].astype("category")
    dm["cost"] = dm.value * dm.price
    dm["cost"] = dm.cost.fillna(0.0)
    dm["cost_type"] = dm["material_category"].apply(
        lambda material: input_data.resource_category_mapper[material]
    )
    dm["cost_type"] = dm["cost_type"].astype("category")
    
    return dm


def plant_variable_costs(input_data: PlantVariableCostsInput) -> pd.DataFrame:
    """
    Creates a DataFrame reference of each plant's variable cost.

    Args:
        input_data (PlantVariableCostsInput): object holding all the input data for calculation.

    Returns:
        pd.DataFrame: A DataFrame containing each plant's variable costs.

    """
    return plant_variable_costs_vectorized(input_data)


def format_variable_costs(
    variable_cost_df: pd.DataFrame, group_data: bool = True
) -> pd.DataFrame:
    """Formats a Variable Costs DataFrame generated via the plant_variable_costs function.

    Args:
        variable_cost_df (pd.DataFrame): A DataFrame generated from the plant_variable_costs function.
        group_data (bool, optional): Boolean flag that groups data by "country_code", "year", "technology". Defaults to True.

    Returns:
        pd.DataFrame: A formatted variable costs DataFrame.
    """

    df_c = variable_cost_df.copy()
    df_c.reset_index(drop=True, inplace=True)
    df_em = df_c[df_c["material_category"] != "Emissivity"]
    assert_series_equal(df_em["cost"], df_em["value"] * df_em["price"], check_names=False)
    if group_data:
        prices_columns = [col for col in df_c.columns if 'price' in col]
        non_price_columns_to_drop = ["material_category", "unit", "cost_type", "value"]
        df_c.drop(
            prices_columns + non_price_columns_to_drop, axis=1, inplace=True
        )
        df_c = (
            df_c.groupby(by=["country_code", "year", "technology"])
            .sum()
            .sort_values(by=["country_code", "year", "technology"])
        )
        df_c["cost"] = df_c["cost"].apply(lambda x: cast_to_float(x))

    return df_c

@timer_func
def generate_variable_plant_summary(
    scenario_dict: dict, pkl_paths: Union[dict, None] = None, serialize: bool = False
) -> pd.DataFrame:
    """The complete flow for creating variable costs.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        pkl_paths (Union[dict, None], optional): A dictionary containing custom pickle paths. Defaults to {}.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the variable plant results.
    """
    _, intermediate_path, _ = return_pkl_paths(scenario_name=scenario_dict["scenario_name"], paths=pkl_paths)
    input_data = PlantVariableCostsInput.from_filesystem(scenario_dict, intermediate_path)
    variable_costs = plant_variable_costs(input_data)
    variable_costs_summary = format_variable_costs(variable_costs)
    variable_costs_summary_material_breakdown = format_variable_costs(
        variable_costs, group_data=False
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(
            variable_costs_summary, intermediate_path, "variable_costs_regional"
        )
        serialize_file(
            variable_costs_summary_material_breakdown,
            intermediate_path,
            "variable_costs_regional_material_breakdown",
        )
    return variable_costs_summary
