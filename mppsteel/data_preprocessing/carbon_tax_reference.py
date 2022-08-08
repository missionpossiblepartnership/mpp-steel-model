"""Script to create Carbon Tax Reference"""

import itertools
from typing import Union
import pandas as pd

from tqdm import tqdm

from mppsteel.config.model_config import MODEL_YEAR_RANGE

from mppsteel.config.model_config import (
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED
)
from mppsteel.config.reference_lists import TECHNOLOGIES_TO_DROP

from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    return_pkl_paths,
    serialize_file
)
from mppsteel.utility.function_timer_utility import timer_func

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)

def carbon_tax_estimate(
    s1_emissions_value: float, s2_emissions_value: float, carbon_tax_value: float
) -> float:
    """Creates a carbon tax based on the scope 1 & 2 emissivity as a standardised unit and a technology and a carbon tax value per ton of steel.

    Args:
        s1_emissions_value (float): Scope 1 emissivity as a standarised unit.
        s2_emissions_value (float): Scope 2 emissivity as a standarised unit.
        carbon_tax_value (float): A carbon tax value per standardised unit.

    Returns:
        float: A  carbon tax estimate based on S1 & S2 emissions and a carbon tax per unit value.
    """
    return (s1_emissions_value + s2_emissions_value) * carbon_tax_value


def carbon_tax_estimate_handler(
    s1_emissions_ref: pd.DataFrame,
    s2_emissions_ref: pd.DataFrame,
    carbon_tax_timeseries: dict,
    year: int,
    country_code: str
) -> float:
    return carbon_tax_estimate(
        s1_emissions_ref.loc[year],
        s2_emissions_value = s2_emissions_ref.loc[year, country_code],
        carbon_tax_value = carbon_tax_timeseries[year]
    )

def create_carbon_tax_reference(
    product_range_year_country: list,
    s1_emissions_ref: pd.DataFrame,
    s2_emissions_ref: pd.DataFrame,
    carbon_tax_timeseries: dict
) -> dict:
    df_list = []
    for year, country_code in tqdm(
        product_range_year_country,
        total=len(product_range_year_country),
        desc="Carbon Tax Full Reference"
    ):
        df = carbon_tax_estimate_handler(
            s1_emissions_ref,
            s2_emissions_ref,
            carbon_tax_timeseries,
            year,
            country_code
        )
        df["year"] = year
        df["country_code"] = country_code
        df_list.append(df)
    full_df = pd.concat(df_list).rename(mapper={"emissions": "value"}, axis=1)
    return full_df.reset_index().set_index(["year", "country_code", "technology"]).sort_index()

@timer_func
def generate_carbon_tax_reference(
    scenario_dict: dict, pkl_paths: Union[dict, None] = None, serialize: bool = False
) -> pd.DataFrame:
    logger.info("Carbon Tax Preprocessing")

    _, intermediate_path, _ = return_pkl_paths(scenario_name=scenario_dict["scenario_name"], paths=pkl_paths)
    # Carbon Tax preprocessing
    carbon_tax_df = read_pickle_folder(intermediate_path, "carbon_tax_timeseries", "df")
    carbon_tax_df = carbon_tax_df.set_index("year")
    carbon_tax_ref = carbon_tax_df.to_dict()["value"]

    # Emissivity preprocessing
    calculated_s1_emissivity = read_pickle_folder(
        intermediate_path, "calculated_s1_emissivity", "df"
    )
    calculated_s2_emissivity = read_pickle_folder(
        intermediate_path, "calculated_s2_emissivity", "df"
    )
    calculated_s2_emissivity.set_index(
        ["year", "country_code", "technology"], inplace=True
    )
    calculated_s2_emissivity.rename(
        mapper={"s2_emissivity": "emissions"}, axis=1, inplace=True
    )
    calculated_s2_emissivity = calculated_s2_emissivity.sort_index(ascending=True)
    calculated_s1_emissivity.drop(TECHNOLOGIES_TO_DROP, level="technology", inplace=True)

    # Get iteration loop
    steel_plants = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )
    
    product_range_year_country = list(itertools.product(
        MODEL_YEAR_RANGE, steel_plants["country_code"].unique()))

    logger.info("Creating Carbon Tax Reference Table")

    carbon_tax_reference = create_carbon_tax_reference(
        product_range_year_country,
        calculated_s1_emissivity,
        calculated_s2_emissivity,
        carbon_tax_ref
    )

    if serialize:
        logger.info("-- Serializing dataframe")
        serialize_file(
            carbon_tax_reference,
            intermediate_path,
            "carbon_tax_reference",
        )
    return carbon_tax_reference
