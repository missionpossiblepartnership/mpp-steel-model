"""Script with functions for implementing solver constraints."""

import itertools
import pandas as pd

from mppsteel.config.model_config import (
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    TECH_MORATORIUM_DATE,
    SCRAP_CONSTRAINT_TOLERANCE_FACTOR,
)
from mppsteel.config.reference_lists import TECHNOLOGY_PHASES
from mppsteel.utility.log_utility import get_logger

# Create logger
logger = get_logger(__name__)


def map_technology_state(tech: str) -> str:
    """Returns the technology phase according to a technology phases dictionary.

    Args:
        tech (str): The technology you want to return the technology phase for.

    Returns:
        str: The technology phase of `tech`.
    """
    for tech_state in TECHNOLOGY_PHASES.keys():
        if tech in TECHNOLOGY_PHASES[tech_state]:
            return tech_state


def read_and_format_tech_availability(df: pd.DataFrame) -> pd.DataFrame:
    """Formats the technology availability DataFrame.

    Args:
        df (pd.DataFrame): A Technology availability DataFrame.

    Returns:
        pd.DataFrame: A formatted technology availability DataFrame.
    """
    df_c = df.copy()
    df_c.columns = [col.lower().replace(" ", "_") for col in df_c.columns]
    df_c = df_c[
        ~df_c["technology"].isin(
            ["Close plant", "Charcoal mini furnace", "New capacity"]
        )
    ]
    df_c["technology_phase"] = df_c["technology"].apply(
        lambda x: map_technology_state(x)
    )
    col_order = [
        "technology",
        "main_technology_type",
        "technology_phase",
        "year_available_from",
        "year_available_until",
    ]
    return df_c[col_order].set_index("technology")


def tech_availability_check(
    tech_df: pd.DataFrame,
    technology: str,
    year: int,
    tech_moratorium: bool = False,
    default_year_unavailable: int = 2200,
) -> bool:
    """Checks whether a technology is available in a given year.

    Args:
        tech_df (pd.DataFrame): The technology availability DataFrame.
        technology (str): The technology to check availability for.
        year (int): The year to check whether a specified `technology` is available or not.
        tech_moratorium (bool, optional): Boolean flag that determines whether a specified technology is available or not. Defaults to False.
        default_year_unavailable (int): Determines the default year a given technology will not be available from - will be altered according to function logic. Defaults to 2200.

    Returns:
        bool: A boolean that determines whether a specified `technology` is available in the specified `year`.
    """
    row = tech_df.loc[technology]
    year_available_from = row.loc["year_available_from"]
    technology_phase = row.loc["technology_phase"]
    year_available_until = default_year_unavailable

    if tech_moratorium and (technology_phase in ["initial", "transitional"]):
        year_available_until = TECH_MORATORIUM_DATE
    if int(year_available_from) <= int(year) < int(year_available_until):
        # Will be available
        return True
    if int(year) <= int(year_available_from):
        # Will not be ready yet
        return False
    if int(year) > int(year_available_until):
        # Will become unavailable
        return False


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
    rsd = (
        rsd[["region", "value"]]
        .loc[:, :, "Scrap availability"]
        .reset_index()
        .drop(["scenario"], axis=1)
        .set_index(["year", "region"])
        .copy()
    )
    rsd["value"] = rsd["value"] * (1 + SCRAP_CONSTRAINT_TOLERANCE_FACTOR)
    return {
        year: rsd.loc[year].to_dict()["value"]
        for year in rsd.index.get_level_values(0)
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
