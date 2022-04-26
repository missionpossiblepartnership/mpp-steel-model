"""Script with functions for implementing solver constraints."""

import pandas as pd

from mppsteel.config.model_config import (
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    TECH_MORATORIUM_DATE,
)
from mppsteel.config.reference_lists import (
    TECHNOLOGY_PHASES
)
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
    """[summary]

    Args:
        tech_df (pd.DataFrame): A formatted tech availability DataFrame.
        technology (str): The technology to check availability for.
        year (int): The year to check whether a specified `technology` is available or not.
        tech_moratorium (bool, optional): Boolean flag that determines whether a specified technology is available or not. Defaults to False.
        default_year_unavailable (int): Determines the default year a given technology will not be available from - will be altered according to function logic.

    Returns:
        bool: A boolean that determines whether a specified `technology` is available in the specified `year`.
    """
    row = tech_df.loc[technology]
    year_available_from = row.loc["year_available_from"]
    technology_phase = row.loc["technology_phase"]
    year_available_until = default_year_unavailable

    if tech_moratorium and (technology_phase in ["Initial", "Transition"]):
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


def create_co2_use_constraint(model: pd.DataFrame): # Mt CO2
    return model[model['Metric'] == 'Steel CO2 use market'][['Value', 'Year']].set_index(['Year']).to_dict()['Value']

def create_ccs_constraint(model: pd.DataFrame): # Mt CO2
    return model.swaplevel().loc['Global'][['value']].to_dict()['value']

def create_biomass_constraint(model: pd.DataFrame): # GJ Energy
    return model[['value']].to_dict()['value']

def create_scrap_constraints(model: pd.DataFrame, world: bool = True): # Mt Scrap
    rsd = model[['region', 'value']] \
        .loc[:,:,'Scrap availability'].reset_index() \
        .drop(['scenario'], axis=1) \
        .set_index(['year', 'region']) \
        .copy()
    if world:
        return {int(year): rsd.loc[str(year)].to_dict()['value']['World'] for year in rsd.index.get_level_values(0)}
    return {int(year): rsd.loc[str(year)].to_dict()['value'] for year in rsd.index.get_level_values(0)}


def return_projected_usage(
    plant_name: str, technology: str, capacities_dict: dict,
    business_case_ref: dict, materials: list
):
    return sum([
        business_case_ref[(technology, material)] * (capacities_dict[plant_name] * CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION) for material in materials 
    ])


def return_current_usage(
    plant_list: list, technology_choices: dict, capacities_dict: dict,
    utilization_dict: dict, plant_region_dict: dict,
    business_case_ref: dict, materials: list,
):

    usage_sum = []
    for material in materials:
        agg = sum([
            business_case_ref[(technology_choices[plant_name], material)] * (capacities_dict[plant_name] * utilization_dict[plant_region_dict[plant_name]] )  for plant_name in plant_list ])
        usage_sum.append(agg)
    return sum(usage_sum)
