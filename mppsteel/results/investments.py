"""Investment Results generator for technology investments"""

import itertools
from typing import Union

import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    MODEL_YEAR_START,
    MODEL_YEAR_END,
    PKL_DATA_IMPORTS,
    PKL_DATA_FORMATTED
)
from mppsteel.config.reference_lists import SWITCH_DICT

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.data_loading.steel_plant_formatter import map_plant_id_to_df
from mppsteel.results.production import get_tech_choice, production_stats_getter
from mppsteel.utility.location_utility import create_country_mapper

# Create logger
logger = get_logger(__name__)


def create_capex_dict() -> pd.DataFrame:
    """Creates a reformatted and reindexed Capex Switching DataFrame.

    Returns:
        pd.DataFrame: A reformatted and reindexed Capex Switching DataFrame.
    """
    capex = read_pickle_folder(PKL_DATA_FORMATTED, "capex_switching_df", "df")
    capex_c = capex.copy()
    capex_c.reset_index(inplace=True)
    capex_c.columns = [col.lower().replace(" ", "_") for col in capex_c.columns]
    return capex_c.set_index(["year", "start_technology"]).sort_index()


def capex_getter_f(
    capex_df: pd.DataFrame, year: int, start_tech: str, new_tech: str, switch_type: str
) -> float:
    """Returns a capex value from a reference DataFrame taking into consideration edge cases.

    Args:
        capex_df (pd.DataFrame): The capex reference DataFrame.
        year (int): The year you want to retrieve values for.
        start_tech (str): The initial technology used.
        new_tech (str): The desired switch technology.
        switch_type (str): The type of technology switch [`no switch`, `trans switch`, `main cycle`].

    Returns:
        float: A value containing the capex value based on the function arguments.
    """
    if not start_tech or (new_tech == "Close plant") or (switch_type == "no switch"):
        return 0
    capex_ref = capex_df.loc[year, start_tech]
    return capex_ref.loc[capex_ref["new_technology"] == new_tech]["value"].values[0]


def investment_switch_getter(
    inv_df: pd.DataFrame, year: int, plant_name: str
) -> str:
    """Returns the switch type of an investment made in a particular year for a particular plant.

    Args:
        inv_df (pd.DataFrame): The Investment cycle reference DataFrame.
        year (int): The year that you want to reference.
        plant_name (str): The name of the reference plant.

    Returns:
        str: The switch type [`no switch`, `trans switch`, `main cycle`].
    """
    return inv_df_ref.loc[year, plant_name].values[0]


def investment_row_calculator(
    investment_switch_ref: dict,
    capex_df: pd.DataFrame,
    tech_choices: dict,
    capacity_ref: dict,
    year: int,
    plant_name: str
) -> dict:
    """_summary_

    Args:
        inv_df (pd.DataFrame): The Investment cycle reference DataFrame.
        capex_df (pd.DataFrame): A switch capex reference. 
        tech_choices (dict): Dictionary containing all technology choices for every plant across every year.
        plant_name (str): The name of the reference plant.
        country_code (str): Country code of the plant.
        year (int): The year that you want to reference.
        capacity_value (float): The value of the plant capacity.

    Returns:
        dict: A dictionary containing the column-value pairs to be inserted in a DataFrame.
    """
    switch_type = investment_switch_ref[(year, plant_name)]

    if year == 2020:
        start_tech = get_tech_choice(tech_choices, 2020, plant_name)
    else:
        start_tech = get_tech_choice(tech_choices, year - 1, plant_name)

    new_tech = get_tech_choice(tech_choices, year, plant_name)
    actual_capex = 0
    if new_tech:
        capex_value = capex_getter_f(capex_df, year, start_tech, new_tech, switch_type)
        actual_capex = capex_value * (capacity_ref[plant_name] * 1000 * 1000)  # convert from Mt to T
    return {
        "plant_name": plant_name,
        "year": year,
        "start_tech": start_tech,
        "end_tech": new_tech,
        "switch_type": switch_type,
        "capital_cost": actual_capex,
    }


def create_inv_stats(
    df: pd.DataFrame, results: str = "global", agg: bool = False, operation: str = "sum"
) -> Union[pd.DataFrame, dict]:
    """Generates an statistics column for an Investment DataFrame according to parameters set in the function arguments.

    Args:
        df (pd.DataFrame): The initial Investments DataFrame.
        results (str, optional): Specifies the desired regional results [`regional` or `global`]. Defaults to "global".
        agg (bool, optional): Determines whether to aggregate regional results as a DataFrame. Defaults to False.
        operation (str, optional): Determines the type of operation to be conducted for the new stats column [`sum` or `cumsum` for cumulative sum]. Defaults to "sum".

    Returns:
        Union[pd.DataFrame, dict]: Returns a DataFrame if `results` is set to `global`. Returns a dict of regional results if `results` is set to `regional` and `agg` is set to False.
    """

    df_c = df[
        [
            "year",
            "plant_name",
            "country_code",
            "start_tech",
            "end_tech",
            "switch_type",
            "capital_cost",
            "rmi_region",
        ]
    ].copy()

    def create_global_stats(df, operation: str = "sum"):
        calc = df_c.groupby(["year"]).sum()
        if operation == "sum":
            return calc
        if operation == "cumsum":
            return calc.cumsum()

    if results == "global":
        return create_global_stats(df_c, operation).reset_index()

    if results == "regional":
        regions = df_c["rmi_region"].unique()
        region_dict = {}
        for region in regions:
            calc = df_c[df_c["rmi_region"] == region].groupby(["year"]).sum()
            if operation == "sum":
                pass
            if operation == "cumsum":
                calc = calc.cumsum()
            region_dict[region] = calc
        if agg:
            df_list = []
            for region_key in region_dict:
                df_r = region_dict[region_key]
                df_r["region"] = region_key
                df_list.append(df_r[["region", "capital_cost"]])
            return pd.concat(df_list).reset_index()
        return region_dict


def get_plant_cycle(plant_cycles: dict, plant_name: str, current_year: int, model_start_year: int, model_end_year: int):
    if (current_year < model_start_year) or (current_year > model_end_year):
        return None
    cycles = plant_cycles[plant_name]
    if len(cycles) == 0:
        return None
    first_year = cycles[0]
    if len(cycles) == 1:
        if first_year > current_year:
            return range(model_start_year, current_year + 1)
    elif len(cycles) == 2:
        second_year = cycles[1]
        if first_year > current_year:
            return range(model_start_year, current_year + 1)
        elif first_year <= current_year < second_year:
            return range(first_year, current_year + 1)
    elif len(cycles) == 3:
        third_year = cycles[2]
        if first_year > current_year:
            return range(model_start_year, current_year + 1)
        elif first_year <= current_year < second_year:
            return range(first_year, current_year + 1)
        elif second_year <= current_year < third_year:
            return range(second_year, current_year + 1)
    return range(current_year, model_end_year + 1)


def get_investment_capital_costs(investment_df: pd.DataFrame, investment_cycles: dict, plant_name: str, current_year: int):
    range_obj = get_plant_cycle(investment_cycles, plant_name, current_year, MODEL_YEAR_START, MODEL_YEAR_END)
    if range_obj:
        return investment_df.set_index(['year']).loc[list(range_obj)]['capital_cost'].sum()
    else:
        return 0

@timer_func
def investment_results(scenario_dict: dict, serialize: bool = False) -> pd.DataFrame:
    """Complete Investment Results Flow to generate the Investment Results References DataFrame.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the investment results.
    """
    logger.info("Generating Investment Results")
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    final_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'final')
    
    tech_choice_dict = read_pickle_folder(
        intermediate_path, "tech_choice_dict", "df"
    )
    plant_investment_cycles = read_pickle_folder(
        intermediate_path, "investment_cycle_ref_result", "df"
    )
    plant_investment_cycles_c = plant_investment_cycles.reset_index().set_index(["year", "plant_name"]).sort_values(["year"])
    plant_result_df = read_pickle_folder(
        intermediate_path, "plant_result_df", "df"
    )
    plant_names = plant_result_df["plant_name"].unique()
    country_codes = plant_result_df["country_code"].unique()
    technologies = SWITCH_DICT.keys()
    production_resource_usage = read_pickle_folder(
        final_path, "production_resource_usage", "df"
    )
    capex_df = create_capex_dict()
    max_year = max([int(year) for year in tech_choice_dict])
    year_range = range(MODEL_YEAR_START, max_year + 1)


    year_plant_product = list(itertools.product(year_range, plant_names))
    year_plant_tech_product = list(itertools.product(year_range, plant_names, technologies))
    year_country_code_plant_product = list(itertools.product(year_range, country_codes, plant_names))

    capacity_ref = dict(zip(plant_result_df['plant_name'], plant_result_df['plant_capacity']))
    plant_country_code_ref = dict(zip(plant_result_df['plant_name'], plant_result_df['country_code']))
    
    investment_switch_ref = {}
    for year, plant_name in tqdm(
        year_plant_product,
        total=len(year_plant_product),
        desc="Investment Reference Loop",
    ):
        investment_switch_ref[(year, plant_name)] = plant_investment_cycles_c.loc[year, plant_name].values[0]

    data_container = []
    for year, plant_name in tqdm(
        year_plant_product,
        total=len(year_plant_product),
        desc="Creating Steel Plant Investment DataFrame",
    ):
        data_container.append(
            investment_row_calculator(
                investment_switch_ref,
                capex_df,
                tech_choice_dict,
                capacity_ref,
                year,
                plant_name
            )
        )

    investment_results = (
        pd.DataFrame(data_container).set_index(["year"]).sort_values("year")
    )
    country_ref = read_pickle_folder(PKL_DATA_IMPORTS, "country_ref", "df")
    rmi_mapper = create_country_mapper(country_ref, 'rmi')
    investment_results['country_code'] = investment_results['plant_name'].apply(
        lambda x: plant_country_code_ref[x])
    investment_results['rmi_region'] = investment_results['country_code'].apply(
            lambda x: rmi_mapper[x])
    investment_results.reset_index(inplace=True)
    investment_results = map_plant_id_to_df(investment_results, plant_result_df, "plant_name")
    investment_results = add_results_metadata(
        investment_results, scenario_dict, single_line=True
    )
    cumulative_investment_results = create_inv_stats(
        investment_results, results="regional", agg=True, operation="cumsum"
    )

    if serialize:
        logger.info(f"-- Serializing dataframes")
        serialize_file(
            investment_results, 
            final_path, 
            "investment_results"
        )
        serialize_file(
            cumulative_investment_results,
            final_path,
            "cumulative_investment_results",
        )
    return investment_results
