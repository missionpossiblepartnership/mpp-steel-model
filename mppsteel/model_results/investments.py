"""Investment Results generator for technology investments"""

from typing import Union

import pandas as pd
from tqdm import tqdm

from mppsteel.config.model_config import (
    MEGATON_TO_TON,
    MODEL_YEAR_RANGE,
    MODEL_YEAR_START,
    MODEL_YEAR_END,
    PKL_DATA_FORMATTED
)

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.dataframe_utility import add_results_metadata
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.model_results.production import get_tech_choice
from mppsteel.utility.log_utility import get_logger
from mppsteel.data_load_and_format.steel_plant_formatter import map_plant_id_to_df
from mppsteel.utility.location_utility import create_country_mapper

# Create logger
logger = get_logger(__name__)


def capex_getter_f(
    capex_ref: dict, greenfield_ref: dict, year: int, start_tech: str, new_tech: str, switch_type: str
) -> float:
    """Returns a capex value from a reference DataFrame taking into consideration edge cases.

    Args:
        capex_ref (dict): The capex reference DataFrame.
        greenfield_ref (dict): Greenfield capex reference.
        year (int): The year you want to retrieve values for.
        start_tech (str): The initial technology used.
        new_tech (str): The desired switch technology.
        switch_type (str): The type of technology switch [`no switch`, `trans switch`, `main cycle`].

    Returns:
        float: A value containing the capex value based on the function arguments.
    """
    if not start_tech:
        return greenfield_ref[(new_tech, year)]
    elif new_tech == "Close plant":
        return 0
    elif switch_type == "no switch":
        return 0
    elif (start_tech == new_tech) and (switch_type == 'trans switch'):
        return 0
    else:
        return capex_ref[(year, start_tech, new_tech)]


def investment_row_calculator(
    plant_investment_cycles: pd.DataFrame,
    switch_capex_ref: dict,
    greenfield_ref: dict,
    active_plant_checker_dict: dict,
    tech_choices: dict,
    capacity_ref: dict,
    year: int,
    plant_name: str
) -> dict:
    """Calculates a row for the investment DataFrame by establishing the switch type for the plant in a particular year, and where necessary, applying a capex value.

    Args:
        plant_investment_cycles (pd.DataFrame): The Investment cycle reference DataFrame.
        switch_capex_ref (dict): A switch capex reference.
        greenfield_ref (dict): Greenfield capex reference.
        active_plant_checker_dict (dict): Dictionary with values of whether a plant in a particular year was active or not.
        tech_choices (dict): Dictionary containing all technology choices for every plant across every year.
        capacity_ref (dict): A dictionary of capacity references.
        year (int): The year that you want to reference.
        plant_name (str): The name of the reference plant.

    Returns:
        dict: A dictionary containing the column-value pairs to be inserted in a DataFrame.
    """
    switch_type = plant_investment_cycles.loc[year, plant_name]['switch_type']
    if year == MODEL_YEAR_START:
        start_tech = get_tech_choice(tech_choices, active_plant_checker_dict, MODEL_YEAR_START, plant_name)
    else:
        start_tech = get_tech_choice(tech_choices, active_plant_checker_dict, year - 1, plant_name)

    new_tech = get_tech_choice(tech_choices, active_plant_checker_dict, year, plant_name)
    actual_capex = 0
    if new_tech:
        capex_value = capex_getter_f(switch_capex_ref, greenfield_ref, year, start_tech, new_tech, switch_type)
        actual_capex = capex_value * (capacity_ref[plant_name] * MEGATON_TO_TON)
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
            "region_rmi",
        ]
    ].copy()

    def create_global_stats(df, operation: str = "sum"):
        calc = df.groupby(["year"]).sum()
        if operation == "sum":
            return calc
        if operation == "cumsum":
            return calc.cumsum()

    if results == "global":
        return create_global_stats(df_c, operation).reset_index()

    if results == "regional":
        regions = df_c["region_rmi"].unique()
        region_dict = {}
        for region in regions:
            calc = df_c[df_c["region_rmi"] == region].groupby(["year"]).sum()
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


def get_plant_cycle(plant_cycles: dict, plant_name: str, current_year: int, model_start_year: int, model_end_year: int) -> range:
    """Get a plant's cycle based on the current year.

    Args:
        plant_cycles (dict): A dictionary of each plant's investment cycle.
        plant_name (str): The name of the plant.
        current_year (int): The current year of the model cycle.
        model_start_year (int): The start year of the model range.
        model_end_year (int): The end year of model range.

    Returns:
        range: A range object conatining the a plant's updates cycle from a given start year.
    """
    if (current_year < model_start_year) or (current_year > model_end_year):
        return None
    cycles = plant_cycles[plant_name]
    if len(cycles) == 0:
        return None
    first_year, second_year, third_year = (0, 0, 0)
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


def get_investment_capital_costs(investment_df: pd.DataFrame, investment_cycles: dict, plant_name: str, current_year: int) -> float:
    """Calculates the investment costs within a specified range of years.

    Args:
        investment_df (pd.DataFrame): The investment costs DataFrame.
        investment_cycles (dict): A dictionary of each plant's investment cycle.
        plant_name (str): The name of the plant.
        current_year (int): The current model cycle year.

    Returns:
        float: Get capex costs within a specified range of years.
    """
    range_obj = get_plant_cycle(investment_cycles, plant_name, current_year, MODEL_YEAR_START, MODEL_YEAR_END)
    
    if range_obj:
        return investment_df.iloc[list(range_obj)]['capital_cost'].sum()
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
    active_check_results_dict = read_pickle_folder(
        intermediate_path, "active_check_results_dict", "df"
    )
    plant_result_df = read_pickle_folder(
        intermediate_path, "plant_result_df", "df"
    )
    plant_names = plant_result_df["plant_name"].unique()
    capex_switching_df = read_pickle_folder(PKL_DATA_FORMATTED, "capex_switching_df", "df")
    capex_ref = capex_switching_df.reset_index().set_index(['Year', 'Start Technology', 'New Technology']).sort_index(ascending=True).to_dict()['value']
    capex_dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    greenfield_capex_ref = capex_dict['greenfield'].to_dict()['value']
    plant_capacity_results = read_pickle_folder(
        intermediate_path, "plant_capacity_results", "df"
    )
    plant_country_code_ref = dict(zip(plant_result_df['plant_name'], plant_result_df['country_code']))

    data_container = []
    for year in tqdm(
        MODEL_YEAR_RANGE,
        total=len(MODEL_YEAR_RANGE),
        desc="Creating Steel Plant Investment DataFrame",
    ):
        plant_names = plant_capacity_results[year].keys()
        for plant_name in plant_names:
            if active_check_results_dict[plant_name][year]:
                data_container.append(
                    investment_row_calculator(
                        plant_investment_cycles,
                        capex_ref,
                        greenfield_capex_ref,
                        active_check_results_dict,
                        tech_choice_dict,
                        plant_capacity_results[year],
                        year,
                        plant_name
                    )
                )
    investment_results = (
        pd.DataFrame(data_container).set_index(["year"]).sort_values("year")
    )
    rmi_mapper = create_country_mapper()
    investment_results['country_code'] = investment_results['plant_name'].apply(
        lambda x: plant_country_code_ref[x])
    investment_results['region'] = investment_results['country_code'].apply(
            lambda x: rmi_mapper[x])
    investment_results.reset_index(inplace=True)
    investment_results = map_plant_id_to_df(investment_results, plant_result_df, "plant_name")
    investment_results = add_results_metadata(
        investment_results, scenario_dict, single_line=True, scenario_name=True
    )
    cumulative_investment_results = create_inv_stats(
        investment_results, results="regional", agg=True, operation="cumsum"
    )
    cumulative_investment_results = add_results_metadata(
        cumulative_investment_results, scenario_dict, single_line=True, 
        include_regions=False, scenario_name=True
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
