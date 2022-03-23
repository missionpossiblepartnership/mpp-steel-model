"""Calculation Functions used to derive various forms of Cost of Steelmaking."""

import pandas as pd
import numpy_financial as npf

from tqdm import tqdm

from mppsteel.data_loading.steel_plant_formatter import create_plant_capacities_dict
from mppsteel.data_loading.reg_steel_demand_formatter import steel_demand_getter
from mppsteel.model.levelized_cost import calculate_cc

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    PKL_DATA_FINAL,
    PKL_DATA_INTERMEDIATE,
    DISCOUNT_RATE,
    INVESTMENT_CYCLE_DURATION_YEARS,
)

logger = get_logger("Cost of Steelmaking")


def create_region_plant_ref(df: pd.DataFrame, region_string: str) -> dict:
    """Creates a mapping of plants to a region(s) of interest.

    Args:
        df (pd.DataFrame): The Plant DataFrame.
        region_string (str): The region(s) you want to map.

    Returns:
        dict: A dictionary containing a mapping of region to plant names
    """
    return {region: list(
            df[df[region_string] == region]["plant_name"].unique()
        ) for region in df[region_string].unique()}


def extract_dict_values(
    main_dict: dict, key_to_extract: str, reference_dict: dict = None, ref_key: str = None
) -> float:
    """Extracts dictionary values based on function arguments passed (multi-nested dict). final values must be numerical.
    Args:
        main_dict (dict): The main dictionary you want to extract values from.
        key_to_extract (str): The specific dictionary key you want to extract values for (when dictionary has multiple levels).
        reference_dict (dict, optional): A reference dictionary containing metadata about the keys in main_dict. Defaults to None.
        ref_key (str, optional): A reference key for the reference_dict. Defaults to None.

    Returns:
        float: A summation of the values in the dictionary.
    """
    if reference_dict and ref_key:
        ref_list = reference_dict[ref_key]
        return sum(
            [
                main_dict[key][key_to_extract]
                for key in main_dict
                if key in ref_list
            ]
        )
    return sum([main_dict[key][key_to_extract] for key in main_dict])


def apply_cos(
    row,
    year: int,
    cap_dict: dict,
    v_costs: pd.DataFrame,
    capex_costs: dict,
    steel_demand: pd.DataFrame,
    steel_scenario: str,
    capital_charges: bool,
) -> float:
    """Applies the Cost of Steelmaking function to a given row in a DataFrame.

    Args:
        row (_type_): A vectorized DataFrame row from .apply function.
        year (int): The current year.
        cap_dict (dict): A DataFrame containing the steel plant metadata.
        v_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_costs (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        steel_demand (pd.DataFrame): A DataFrame containing the steel demand value timeseries.
        steel_scenario (str): A string containing the scenario to be used in the steel.
        capital_charges (bool): A boolean flag to toggle the capital charges function.

    Returns:
        float: The cost of Steelmaking value to be applied.
    """

    primary_capacity = cap_dict[row.plant_name]["primary_capacity"]
    secondary_capacity = cap_dict[row.plant_name]["secondary_capacity"]
    variable_cost = 0
    other_opex_cost = 0
    gf_value = 0
    if row.technology:
        variable_cost = v_costs.loc[row.country_code, year, row.technology]["cost"]
        other_opex_cost = capex_costs["other_opex"].loc[row.technology, year]["value"]
        gf_value = capex_costs["greenfield"].loc[row.technology, year]["value"]
    steel_demand_value = steel_demand_getter(
        steel_demand, year, steel_scenario, "crude", region="World"
    )
    discount_rate = DISCOUNT_RATE
    relining_year_span = INVESTMENT_CYCLE_DURATION_YEARS

    # cuf = steel_demand_value / row.capacity
    relining_cost = 0

    if capital_charges and row.technology:
        relining_cost = calculate_cc(
            capex_costs,
            year,
            relining_year_span,
            row.technology,
            discount_rate,
            "brownfield",
        )

    result_1 = (primary_capacity + secondary_capacity) * (
        (variable_cost * row.capacity_utilization) + other_opex_cost + relining_cost
    )

    if not capital_charges:
        return result_1

    result_2 = npf.pmt(discount_rate, relining_year_span, gf_value) / steel_demand_value

    return result_1 - result_2


def cost_of_steelmaking(
    production_stats: pd.DataFrame,
    variable_costs: pd.DataFrame,
    capex_df: pd.DataFrame,
    steel_demand: pd.DataFrame,
    capacities_dict: dict,
    steel_scenario: str = "bau",
    region_group: str = "region_wsa_region",
    regional: bool = False,
    capital_charges: bool = False,
) -> dict:
    """Applies the cost of steelmaking function to the Production Stats DataFrame.

    Args:
        production_stats (pd.DataFrame): A DataFrame containing the Production Stats.
        variable_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_df (pd.DataFrame): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        steel_demand (pd.DataFrame): A DataFrame containing the steel demand value timeseries.
        capacities_dict (dict): A dictionary containing the initial capacities of each plant.
        steel_scenario (str): A string containing the scenario to be used in the steel. Defaults to "bau".
        region_group (str, optional): Determines which regional schema to use if the `regional` flag is set to `True`. Defaults to "region_wsa_region".
        regional (bool, optional): Boolean flag to determine whether to calculate the Cost of Steelmaking at the regional level or the global level. Defaults to False.
        capital_charges (bool): A boolean flag to toggle the capital charges function. Defaults to False.

    Returns:
        dict: A dictionary containing each year and the Cost of Steelmaking values.
    """
        

    regions = production_stats[region_group].unique()
    years = production_stats["year"].unique()
    cols_to_keep = [
        "year",
        "plant_name",
        "country_code",
        "technology",
        "capacity",
        "production",
        "capacity_utilization",
        "region_wsa_region",
        "region_continent",
        "region_region",
    ]
    production_stats = production_stats[cols_to_keep].set_index("year").copy()
    plant_region_ref = create_region_plant_ref(production_stats, region_group)
    cos_year_list = []

    def calculate_cos(df, ref=None) -> float:
        df_c = df.copy()
        cos_values = df_c.apply(
            apply_cos,
            year=year,
            cap_dict=capacities_dict,
            v_costs=variable_costs,
            capex_costs=capex_df,
            steel_demand=steel_demand,
            steel_scenario=steel_scenario,
            capital_charges=capital_charges,
            axis=1,
        )
        cos_sum = cos_values.sum()
        primary_sum = extract_dict_values(
            capacities_dict, "primary_capacity", plant_region_ref, ref
        )
        secondary_sum = extract_dict_values(
            capacities_dict, "secondary_capacity", plant_region_ref, ref
        )
        return cos_sum / (primary_sum + secondary_sum)

    for year in tqdm(years, total=len(years), desc="Cost of Steelmaking: Year Loop"):
        ps_y = production_stats.loc[year]

        if regional:
            ps_y = ps_y.set_index(region_group)
            region_dict = {}
            for region in regions:
                ps_r = ps_y.loc[region]
                cos_r = calculate_cos(ps_r, region)
                region_dict[region] = cos_r
            cos_year_list.append(region_dict)

        else:
            cos_final = calculate_cos(ps_y)
            cos_year_list.append(cos_final)
    return dict(zip(years, cos_year_list))


def dict_to_df(df_values_dict: dict, region_group: str, cc: bool = False) -> pd.DataFrame:
    """Turns a dictionary of of cost of steelmaking values into a DataFrame.

    Args:
        df_values_dict (dict): A dictionary containing each year and the Cost of Steelmaking values.
        region_group (str): Determines which regional schema to use.
        cc (bool, optional): Determines whether the cost of steelmaking column will include a reference to the capital charges. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the Cost of Steelmaking values.
    """
    value_col = "cost_of_steelmaking"
    if cc:
        value_col = f"{value_col}_with_cc"
    df_c = pd.DataFrame(df_values_dict).transpose().copy()
    df_c = df_c.reset_index().rename(mapper={"index": "year"}, axis=1)
    df_c = df_c.melt(id_vars=["year"], var_name=region_group, value_name=value_col)
    return df_c.set_index(["year", region_group]).sort_values(
        by=["year", region_group], axis=0
    )


def create_cost_of_steelmaking_data(
    production_df: pd.DataFrame,
    variable_costs_df: pd.DataFrame,
    capex_ref: dict,
    steel_demand_df: pd.DataFrame,
    capacities_ref: dict,
    demand_scenario: str,
    region_group: str,
) -> pd.DataFrame:
    """Generates a DataFrame containing two value columns: one with standard cost of steelmaking, and cost of steelmaking with capital charges.
    Args:
        production_df (pd.DataFrame): A DataFrame containing the Production Stats.
        variable_costs_df (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_ref (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        steel_demand_df (pd.DataFrame): A DataFrame containing the steel demand value timeseries.
        capacities_ref (dict): A dictionary containing the initial capacities of each plant.
        demand_scenario (str): A string containing the scenario to be used in the steel. Defaults to "bau".
        region_group (str, optional): Determines which regional schema to use if the `regional` flag is set to `True`. Defaults to "region_wsa_region".

    Returns:
        pd.DataFrame: A DataFrame containing the new columns.
    """

    standard_cos = cost_of_steelmaking(
        production_df,
        variable_costs_df,
        capex_ref,
        steel_demand_df,
        capacities_ref,
        demand_scenario,
        region_group,
        regional=True,
    )
    cc_cos = cost_of_steelmaking(
        production_df,
        variable_costs_df,
        capex_ref,
        steel_demand_df,
        capacities_ref,
        demand_scenario,
        region_group,
        regional=True,
        capital_charges=True,
    )
    cc_cos_d = dict_to_df(cc_cos, region_group, True)
    standard_cos_d = dict_to_df(standard_cos, region_group, False)
    return standard_cos_d.join(cc_cos_d)


@timer_func
def generate_cost_of_steelmaking_results(scenario_dict: dict, serialize: bool = False) -> dict:
    """Full flow to create the Cost of Steelmaking and the Levelized Cost of Steelmaking DataFrames.

    Args:
        scenario_dict (dict): A dictionary with scenarios key value mappings from the current model execution.
        serialize (bool, optional): Flag to only serialize the dict to a pickle file and not return a dict. Defaults to False.

    Returns:
        dict: A dictionary with the Cost of Steelmaking DataFrame and the Levelized Cost of Steelmaking DataFrame.
    """
    variable_costs_regional = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "variable_costs_regional", "df"
    )
    capex_dict = read_pickle_folder(PKL_DATA_INTERMEDIATE, "capex_dict", "df")
    steel_demand_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "regional_steel_demand_formatted", "df"
    )
    production_resource_usage = read_pickle_folder(
        PKL_DATA_FINAL, "production_resource_usage", "df"
    )
    plant_result_df = read_pickle_folder(
        PKL_DATA_INTERMEDIATE, "plant_result_df", "df"
    )
    capacities_dict = create_plant_capacities_dict(plant_result_df)
    steel_demand_scenario = scenario_dict["steel_demand_scenario"]

    cos_data = create_cost_of_steelmaking_data(
        production_resource_usage,
        variable_costs_regional,
        capex_dict,
        steel_demand_df,
        capacities_dict,
        steel_demand_scenario,
        "region_wsa_region",
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(cos_data, PKL_DATA_FINAL, "cost_of_steelmaking")
    return cos_data
