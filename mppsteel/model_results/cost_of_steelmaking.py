"""Calculation Functions used to derive various forms of Cost of Steelmaking."""
import pandas as pd
import numpy_financial as npf

from tqdm import tqdm

from mppsteel.data_preprocessing.levelized_cost import calculate_cc
from mppsteel.model_results.investments import get_investment_capital_costs

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder, serialize_file, get_scenario_pkl_path
)
from mppsteel.utility.log_utility import get_logger
from mppsteel.config.model_config import (
    MODEL_YEAR_RANGE,
    PKL_DATA_FORMATTED,
    DISCOUNT_RATE,
    INVESTMENT_CYCLE_DURATION_YEARS,
)

logger = get_logger(__name__)

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
    main_dict: dict, reference_dict: dict = None, ref_key: str = None
) -> float:
    """Extracts dictionary values based on function arguments passed (multi-nested dict). final values must be numerical.
    Args:
        main_dict (dict): The main dictionary you want to extract values from.
        reference_dict (dict, optional): A reference dictionary containing metadata about the keys in main_dict. Defaults to None.
        ref_key (str, optional): A reference key for the reference_dict. Defaults to None.

    Returns:
        float: A summation of the values in the dictionary.
    """
    if reference_dict and ref_key:
        ref_list = reference_dict[ref_key]
        return sum(
            [
                main_dict[key]
                for key in main_dict
                if key in ref_list
            ]
        )
    return sum([main_dict[key] for key in main_dict])


def apply_cos(
    row,
    year: int,
    cap_dict: dict,
    variable_costs_ref: pd.DataFrame,
    capex_ref: dict,
    investment_cost_ref: dict,
    production_ref: pd.DataFrame,
    relining_span_ref: dict,
    capital_charges: bool,
) -> float:
    """Applies the Cost of Steelmaking function to a given row in a DataFrame.

    Args:
        row (_type_): A vectorized DataFrame row from .apply function.
        year (int): The current year.
        cap_dict (dict): A DataFrame containing the steel plant metadata.
        v_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_costs (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        production_df (pd.DataFrame): A DataFrame containing the production values.
        steel_scenario (str): A string containing the scenario to be used in the steel.
        capital_charges (bool): A boolean flag to toggle the capital charges function.

    Returns:
        float: The cost of Steelmaking value to be applied.
    """

    plant_capacity = cap_dict[row.plant_name]
    variable_cost, other_opex_cost, capital_investment = (0, 0, 0)
    if row.technology:
        variable_cost = variable_costs_ref[(year, row.country_code, row.technology)]
        other_opex_cost = capex_ref["other_opex"][(year, row.technology)]
        capital_investment = investment_cost_ref[(year, row.plant_name)]
    discount_rate = DISCOUNT_RATE
    relining_year_span = relining_span_ref[row.plant_name]

    relining_cost = 0

    if capital_charges and row.technology:
        relining_cost = calculate_cc(
            capex_ref['brownfield'],
            year,
            relining_year_span,
            row.technology,
            discount_rate
        )

    if row.capacity_utilization == 0:
        result_1 = 0

    else:
        result_1 = plant_capacity * (
            (variable_cost * row.capacity_utilization) + other_opex_cost + relining_cost
        )

    if not capital_charges:
        return result_1

    production_value = production_ref[(year, row.plant_name)]
    if production_value == 0:
        result_2 = 0
    else:
        result_2 = npf.pmt(discount_rate, relining_year_span, capital_investment) / production_value

    return result_1 - result_2


def cost_of_steelmaking(
    production_df: pd.DataFrame,
    production_ref: dict,
    variable_costs_ref: pd.DataFrame,
    capex_ref: pd.DataFrame,
    capacities_dict: dict,
    investment_cost_ref: dict,
    relining_span_ref: dict,
    cols_to_keep: list,
    region_group: str = "region_rmi",
    regional: bool = False,
    capital_charges: bool = False,
) -> dict:
    """Applies the cost of steelmaking function to the Production Stats DataFrame.

    Args:
        production_stats (pd.DataFrame): A DataFrame containing the Production Stats.
        variable_costs (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_df (pd.DataFrame): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        capacities_dict (dict): A dictionary containing the initial capacities of each plant.
        investment_df
        region_group (str, optional): Determines which regional schema to use if the `regional` flag is set to `True`. Defaults to "region_rmi".
        regional (bool, optional): Boolean flag to determine whether to calculate the Cost of Steelmaking at the regional level or the global level. Defaults to False.
        capital_charges (bool): A boolean flag to toggle the capital charges function. Defaults to False.

    Returns:
        dict: A dictionary containing each year and the Cost of Steelmaking values.
    """
    plant_region_ref = create_region_plant_ref(production_df, region_group)
    regions = production_df[region_group].unique()
    years = production_df["year"].unique()
    production_stats_modified = production_df[cols_to_keep].set_index("year").copy()
    
    cos_year_list = []
    def calculate_cos(df, ref=None) -> float:
        df_c = df.copy()
        cos_values = df_c.apply(
            apply_cos,
            year=year,
            cap_dict=capacities_dict[year],
            variable_costs_ref=variable_costs_ref,
            capex_ref=capex_ref,
            investment_cost_ref=investment_cost_ref,
            production_ref=production_ref,
            relining_span_ref=relining_span_ref,
            capital_charges=capital_charges,
            axis=1,
        )
        cos_sum = cos_values.sum()
        capacity_sum = extract_dict_values(
            capacities_dict, plant_region_ref, ref
        )
        return cos_sum / capacity_sum
    desc = "Cost of Steelmaking without Captial Charges: Year Loop"
    if capital_charges:
        desc = "Cost of Steelmaking with Captial Charges: Year Loop"

    for year in tqdm(years, total=len(years), desc=desc):
        ps_y = production_stats_modified.loc[year]

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
    steel_plant_df: pd.DataFrame,
    production_df: pd.DataFrame,
    variable_costs_df: pd.DataFrame,
    investment_df: pd.DataFrame,
    capex_ref: dict,
    capacities_ref: dict,
    investment_cycle_lengths: dict,
    full_investment_cycles: dict,
    region_group: str,
) -> pd.DataFrame:
    """Generates a DataFrame containing two value columns: one with standard cost of steelmaking, and cost of steelmaking with capital charges.
    Args:
        production_df (pd.DataFrame): A DataFrame containing the Production Stats.
        variable_costs_df (pd.DataFrame): A DataFrame containing the variable costs for each technology across each year and region.
        capex_ref (dict): A dictionary containing the Capex values for Greenfield, Brownfield and Other Opex values.
        capacities_ref (dict): A dictionary containing the initial capacities of each plant.
        demand_scenario (str): A string containing the scenario to be used in the steel. Defaults to "bau".
        region_group (str, optional): Determines which regional schema to use if the `regional` flag is set to `True`. Defaults to "region_rmi".

    Returns:
        pd.DataFrame: A DataFrame containing the new columns.
    """

    variable_cost_ref = variable_costs_df.reset_index().set_index(['year', 'country_code', 'technology']).to_dict()['cost']
    brownfield_capex_ref = capex_ref['brownfield'].reset_index().set_index(['Year', 'Technology']).to_dict()['value']
    greenfield_capex_ref = capex_ref['greenfield'].reset_index().set_index(['Year', 'Technology']).to_dict()['value']
    other_opex_ref = capex_ref['other_opex'].reset_index().set_index(['Year', 'Technology']).to_dict()['value']

    combined_capex_ref = {
        'brownfield': brownfield_capex_ref,
        'greenfield': greenfield_capex_ref,
        'other_opex': other_opex_ref
    }

    investment_dict_result_cycles_only = {
        key: [val for val in values if isinstance(val, int)] for key, values in full_investment_cycles.items()
        }

    investment_df.set_index(['year'], inplace=True)

    investment_cost_ref = {}
    production_ref = production_df[['year', 'plant_name', 'production']].set_index(['year', 'plant_name']).to_dict()['production']
    for year in tqdm(MODEL_YEAR_RANGE, total=len(MODEL_YEAR_RANGE), desc='COS Investment Reference Loop'):
        for plant_name in capacities_ref[year].keys():
            investment_cost_ref[(year, plant_name)] = get_investment_capital_costs(
                investment_df, investment_dict_result_cycles_only, plant_name, year)

    relining_span_ref = {}
    plant_names = steel_plant_df['plant_name'].unique()
    for plant_name in tqdm(plant_names, total=len(plant_names), desc='Relining Year Span Loop'):
        relining_span_ref[plant_name] = investment_cycle_lengths.get(plant_name, INVESTMENT_CYCLE_DURATION_YEARS)

    cols_to_keep = [
        "year",
        "plant_name",
        "country_code",
        "technology",
        "capacity",
        "production",
        "capacity_utilization",
        region_group
    ]

    standard_cos = cost_of_steelmaking(
        production_df,
        production_ref,
        variable_cost_ref,
        combined_capex_ref,
        capacities_ref,
        investment_cost_ref,
        relining_span_ref,
        cols_to_keep,
        region_group,
        regional=True,
    )
    cc_cos = cost_of_steelmaking(
        production_df,
        production_ref,
        variable_cost_ref,
        combined_capex_ref,
        capacities_ref,
        investment_cost_ref,
        relining_span_ref,
        cols_to_keep,
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
    intermediate_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'intermediate')
    final_path = get_scenario_pkl_path(scenario_dict['scenario_name'], 'final')
    variable_costs_regional = read_pickle_folder(
        intermediate_path, "variable_costs_regional", "df"
    )
    capex_dict = read_pickle_folder(PKL_DATA_FORMATTED, "capex_dict", "df")
    
    production_resource_usage = read_pickle_folder(
        final_path, "production_resource_usage", "df"
    )
    plant_result_df = read_pickle_folder(
        intermediate_path, "plant_result_df", "df"
    )
    plant_capacity_results = read_pickle_folder(intermediate_path, "plant_capacity_results", "df")
    investment_results = read_pickle_folder(
        final_path, "investment_results", "df"
    )
    investment_dict_result = read_pickle_folder(
        intermediate_path, "investment_dict_result", "df"
    )
    plant_cycle_length_mapper_result = read_pickle_folder(
        intermediate_path, "plant_cycle_length_mapper_result", "df"
    )
    
    cos_data = create_cost_of_steelmaking_data(
        plant_result_df,
        production_resource_usage,
        variable_costs_regional,
        investment_results,
        capex_dict,
        plant_capacity_results,
        plant_cycle_length_mapper_result,
        investment_dict_result,
        "region_rmi",
    )

    if serialize:
        logger.info("-- Serializing dataframes")
        serialize_file(cos_data, final_path, "cost_of_steelmaking")
    return cos_data
