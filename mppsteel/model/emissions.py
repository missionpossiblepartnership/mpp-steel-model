"""Script to generate Switching tables for TCO and emission abatement"""

import pandas as pd
from tqdm import tqdm

# For logger and units dict
from mppsteel.utility.utils import get_logger, read_pickle_folder, serialize_file, timer_func

from mppsteel.model_config import (
    MODEL_YEAR_END,
    MODEL_YEAR_START,
    PKL_DATA_INTERMEDIATE, PKL_DATA_IMPORTS,
    INVESTMENT_CYCLE_LENGTH,
)

from mppsteel.utility.reference_lists import (
    SWITCH_DICT,
)

# Create logger
logger = get_logger("Emissions")

def get_emissions_by_year(
    df: pd.DataFrame, tech: str, start_year: int = MODEL_YEAR_START, date_span: int = 20
) -> dict:
    """Generates a dictionary of years as keys, and emissions as values.

    Args:
        df (pd.DataFrame): A DataFrame containing emissions.
        tech (str): The technology to subset the DataFrame.
        start_year (int, optional): The start year for the technology. Defaults to ModelYearStart.
        date_span (int, optional): The years that comprise the investment. Defaults to 20.

    Returns:
        dict: A dictionary with the with the years and emissions value for the technology.
    """
    # logger.info(f'--- Getting emissions for {tech} for each year across the relevant range, starting at {start_year}')

    df_c = df.copy()
    df_c = df_c.reorder_levels(["technology", "year"])
    df_c = df_c.loc[tech]
    max_value = df_c.loc[2050]["emissions"]
    year_range = range(start_year, start_year + date_span)

    value_list = []
    for year in year_range:
        if year <= 2050:
            value_list.append(df_c.loc[year]["emissions"])

        if year > 2050:
            value_list.append(max_value)

    return dict(zip(year_range, value_list))


def compare_emissions(
    df: pd.DataFrame,
    base_tech: str,
    comp_tech: str,
    emission_type: str,
    start_year: int = MODEL_YEAR_START,
    date_span: int = 20,
):

    # logger.info(f'--- Comparing emissions for {base_tech} and {comp_tech}')

    base_tech_dict = get_emissions_by_year(df, base_tech, start_year, date_span)
    comp_tech_dict = get_emissions_by_year(df, comp_tech, start_year, date_span)
    years = list(base_tech_dict.keys())
    df = pd.DataFrame(
        data={
            "start_year": start_year,
            "year": years,
            "start_technology": base_tech,
            "end_technology": comp_tech,
            "start_tech_values": base_tech_dict.values(),
            "comp_tech_values": comp_tech_dict.values(),
        }
    )
    df[f"abated_{emission_type}_emissions"] = (
        df["start_tech_values"] - df["comp_tech_values"]
    )
    return df

@timer_func
def calculate_emissions(
    year_end: int = MODEL_YEAR_END, output_type: str = "full", serialize_only: bool = False
) -> pd.DataFrame:
    """Calculates the complete array of technology switch matches to years.

    Args:
        year_end (int, optional): The year that the table should stop calculating. Defaults to 2050.
        output_type (str, optional): Determines whether to return the full DataFrame or a summary. Defaults to 'full'.
        serialize_only (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with the complete iterations of years and technology switches available.
    """

    logger.info(
        f"Calculating emissions for all technologies from 2020 up to {year_end}"
    )

    df_list = []
    df_base_cols = [
        "start_year",
        "year",
        "start_technology",
        "end_technology",
        "start_tech_values",
        "comp_tech_values",
    ]
    s1_emissions = read_pickle_folder(PKL_DATA_INTERMEDIATE, "calculated_s1_emissions", "df")
    s2_emissions = read_pickle_folder(PKL_DATA_INTERMEDIATE, "calculated_s2_emissions", "df")
    s3_emissions = read_pickle_folder(PKL_DATA_INTERMEDIATE, "calculated_s3_emissions", "df")
    year_range = range(MODEL_YEAR_START, year_end + 1)
    for year in tqdm(year_range, total=len(year_range), desc='Emissions'):
        logger.info(f"Calculating technology emissions for {year}")
        for base_tech in SWITCH_DICT.keys():
            for switch_tech in SWITCH_DICT[base_tech]:
                if switch_tech in ["Close plant"]:
                    pass
                else:
                    df_cols = {}
                    emission_dict = {
                        "s1": s1_emissions,
                        "s2": s2_emissions,
                        "s3": s3_emissions,
                    }
                    for item in emission_dict.items():
                        emission_type = item[0]
                        emission_df = item[1]
                        emission_type_col = f"abated_{emission_type}_emissions"
                        df = compare_emissions(
                            emission_df,
                            base_tech,
                            switch_tech,
                            emission_type,
                            year,
                            INVESTMENT_CYCLE_LENGTH,
                        )
                        df_cols[emission_type] = df[emission_type_col]
                    df_base = df[df_base_cols]
                    for key in df_cols.keys():
                        df_base[f"abated_{key}_emissions"] = df_cols[key]
                    df_list.append(df_base)
    full_df = pd.concat(df_list)
    col_list = [
        "start_year",
        "start_technology",
        "end_technology",
        "abated_s1_emissions",
        "abated_s2_emissions",
        "abated_s3_emissions",
    ]
    full_summary_df = (
        full_df[col_list]
        .groupby(by=["start_year", "start_technology", "end_technology"])
        .sum()
    )

    if serialize_only:
        serialize_file(full_summary_df, PKL_DATA_INTERMEDIATE, "emissions_switching_df_summary")
        serialize_file(full_df, PKL_DATA_INTERMEDIATE, "emissions_switching_df_full")
        return

    if output_type == "summary":
        return full_summary_df
    return full_df
