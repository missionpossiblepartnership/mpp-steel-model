"""Script to test the business cases"""

import pandas as pd
import numpy as np

from tqdm import tqdm
from tqdm.auto import tqdm as tqdma

# For logger
from mppsteel.utility.utils import enumerate_iterable
from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import (
    read_pickle_folder,
    serialize_file,
    extract_data,
)

from mppsteel.utility.log_utility import get_logger

from mppsteel.config.model_config import (
    IMPORT_DATA_PATH,
    PKL_DATA_FORMATTED,
    PKL_DATA_IMPORTS
)

from mppsteel.config.reference_lists import (
    TECH_REFERENCE_LIST,
    FURNACE_GROUP_DICT,
    TECHNOLOGY_PROCESSES,
    bosc_factor_group,
    eaf_factor_group,
    electricity_and_steam_self_gen_group,
    electricity_self_gen_group,
    HARD_CODED_FACTORS,
)

from mppsteel.data_loading.standardise_business_cases import (
    full_model_flow,
    create_hardcoded_exceptions,
    business_case_formatter_splitter,
    create_production_factors,
    furnace_group_from_tech,
)

# Create logger
logger = get_logger("Business Case Tests")

def create_full_process_summary(bc_process_df: pd.DataFrame) -> pd.DataFrame:
    """A DataFrame containing the fully processed business cases for each technology.

    Args:
        bc_process_df (pd.DataFrame): A DataFrame containing the processes contained in the business cases.

    Returns:
        pd.DataFrame: A DataFrame containing the combined process summaries for each Technology.
    """
    return_strings = lambda x: [y for y in x if isinstance(y, str)]
    # Create a list of materials as a reference
    material_list = return_strings(bc_process_df["material_category"].unique())
    main_container = []
    # Loop through each technology and material
    for technology in tqdm(
        TECH_REFERENCE_LIST,
        total=len(TECH_REFERENCE_LIST),
        desc="Business Case Full Summary",
    ):
        for material_ref in material_list:
            if material_ref in return_strings(
                bc_process_df[bc_process_df["technology"] == technology][
                    "material_category"
                ].unique()
            ):
                throw, keep = full_model_flow(technology, material_ref)
                main_container.append(keep)
    return pd.concat(main_container).reset_index(drop=True)


def master_getter(df: pd.DataFrame, materials_ref: list, tech: str, material: str, rounding: int = 3) -> float:
    """A getter function for the master reference technology process summary DataFrame.

    Args:
        df (pd.DataFrame): The preprocessed Process summary DataFrame.
        materials_ref (list): A list of potential materials as a reference.
        tech (str): The technology to reference
        material (str): The material to reference.
        rounding (int, optional): The figure to round DataFrame values. Defaults to 3.

    Returns:
        float: The value based on the parameters inputted to the function.
    """
    if material in materials_ref:
        return round(df.loc[tech, material].values[0], rounding)
    return 0


def process_inspector(
    df: pd.DataFrame, excel_bc_summary: pd.DataFrame, rounding: int = 3
) -> pd.DataFrame:
    """Compares a DataFrame to the Excel Business Case Summaries.

    Args:
        df (pd.DataFrame): The DataFrame containing the fully processed business cases.
        excel_bc_summary (pd.DataFrame): The reference sheet that will be compared against a `df`.
        rounding (int, optional): The figure to round DataFrame values. Defaults to 3.

    Returns:
        pd.DataFrame: A DataFrame containing the columns that verify matches against a reference DataFrame.
    """
    df_c = df.copy()
    # Create empty column references.
    df_c["ref_value"] = ""
    df_c["matches_ref"] = ""
    
    # Create a reference of materials.
    materials_ref = excel_bc_summary.index.get_level_values(1).unique()

    def value_mapper(row, enum_dict) -> pd.DataFrame:
        ref_value = master_getter(
            excel_bc_summary,
            materials_ref,
            row[enum_dict["technology"]],
            row[enum_dict["material"]],
            rounding,
        )
        calculated_value = round(row[enum_dict["value"]], rounding)
        row[enum_dict["matches_ref"]] = 1 if calculated_value == ref_value else 0
        row[enum_dict["ref_value"]] = ref_value
        return row

    tqdma.pandas(desc="Process Inspector")
    enumerated_cols = enumerate_iterable(df_c.columns)

    # Apply the vectorized checker functions 
    df_c = df_c.progress_apply(
        value_mapper, enum_dict=enumerated_cols, axis=1, raw=True
    )
    return df_c


def inspector_getter(df: pd.DataFrame, technology: str, material: str = None, process: str = None) -> pd.DataFrame:
    """A getter function for the Process Inspector.

    Args:
        df (pd.DataFrame): The DataFrame generated from the `Process Inspector` function
        technology (str): The technology you want to subset.
        material (str, optional): The material you want to subset. Defaults to None.
        process (str, optional): The process you want to subset. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the values based on the parameters entered.
    """
    row_order = ["process_factor_value", "value", "ref_value", "matches_ref"]
    df_c = df.copy()
    df_c.set_index(["technology", "material", "stage", "process"], inplace=True)
    if material:
        return df_c.xs(key=(technology, material), level=["technology", "material"])[
            row_order
        ]
    if process:
        return df_c.xs(key=(technology, process), level=["technology", "process"])[
            row_order
        ]


def get_summary_dict_from_idf(
    df: pd.DataFrame, technology: str, material: str, function_order: list = None, rounding: int = 3
) -> pd.DataFrame:
    """Creates a summary the Inspector DataFrame created from 

    Args:
        df (pd.DataFrame): A DataFrame based on the Inspector DataFrame created by `Process Inspector`.
        technology (str): The technology you would like to subset.
        material (str): The material you would like to subset.
        function_order (list, optional): The order of the process function formatters you would like to use to sort the values. Defaults to None.
        rounding (int, optional): The figure to round DataFrame values. Defaults to 3.

    Returns:
        pd.DataFrame: The DataFrame you would like to subset for.
    """
    if not function_order:
        function_order = [
            "Initial Creation",
            "Limestone Editor",
            "CCS",
            "CCU",
            "Self Gen",
        ]
    df_c = (
        df.set_index(["technology", "material"])
        .xs((technology, material))
        .reset_index()
        .sort_index()
    )
    return (
        df_c.groupby(["technology", "material", "stage"])
        .agg("sum")["value"]
        .droplevel(["technology", "material"])
        .round(rounding)
        .reindex(function_order)
        .to_dict()
    )


def all_process_values(dfi: pd.DataFrame, tech: str, material: str, rounding: int = 3) -> dict:
    """Creates a reference dictionary that summaries the material usage for all processes for a specified technology.

    Args:
        dfi (pd.DataFrame): A DataFrame created by the `Process Inspector` function.
        tech (str): The technology you want to summarise.
        material (str): The material you want to summarise.
        rounding (int, optional): The figure to round DataFrame values. Defaults to 3.

    Returns:
        dict: A dictionary containing the processes and values you want to summarise.
    """
    df = inspector_getter(dfi, tech, material).copy()["value"]
    return {
        stage: df.loc[stage].round(rounding).to_dict()
        for stage in df.index.get_level_values(0).unique()
    }


def inspector_df_flow(bc_master_df: pd.DataFrame) -> pd.DataFrame:
    """Creates the process inspector DataFrame from the raw business cases import and the master reference inspector DataFrame.

    Args:
        bc_master_df (pd.DataFrame): [description]

    Returns:
        pd.DataFrame: [description]
    """
    logger.info("Running all model flows")
    bc_master_c = bc_master_df.copy()
    business_cases = read_pickle_folder(PKL_DATA_IMPORTS, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    create_full_process_summary_df = create_full_process_summary(bc_processes)
    return process_inspector(create_full_process_summary_df, bc_master_c)


def check_matches(
    i_df: pd.DataFrame,
    materials_ref: list,
    technology: str,
    file_obj = None,
    rounding: int = 3,
) -> None:
    """Checks matches from the Process Inspector DataFrame as summarised them in a written file based on `file_obj`.

    Args:
        i_df (pd.DataFrame): The Process Inspector DataFrame.
        materials_ref (list): The materials reference list.
        technology (str): Th technology you want to create the summary for.
        file_obj ([type], optional): The file object you want to write the outputs to. Will not write to file if nothing is passed. Defaults to None.
        rounding (int, optional): The figure to round DataFrame values. Defaults to 3.
    """
    logger.info(f"-- Printing results for {technology}")
    process_prod_factor_mapper = create_production_factors(
        technology, FURNACE_GROUP_DICT, HARD_CODED_FACTORS
    )
    furnace_group = furnace_group_from_tech(FURNACE_GROUP_DICT)[technology]
    materials_list = materials_ref[:-4].copy()
    tech_processes = TECHNOLOGY_PROCESSES[technology].copy()
    hard_coded_exception_check = False
    bosc_factor_group_check = False
    eaf_factor_group_check = False
    electricity_and_steam_self_gen_group_check = False
    electricity_self_gen_group_check = False

    if technology in create_hardcoded_exceptions(
        HARD_CODED_FACTORS, FURNACE_GROUP_DICT
    ):
        hard_coded_exception_check = True
    if technology in bosc_factor_group:
        bosc_factor_group_check = True
    if technology in eaf_factor_group:
        eaf_factor_group_check = True
    if technology in electricity_and_steam_self_gen_group:
        electricity_and_steam_self_gen_group_check = True
    if technology in electricity_self_gen_group:
        electricity_self_gen_group_check = True

    pretty_dict_flow = lambda dict_obj: " -> ".join(
        [f"{step}: {value}" for step, value in dict_obj.items()]
    )

    if file_obj:

        def write_line_to_file(line, file_obj=file_obj):
            file_obj.write(f"\n{line}")

    write_line_to_file(
        f"============================ RESULTS FOR {technology} ============================"
    )
    write_line_to_file("")
    write_line_to_file(f"Furnace Group: {furnace_group}")
    write_line_to_file(
        f"hard_coded_exception_check: {hard_coded_exception_check or False}"
    )
    write_line_to_file(f"bosc_factor_group_check: {bosc_factor_group_check or False}")
    write_line_to_file(f"eaf_factor_group_check: {eaf_factor_group_check or False}")
    write_line_to_file(
        f"electricity_and_steam_self_gen_group_check: {electricity_and_steam_self_gen_group_check or False}"
    )
    write_line_to_file(
        f"electricity_self_gen_group_check: {electricity_self_gen_group_check or False}"
    )
    write_line_to_file("")

    # iterate over every material

    for process in tech_processes:
        write_line_to_file(f"============== {process} results ==============")
        process_factor = round(process_prod_factor_mapper[process], rounding)
        write_line_to_file(f"Factor: {process_factor}")
        write_line_to_file("-------------------")
        df_i = inspector_getter(i_df, technology, process=process).copy()

        # iterate over every material
        for material in df_i.index.get_level_values(0).unique():
            write_line_to_file(f"Material: {material}")
            write_line_to_file("")

            df_im = df_i.xs(key=material, level="material").copy()
            value_match_array = df_im["matches_ref"].values
            stage_index = df_im.index
            ref_value = round(df_im.ref_value.values[0], rounding)
            values = df_im.value.values.round(rounding)
            stage_values = dict(zip(stage_index, values))
            stage_values_string = pretty_dict_flow(stage_values)
            summary_values = get_summary_dict_from_idf(
                i_df, technology, material, rounding=rounding
            )
            summary_values_flow = pretty_dict_flow(summary_values)
            self_gen_summary_value = summary_values["Self Gen"]

            all_values = all_process_values(i_df, technology, material, rounding)
            av_keys = all_values.keys()
            summ_dict = {}
            for key in av_keys:
                summ_dict[key] = round(sum(all_values[key].values()), rounding)

            write_line_to_file("Summary values across processes")
            write_line_to_file(f"Total usage (Excel Version): {ref_value}")
            write_line_to_file(
                f"Total usage (Python Version): {self_gen_summary_value}"
            )
            write_line_to_file(summ_dict)

            write_line_to_file("")
            if self_gen_summary_value == ref_value:
                write_line_to_file(f"Final value matches excel version")
                write_line_to_file(f"ACTION: Do Nothing")

            else:
                for stage, val in summary_values.items():
                    if val == ref_value:
                        write_line_to_file("*** INSIGHT: Match Overwritten ***")
                        write_line_to_file(
                            f"Initial match at {stage} but overwritten later. Follow the stack below."
                        )
                        write_line_to_file(f"Aggregate stack: {summary_values_flow}")
                        write_line_to_file("")

                check_equality = lambda it: all(x == it[0] for x in it)
                stage_vals = list(stage_values.values())
                summary_vals = list(summary_values.values())

                write_line_to_file("")
                if check_equality(summary_vals):
                    write_line_to_file(
                        f"No differences amongst total material consumption for each stage."
                    )
                else:
                    write_line_to_file(
                        f"There are differences amongst total material consumption for each stage."
                    )
                    write_line_to_file(f"ACTION: Check the calculations.")
                    write_line_to_file(f"Aggregate stack: {summary_values_flow}")

                write_line_to_file("")
                if check_equality(stage_vals):
                    write_line_to_file(f"No differences amongst stages for {process}")
                    write_line_to_file(
                        f"ACTION: Check process factor value ({process_factor})."
                    )
                else:
                    write_line_to_file(
                        f"There are differences amongst total material consumption within {process}. Follow the stack below."
                    )
                    write_line_to_file(
                        f"Check the calculations sources of discrepancy."
                    )
                    write_line_to_file(f"Process Stack: {stage_values_string}")

                    # case 2: Pairwise checks: Finding the stage where the discrepancy starts
                    index_track = 0
                    for value in range(value_match_array.size - 1):
                        subarray = np.array(
                            value_match_array[index_track : index_track + 2]
                        )
                        if np.array_equal(subarray, np.array([1, 0])):
                            second_stage = stage_index[index_track + 1]
                            write_line_to_file(
                                f"*** INSIGHT: Problem function found ***"
                            )
                            write_line_to_file(f"Fix this function -> {second_stage}")
                        index_track += 1

                    # case 3: Pairwise checks: Fixes that must be later reversed
                    index_track = 0
                    for value in range(value_match_array.size - 1):
                        subarray = np.array(
                            value_match_array[index_track : index_track + 2]
                        )
                        if np.array_equal(subarray, np.array([0, 1])):
                            second_stage = stage_index[index_track + 1]
                            write_line_to_file(
                                f"*** INSIGHT: THIS FUNCTION DOES THE RIGHT THING: {second_stage} ***"
                            )
                            write_line_to_file(
                                f"-- Stage calculations: {stage_values_string}"
                            )
                        index_track += 1
                write_line_to_file("")
                write_line_to_file(all_values)

                write_line_to_file("")
            write_line_to_file("-------------------")
    write_line_to_file(
        f"============================ END OF RESULTS ============================"
    )


def write_tech_report_to_file(
    df_i: pd.DataFrame, materials_ref: pd.Index, technology: str, folder_path: str
) -> None:
    """Writes a singular technology report from the `check matches DataFrame to file.

    Args:
        df_i (pd.DataFrame): The Process Inspector DataFrame.
        materials_ref (pd.Index): The list of materials you want to run checks for.
        technology (str): The technology you want to create the match report for.
        folder_path (str): The folder path you want to save the report text file to.
    """
    logger.info(f"-- {technology} test")
    file_path = f"{folder_path}/{technology}.txt"
    with open(file_path, "w", encoding="utf-8") as f:
        check_matches(df_i, materials_ref, technology, file_obj=f)


@timer_func
def create_bc_test_df(serialize: bool) -> None:
    """Creates a `Process Inspector` DataFrame for use in completing checks.

    Args:
        serialize (bool): A flag to serialize the Process Inspector Dataframe to a pickle file.
    """
    logger.info("Creating business case tests")
    if serialize:
        business_case_master = extract_data(
            IMPORT_DATA_PATH, "Business Cases Excel Master", "csv"
        )
        bc_master = (
            business_case_master.drop(labels=["Type of metric", "Unit"], axis=1)
            .melt(id_vars=["Material"], var_name="Technology")
            .set_index(["Technology", "Material"])
            .copy()
        )
        df_inspector = inspector_df_flow(bc_master)
        serialize_file(df_inspector, PKL_DATA_FORMATTED, "business_case_test_df")


def test_all_technology_business_cases(folder_path: str) -> None:
    """Runs a test for all technology business cases by created the `Process Inspector` DataFrame
    Writes all tests to file at the specified `folder_path`.


    Args:
        folder_path (str): The path where you want to save the text technology reports.
    """
    logger.info(f"Writing business case tests to path: {folder_path}")
    business_case_master = extract_data(
        IMPORT_DATA_PATH, "Business Cases Excel Master", "csv"
    )
    bc_master = (
        business_case_master.drop(labels=["Type of metric", "Unit"], axis=1)
        .melt(id_vars=["Material"], var_name="Technology")
        .set_index(["Technology", "Material"])
        .copy()
    )
    materials_ref = list(bc_master.index.get_level_values(1).unique())
    df_inspector = read_pickle_folder(
        PKL_DATA_FORMATTED, "business_case_test_df", "df"
    )
    for technology in tqdm(
        TECH_REFERENCE_LIST,
        total=len(TECH_REFERENCE_LIST),
        desc="Writing tests to file",
    ):
        write_tech_report_to_file(df_inspector, materials_ref, technology, folder_path)
