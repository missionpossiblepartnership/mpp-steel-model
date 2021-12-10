"""Script that standardises business cases into per steel units and into summary tables."""

# For Data Manipulation
import pandas as pd

# For logger
from mppSteel.utility.utils import get_logger, read_pickle_folder, serialise_file

from mppSteel.model_config import PKL_FOLDER, TECH_REFERENCE_LIST, FURNACE_GROUP_DICT

# Create logger
logger = get_logger("Business Case Standarisation")


def business_case_formatter_splitter(df: pd.DataFrame):
    logger.info(
        "Splitting the business cases into two DataFrames: Parameters and Processes"
    )
    df_c = df.copy()
    df_c = df_c.melt(
        id_vars=[
            "Section",
            "Process",
            "Process Detail",
            "Step",
            "Material Category",
            "Unit",
        ],
        var_name="Technology",
    )
    df_c.columns = [col.lower().replace(" ", "_") for col in df_c.columns]
    df_c["value"].fillna(0, inplace=True)
    df_c_parameters = df_c.loc[df_c["section"] == "Parameters"]
    df_c_process = df_c.loc[df_c["section"] != "Parameters"]
    df_c_parameters.drop(labels=["section"], axis=1, inplace=True)
    df_c_process.drop(labels=["section"], axis=1, inplace=True)
    return df_c_parameters, df_c_process


def tech_process_getter(
    df,
    technology: str,
    process: str,
    step: str = None,
    material: str = None,
    process_detail: str = None,
    full_ref: bool = False,
):

    df_c = df.copy()
    full_ref_cols = ["technology", "step", "material_category", "value"]
    if full_ref:
        choice_cols = full_ref_cols
    # ALL
    if step and material and process_detail:
        return df_c[
            (df_c["technology"] == technology)
            & (df_c["process"] == process)
            & (df_c["step"] == step)
            & (df_c["material_category"] == material)
            & (df_c["process_detail"] == process_detail)
        ]["value"].values[0]

    # 2 ONLY
    if material and step and not process_detail:
        return df_c[
            (df_c["technology"] == technology)
            & (df_c["process"] == process)
            & (df_c["material_category"] == material)
            & (df_c["step"] == step)
        ]["value"]["value"].values[0]
    if material and process_detail and not step:
        return df_c[
            (df_c["technology"] == technology)
            & (df_c["process"] == process)
            & (df_c["material_category"] == material)
            & (df_c["process_detail"] == process_detail)
        ]["value"].values[0]
    if step and process_detail and not material:
        return df_c[
            (df_c["technology"] == technology)
            & (df_c["process"] == process)
            & (df_c["process_detail"] == process_detail)
            & (df_c["step"] == step)
        ]["value"].values[0]

    # 1 ONLY
    if material and not step and not process_detail:
        return df_c[
            (df_c["technology"] == technology)
            & (df_c["process"] == process)
            & (df_c["material_category"] == material)
        ]["value"].values[0]
    if step and not material and not process_detail:
        return df_c[
            (df_c["technology"] == technology)
            & (df_c["process"] == process)
            & (df_c["step"] == step)
        ]["value"].values[0]
    if process_detail and not material and not step:
        return df_c[
            (df_c["technology"] == technology)
            & (df_c["process"] == process)
            & (df_c["process_detail"] == process_detail)
        ]["value"].values[0]

    # NONE
    if not full_ref:
        return df_c[(df_c["technology"] == technology) & (df_c["process"] == process)]
    full_ref_df = df_c[
        (df_c["technology"] == technology) & (df_c["process"] == process)
    ]
    return full_ref_df


def tech_parameter_getter(df, technology, parameter):
    df_c = df.copy()
    return df_c[(df_c["technology"] == technology) & (df_c["step"] == parameter)][
        "value"
    ].values[0]


def replace_units(df: pd.DataFrame, units_dict: dict):
    df_c = df.copy()
    for row in df_c.itertuples():
        if row.material_category in units_dict.keys():
            df_c.loc[row.Index, "unit"] = units_dict[row.material_category]
        else:
            df_c.loc[row.Index, "unit"] = ""
    return df_c


def create_mini_process_dfs(
    df: pd.DataFrame,
    technology_name: str,
    process_mapper: dict,
    factor_value_dict: dict,
):
    logger.info(f"-- Creating process dictionary for {technology_name}")
    df_c = df.copy()
    df_dict = {}
    for process in process_mapper[technology_name]:
        df_f = tech_process_getter(df_c, technology_name, process=process)
        df_f["value"] = df_f["value"] * factor_value_dict[process]
        df_dict[process] = df_f
    return df_dict


def format_combined_df(df: pd.DataFrame, units_dict: dict):
    logger.info("-- Mapping units to DataFrame")
    df_c = df.copy()
    df_c = replace_units(df_c, units_dict)
    df_c_grouped = df_c.groupby(by=["technology", "material_category", "unit"]).sum()
    return df_c_grouped.reset_index()


def create_hardcoded_exceptions(hard_coded_dict: dict, furnace_group_dict: dict):
    logger.info("-- Creating the hard coded exceptions for the process factors")
    hard_coded_factor_list = []
    for furnace_group in hard_coded_dict:
        hard_coded_factor_list.extend(furnace_group_dict[furnace_group])
    return hard_coded_factor_list


def sum_product_ef(df: pd.DataFrame, ef_dict: dict, materials_to_exclude: list = None):
    logger.info("--- Summing the Emissions Factors")
    df_c = df.copy()
    df_c["material_emissions"] = ""
    for row in df_c.itertuples():
        if materials_to_exclude:
            if (row.material_category in ef_dict.keys()) & (
                row.material_category not in materials_to_exclude
            ):
                df_c.loc[row.Index, "material_emissions"] = (
                    row.value * ef_dict[row.material_category]
                )
            else:
                df_c.loc[row.Index, "material_emissions"] = 0
        else:
            if row.material_category in ef_dict.keys():
                df_c.loc[row.Index, "material_emissions"] = (
                    row.value * ef_dict[row.material_category]
                )
            else:
                df_c.loc[row.Index, "material_emissions"] = 0

    return df_c["material_emissions"].sum()


def get_all_steam_values(
    df: pd.DataFrame, technology: str, process_list: list, factor_mapper: dict
):
    logger.info(f"--- Getting all steam values for {technology}")
    steam_value_list = []
    df_c = df.loc[df["technology"] == technology].copy()
    for process in process_list:
        if "Steam" in df_c["material_category"].unique():
            steam_value = tech_process_getter(
                bc_processes, technology, process=process, material="Steam"
            )
            factor = factor_mapper[process]
            steam_value_list.append(steam_value * factor)
    return steam_value_list


def get_all_electricity_values(
    df: pd.DataFrame,
    technology: str,
    process_list: list,
    factor_mapper: dict = [],
    as_dict: bool = False,
):
    logger.info(f"--- Getting all electricity values for {technology}")
    electricity_value_list = []
    electricity_value_dict = {}
    df_c = df.loc[df["technology"] == technology].copy()
    for process in process_list:
        if (
            "Electricity"
            in df_c[df_c["process"] == process]["material_category"].unique()
        ):
            if process == "Oxygen Generation":
                factor = factor_mapper["Basic Oxygen Steelmaking + Casting"]
            factor = factor_mapper[process]
            if process == "Basic Oxygen Steelmaking + Casting":
                electricity_value_oxygen_furnace = tech_process_getter(
                    df,
                    technology,
                    process=process,
                    process_detail="Energy-oxygen furnace",
                    material="Electricity",
                )
                val_to_append = electricity_value_oxygen_furnace * factor
                electricity_value_list.append(val_to_append)
                if as_dict:
                    electricity_value_dict[f"{process} - Oxygen"] = val_to_append
                electricity_value_casting = tech_process_getter(
                    df,
                    technology,
                    process=process,
                    process_detail="Energy-casting",
                    material="Electricity",
                )
                val_to_append = electricity_value_casting * factor
                electricity_value_list.append(val_to_append)
                if as_dict:
                    electricity_value_dict[f"{process} - Casting"] = val_to_append
            else:
                electricity_value_general = tech_process_getter(
                    bc_processes, technology, process=process, material="Electricity"
                )
                val_to_append = electricity_value_general * factor
                electricity_value_list.append(val_to_append)
                if as_dict:
                    electricity_value_dict[process] = val_to_append
    if as_dict:
        return electricity_value_dict
    return electricity_value_list


def create_tech_processes_list():
    logger.info(f"- Creating the process dictionary for each technology")
    basic_bof_processes = [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "Oxygen Generation",
        "Basic Oxygen Steelmaking + Casting",
        "Limestone",
        "Self-Generation Of Electricity",
    ]
    basic_bof_processes_ccs = basic_bof_processes.copy()
    basic_bof_processes_ccs.append("CCS")
    basic_bof_processes_ccu = basic_bof_processes.copy()
    basic_bof_processes_ccu.extend(["CCU -CO-based", "CCU -CO2-based"])
    dri_eaf_basic_processes = [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "EAF (Steel-making) + Casting",
    ]
    dri_eaf_basic_processes_ccs = dri_eaf_basic_processes.copy()
    dri_eaf_basic_processes_ccs.append("CCS")
    eaf_basic_processes = [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Blast Furnace",
        "EAF (Steel-making) + Casting",
    ]
    eaf_electro_processes = [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Electrolyzer",
        "EAF (Steel-making) + Casting",
    ]
    dri_melt_bof_processes = [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "Remelt",
        "Oxygen Generation",
        "Limestone",
        "Basic Oxygen Steelmaking + Casting",
    ]
    dri_melt_bof_processes_ccs = dri_melt_bof_processes.copy()
    dri_melt_bof_processes_ccs.append("CCS")
    dri_melt_bof_ch2 = [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Shaft Furnace",
        "Remelt",
        "Oxygen Generation",
        "Limestone",
        "Basic Oxygen Steelmaking + Casting",
    ]
    basic_smelting_processes = [
        "Coke Production",
        "Sintering",
        "Pelletisation",
        "Smelting Furnace",
        "Oxygen Generation",
        "Limestone",
        "Basic Oxygen Steelmaking + Casting",
        "Self-Generation Of Electricity",
    ]
    basic_smelting_processes_ccs = basic_smelting_processes.copy()
    basic_smelting_processes_ccs.append("CCS")

    return {
        "Avg BF-BOF": basic_bof_processes,
        "BAT BF-BOF": basic_bof_processes,
        "BAT BF-BOF_bio PCI": basic_bof_processes,
        "BAT BF-BOF_H2 PCI": basic_bof_processes,
        "BAT BF-BOF+CCUS": basic_bof_processes_ccs,
        "DRI-EAF": dri_eaf_basic_processes,
        "DRI-EAF_50% green H2": dri_eaf_basic_processes,
        "DRI-EAF_50% bio-CH4": dri_eaf_basic_processes,
        "DRI-EAF+CCUS": dri_eaf_basic_processes_ccs,
        "DRI-EAF_100% green H2": dri_eaf_basic_processes,
        "Smelting Reduction": basic_smelting_processes,
        "Smelting Reduction+CCUS": basic_smelting_processes_ccs,
        "EAF": eaf_basic_processes,
        "Electrolyzer-EAF": eaf_electro_processes,
        "BAT BF-BOF+CCU": basic_bof_processes_ccu,
        "DRI-Melt-BOF": dri_melt_bof_processes,
        "DRI-Melt-BOF+CCUS": dri_melt_bof_processes_ccs,
        "DRI-Melt-BOF_100% zero-C H2": dri_melt_bof_ch2,
        "Electrowinning-EAF": eaf_electro_processes,
        "BAT BF-BOF+BECCUS": basic_bof_processes_ccs,
        "Non-Furnace": [],
        "Charcoal mini furnace": [],
    }


def create_production_factors(
    technology: str, furnace_group_dict: dict, hard_coded_factors: dict
):
    business_cases = read_pickle_folder(PKL_FOLDER, "business_cases")
    bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
    logger.info(f"-- Creating the production factors for {technology}")
    # Instantiate factors
    COKE_PRODUCTION_FACTOR = None
    SINTERING_FACTOR = None
    PELLETISATION_FACTOR = None
    BLAST_FURNACE_FACTOR = None
    OXYGEN_GENERATION_FACTOR = None
    BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR = None
    SHAFT_FURNACE_FACTOR = None
    EAF_STEELMAKING_CASTING_FACTOR = None
    LIMESTONE_FACTOR = None
    ELECTRICITY_GENERATION_FACTOR = None
    SMELTING_FURNACE_FACTOR = None
    ELECTROLYZER_FACTOR = None
    CCS_FACTOR = None
    CCU_CO_FACTOR = None
    CCU_CO2_FACTOR = None
    REMELT_FACTOR = None

    # SET BASE FACTORS
    if technology in bosc_factor_group:
        BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR = 1.02
    if technology in eaf_factor_group:
        EAF_STEELMAKING_CASTING_FACTOR = 1.02

    # Factor Calculation: BOSC
    if technology in bosc_factor_group:
        hot_metal_required = tech_process_getter(
            bc_processes,
            technology,
            "Basic Oxygen Steelmaking + Casting",
            step="Hot metal required",
        )
        bosc_hot_metal_required = (
            hot_metal_required * BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR
        )
        bof_lime = (
            tech_process_getter(
                bc_processes, technology, process="Limestone", step="BOF lime"
            )
            * BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR
        )
        oxygen_electricity = tech_process_getter(
            bc_processes,
            technology,
            process="Oxygen Generation",
            material="Electricity",
        )
        OXYGEN_GENERATION_FACTOR = (
            BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR * oxygen_electricity
        )

        # Smelting
        if technology in furnace_group_dict["smelting_reduction"]:
            SMELTING_FURNACE_FACTOR = bosc_hot_metal_required.copy()
            smelting_furnace_sinter = (
                tech_process_getter(
                    bc_processes, technology, "Smelting Furnace", step="Sinter"
                )
                * SMELTING_FURNACE_FACTOR
            )
            SINTERING_FACTOR = smelting_furnace_sinter.copy()
            smelting_furnace_pellets = (
                tech_process_getter(
                    bc_processes, technology, "Smelting Furnace", step="Pellets"
                )
                * SMELTING_FURNACE_FACTOR
            )
            PELLETISATION_FACTOR = smelting_furnace_pellets.copy()

        # Blast Furnace
        if technology in furnace_group_dict["blast_furnace"]:
            BLAST_FURNACE_FACTOR = bosc_hot_metal_required.copy()
            blast_furnace_sinter = (
                tech_process_getter(
                    bc_processes, technology, "Blast Furnace", step="Sinter"
                )
                * BLAST_FURNACE_FACTOR
            )
            SINTERING_FACTOR = blast_furnace_sinter.copy()
            blast_furnace_pellets = (
                tech_process_getter(
                    bc_processes, technology, "Blast Furnace", step="Pellets"
                )
                * BLAST_FURNACE_FACTOR
            )
            PELLETISATION_FACTOR = blast_furnace_pellets.copy()
            coke_lcv = tech_parameter_getter(bc_parameters, technology, "Coke LCV")
            blast_furnace_coke = (
                tech_process_getter(
                    bc_processes, technology, "Blast Furnace", step="Coke"
                )
                * coke_lcv
                * BLAST_FURNACE_FACTOR
            )
            COKE_PRODUCTION_FACTOR = blast_furnace_coke / coke_lcv

        # DRI-BOF
        if technology in furnace_group_dict["dri-bof"]:
            REMELT_FACTOR = bosc_hot_metal_required.copy()
            dri_captive_remelt = (
                tech_process_getter(
                    bc_processes, technology, "Remelt", step="DRI - captive"
                )
                * REMELT_FACTOR
            )
            SHAFT_FURNACE_FACTOR = dri_captive_remelt.copy()
            shaft_furnace_pellets = (
                tech_process_getter(
                    bc_processes, technology, "Shaft Furnace", step="Pellets"
                )
                * SHAFT_FURNACE_FACTOR
            )
            PELLETISATION_FACTOR = shaft_furnace_pellets.copy()

    # Factor Calculation: EAF
    # EAF Basic
    if technology in furnace_group_dict["eaf-basic"]:
        COKE_PRODUCTION_FACTOR = 1
        SINTERING_FACTOR = 1
        PELLETISATION_FACTOR = 1
        BLAST_FURNACE_FACTOR = 1

    # EAF Advanced
    if technology in furnace_group_dict["eaf-advanced"]:
        COKE_PRODUCTION_FACTOR = 0
        SINTERING_FACTOR = 0
        ELECTROLYZER_FACTOR = tech_process_getter(
            bc_processes,
            technology,
            "EAF (Steel-making) + Casting",
            step="Iron in steel",
        )
        electrolyzer_pellets = (
            0 * ELECTROLYZER_FACTOR
        )  # * Mystery cell?? - No Pellets in Business Cases One Table
        PELLETISATION_FACTOR = electrolyzer_pellets.copy()
        electrolyzer_coke = (
            0 * ELECTROLYZER_FACTOR
        )  # * Mystery cell?? - No Coke in Business Cases One Table
        electrolyzer_thermal_coal = (
            0 * ELECTROLYZER_FACTOR / 1000
        )  # * Mystery cell?? - No Thermal Coal in Business Cases One Table

    # DRI-EAF
    if technology in furnace_group_dict["dri-eaf"]:
        COKE_PRODUCTION_FACTOR = 0
        SINTERING_FACTOR = 0
        dri_captive_eaf_casting = (
            tech_process_getter(
                bc_processes,
                technology,
                "EAF (Steel-making) + Casting",
                step="DRI - captive",
            )
            * EAF_STEELMAKING_CASTING_FACTOR
        )
        SHAFT_FURNACE_FACTOR = dri_captive_eaf_casting.copy()
        shaft_furnace_pellets = (
            tech_process_getter(
                bc_processes, technology, "Shaft Furnace", step="Pellets"
            )
            * SHAFT_FURNACE_FACTOR
        )
        PELLETISATION_FACTOR = shaft_furnace_pellets.copy()
        shaft_furnace_coke = (
            tech_process_getter(
                bc_processes, technology, "Shaft Furnace", material="Coke"
            )
            * SHAFT_FURNACE_FACTOR
        )  # * Mystery cell??
        shaft_furnace_coal = (
            tech_process_getter(
                bc_processes, technology, "Shaft Furnace", material="Coal"
            )
            * SHAFT_FURNACE_FACTOR
            * tech_parameter_getter(
                bc_parameters, technology, "DRI metallic Fe concentration"
            )
            / 1000
        )

    # Create process factor
    factor_list = [
        COKE_PRODUCTION_FACTOR,
        SINTERING_FACTOR,
        PELLETISATION_FACTOR,
        BLAST_FURNACE_FACTOR,
        OXYGEN_GENERATION_FACTOR,
        BASIC_OXYGEN_STEELMAKING_CASTING_FACTOR,
        SHAFT_FURNACE_FACTOR,
        EAF_STEELMAKING_CASTING_FACTOR,
        LIMESTONE_FACTOR,
        ELECTRICITY_GENERATION_FACTOR,
        CCS_FACTOR,
        SMELTING_FURNACE_FACTOR,
        ELECTROLYZER_FACTOR,
        CCU_CO_FACTOR,
        CCU_CO2_FACTOR,
        REMELT_FACTOR,
    ]

    # Overwrite dictionary values
    process_factor_mapper = dict(zip(processes, factor_list))
    # Overwrite processes
    if technology in hard_coded_factors:
        for furnace_group in hard_coded_factors:
            for tech_list in furnace_group_dict[furnace_group]:
                if technology in tech_list:
                    for process in hard_coded_factors[furnace_group]:
                        process_factor_mapper[process] = hard_coded_factors[
                            furnace_group
                        ][process]
    # Replace None values with 0
    for key in process_factor_mapper.keys():
        process_factor_mapper[key] = list({process_factor_mapper[key] or 0})[0]

    return process_factor_mapper


def limestone_df_editor(
    df_dict: dict, technology: str, furnace_group_dict: dict, factor_dict: dict
):

    logger.info(f"-- Creating the limestone calculations for {technology}")

    df_dict_c = df_dict.copy()

    if technology in electricity_self_gen_group:

        if technology in furnace_group_dict["smelting_reduction"]:
            limestone_df = df_dict_c["Limestone"].copy()
            bof_lime = (
                tech_process_getter(
                    bc_processes, technology, "Limestone", step="BOF lime"
                )
                * factor_dict["Basic Oxygen Steelmaking + Casting"]
            )
            limestone_df.loc[limestone_df["step"] == "BOF lime", "value"] = bof_lime

            new_row = {
                "process": "Limestone",
                "process_detail": "",
                "step": "Process emissions",
                "material_category": "Process emissions",
                "unit": "t CO2 / t LS",
                "technology": technology,
                "value": (bof_lime * 0.75) / 1000,
            }
            limestone_df.append(new_row, ignore_index=True)
            df_dict_c["Limestone"] = limestone_df

        if technology in furnace_group_dict["blast_furnace"]:
            limestone_df = df_dict_c["Limestone"].copy()
            bof_lime = (
                tech_process_getter(
                    bc_processes, technology, "Limestone", step="BOF lime"
                )
                * factor_dict["Basic Oxygen Steelmaking + Casting"]
            )
            limestone_df.loc[limestone_df["step"] == "BOF lime", "value"] = bof_lime
            blast_furnace_lime = tech_process_getter(
                bc_processes, technology, "Limestone", step="Blast furnace lime"
            )
            limestone_df.loc[limestone_df["step"] == "Blast furnace lime", "value"] = (
                blast_furnace_lime * factor_dict["Blast Furnace"]
            )

            new_row = {
                "process": "Limestone",
                "process_detail": "",
                "step": "Process emissions",
                "material_category": "Process emissions",
                "unit": "t CO2 / t LS",
                "technology": technology,
                "value": (bof_lime + blast_furnace_lime) * 0.75 / 1000,
            }
            limestone_df = limestone_df.append(new_row, ignore_index=True)
            df_dict_c["Limestone"] = limestone_df

    return df_dict_c


def ccs_df_editor(
    df_dict: dict,
    technology: str,
    furnace_group_dict: dict,
    factor_dict: dict,
    ef_dict: dict,
):

    logger.info(f"-- Creating the ccs calculations for {technology}")

    df_dict_c = df_dict.copy()

    if technology in furnace_group_dict["ccs"]:

        ccs_df = df_dict_c["CCS"].copy()

        reboiler_duty_natural_gas = tech_process_getter(
            bc_processes, technology, process="CCS", material="Natural gas"
        )

        # CCS FACTOR
        if technology in ["Smelting Reduction+CCUS"]:
            electricity_share_factor = tech_parameter_getter(
                bc_parameters,
                technology,
                "Share of electricity purchased in total demand",
            )
            natural_gas_ccs = 0
            smelting_furnace_thermal_coal = tech_process_getter(
                df_dict_c["Smelting Furnace"],
                technology,
                process="Smelting Furnace",
                material="Thermal coal",
            )
            thermal_coal_ef = ef_dict["Thermal coal"]
            smelting_furnace_natural_gas = tech_process_getter(
                df_dict_c["Smelting Furnace"],
                technology,
                process="Smelting Furnace",
                material="Natural gas",
            )
            CCS_FACTOR = (
                (smelting_furnace_thermal_coal * thermal_coal_ef / 1000)
                + (
                    (natural_gas_ccs + smelting_furnace_natural_gas)
                    * EF_DICT["Natural gas"]
                    / 1000
                )
                + ((1 - electricity_share_factor) * 10 * (EF_DICT["BOF gas"] / 1000))
            )

        if technology in ["DRI-EAF+CCUS"]:
            pellets_natural_gas = tech_process_getter(
                df_dict_c["Pelletisation"],
                technology,
                process="Pelletisation",
                material="Natural gas",
            )
            shaft_furnace_natural_gas = tech_process_getter(
                df_dict_c["Shaft Furnace"],
                technology,
                process="Shaft Furnace",
                material="Natural gas",
            )
            eaf_natural_gas = tech_process_getter(
                df_dict_c["EAF (Steel-making) + Casting"],
                technology,
                process="EAF (Steel-making) + Casting",
                material="Natural gas",
            )
            eaf_process_emissions = tech_process_getter(
                df_dict_c["EAF (Steel-making) + Casting"],
                technology,
                process="EAF (Steel-making) + Casting",
                material="Process emissions",
            )
            CCS_FACTOR = (
                (pellets_natural_gas + pellets_natural_gas + eaf_natural_gas)
                * (ef_dict["Natural gas"] / 1000)
            ) + eaf_process_emissions

        if technology in ["DRI-Melt-BOF+CCUS"]:
            selected_processes_df = concat_process_dfs(
                df_dict_c, ["Shaft Furnace", "Remelt"]
            )
            ef_sum_product = sum_product_ef(selected_processes_df, ef_dict)
            CCS_FACTOR = ef_sum_product / 1000

        if technology in ["BAT BF-BOF+CCUS", "BAT BF-BOF+BECCUS"]:
            electricity_share_factor = tech_parameter_getter(
                bc_parameters,
                technology,
                "Share of electricity purchased in total demand",
            )
            limestone_process_emissions = tech_process_getter(
                df_dict_c["Limestone"],
                technology,
                process="Limestone",
                step="Process emissions",
            )
            selected_processes_df = concat_process_dfs(
                df_dict_c,
                ["Coke Production", "Sintering", "Pelletisation", "Blast Furnace"],
            )
            ef_sum_product_large = sum_product_ef(selected_processes_df, ef_dict)
            selected_processes_df = concat_process_dfs(df_dict_c, ["Blast Furnace"])
            ef_sum_product_small = sum_product_ef(
                selected_processes_df, ef_dict, ["Electricity"]
            )
            CCS_FACTOR = (
                (ef_sum_product_large / 1000)
                + ((ef_sum_product_small * ef_dict["Electricity"]) / 1000)
                + 0.52 * (1 - electricity_share_factor)
                + limestone_process_emissions
            )

        # captured co2 value
        captured_co2 = tech_process_getter(
            df_dict_c["CCS"], technology, process="CCS", step="Captured CO2"
        )
        captured_co2_factored = captured_co2 * CCS_FACTOR
        ccs_df.loc[(ccs_df["step"] == "Captured CO2"), "value"] = captured_co2_factored
        compression_electricity = tech_process_getter(
            df_dict_c["CCS"], technology, process="CCS", process_detail="Compression"
        )

        # Compression Electricity Value
        if technology in ["BAT BF-BOF+CCUS", "DRI-EAF+CCUS", "Smelting Reduction+CCUS"]:
            ccs_df.loc[(ccs_df["process_detail"] == "Compression"), "value"] = (
                captured_co2_factored * compression_electricity
            )

        if technology in ["DRI-Melt-BOF+CCUS"]:
            remelter_heating_efficiency = tech_parameter_getter(
                bc_parameters, technology, "Efficiency of remelter heating"
            )
            ccs_df.loc[(ccs_df["process_detail"] == "Compression"), "value"] = (
                captured_co2_factored
                * compression_electricity
                * remelter_heating_efficiency
            )

        if technology in ["BAT BF-BOF+BECCUS"]:
            electricity_share_factor = tech_parameter_getter(
                bc_parameters,
                technology,
                "Share of electricity purchased in total demand",
            )
            ccs_df.loc[(ccs_df["process_detail"] == "Compression"), "value"] = (
                captured_co2_factored
                * compression_electricity
                * electricity_share_factor
            )

        # Reboiler duty: Natural Gas / Electricity Value

        if technology in ["DRI-EAF+CCUS"]:
            ccs_df.loc[(ccs_df["material_category"] == "Natural gas"), "value"] = (
                reboiler_duty_natural_gas * CCS_FACTOR
            )

        if technology in ["Smelting Reduction+CCUS"]:
            ccs_df.loc[(ccs_df["material_category"] == "Natural gas"), "value"] = 0

        if technology in ["DRI-Melt-BOF+CCUS", "BAT BF-BOF+BECCUS"]:
            column_matcher_dict = {
                "DRI-Melt-BOF+CCUS": ["Pelletisation", "Shaft Furnace", "Remelt"],
                "BAT BF-BOF+BECCUS": [
                    "Coke Production",
                    "Sintering",
                    "Pelletisation",
                    "Blast Furnace",
                ],
            }
            selected_processes_df = concat_process_dfs(
                df_dict_c, column_matcher_dict[technology]
            )
            ef_sum_product = sum_product_ef(selected_processes_df, ef_dict)
            ccs_df.loc[(ccs_df["material_category"] == "Natural gas"), "value"] = (
                3.6 * ef_sum_product / 1000
            )

        if technology in ["BAT BF-BOF+CCUS"]:
            selected_processes_df = concat_process_dfs(df_dict_c, ["Blast Furnace"])
            ef_sum_product = sum_product_ef(
                selected_processes_df, ef_dict, ["Electricity"]
            )
            ccs_df.loc[(ccs_df["material_category"] == "Natural gas"), "value"] = (
                reboiler_duty_natural_gas * ef_sum_product / 1000
            )

        # Remove last value for smelting reduction
        if technology in ["Smelting Reduction+CCUS"]:
            ccs_df = ccs_df[1:]

        df_dict_c["CCS"] = ccs_df

    return df_dict_c


def ccu_df_editor(
    df_dict: dict, technology: str, furnace_group_dict: dict, ef_dict: dict
):

    logger.info(f"-- Creating the ccu calculations for {technology}")

    df_dict_c = df_dict.copy()

    if technology in furnace_group_dict["ccu"]:
        ccu_co = df_dict_c["CCU -CO-based"].copy()
        ccu_co2 = df_dict_c["CCU -CO2-based"].copy()

        # co
        ETHANOL_PRODUCTION_FROM_CO = 180
        co_utilization_rate = tech_parameter_getter(
            bc_parameters, technology, "CCS Capture rate"
        )
        CCU_CO_FACTOR = ETHANOL_PRODUCTION_FROM_CO * co_utilization_rate / 1000
        used_co2 = tech_process_getter(
            df_dict_c["CCU -CO-based"],
            technology,
            process="CCU -CO-based",
            material="Used CO2",
        )
        ccu_co.loc[(ccu_co["material_category"] == "Used CO2"), "value"] = (
            used_co2 * CCU_CO_FACTOR
        )
        ccu_co.loc[(ccu_co["material_category"] == "Electricity"), "value"] = 0
        df_dict_c["CCU -CO-based"] = ccu_co

        # co2
        selected_processes_df = concat_process_dfs(
            df_dict_c,
            ["Coke Production", "Sintering", "Pelletisation", "Blast Furnace"],
        )
        ef_sum_product_large = sum_product_ef(selected_processes_df, ef_dict)
        limestone_process_emissions = tech_process_getter(
            df_dict_c["Limestone"],
            technology,
            process="Limestone",
            step="Process emissions",
        )
        CCU_CO2_FACTOR = (
            (ef_sum_product_large / 1000) + limestone_process_emissions - used_co2
        )
        selected_processes_df = concat_process_dfs(df_dict_c, ["Blast Furnace"])
        ef_sum_product_small = sum_product_ef(
            selected_processes_df, ef_dict, ["Electricity"]
        )
        ccu_co2.loc[(ccu_co2["process_detail"] == "Reboiler duty"), "value"] = (
            used_co2 * ef_sum_product_small / 1000
        )
        compression_electricity = tech_process_getter(
            df_dict_c["CCU -CO2-based"],
            technology,
            process="CCU -CO2-based",
            process_detail="Compression",
        )
        ccu_co2.loc[(ccu_co2["process_detail"] == "Compression"), "value"] = (
            CCU_CO2_FACTOR * compression_electricity
        )
        captured_co2 = tech_process_getter(
            df_dict_c["CCU -CO2-based"],
            technology,
            process="CCU -CO2-based",
            step="Captured CO2",
        )
        ccu_co2.loc[(ccu_co2["material_category"] == "Captured CO2"), "value"] = (
            captured_co2 * CCU_CO2_FACTOR
        )
        df_dict_c["CCU -CO2-based"] = ccu_co2

    return df_dict_c


# SELF GEN ELECTRICITY
def self_gen_df_editor(
    df_dict: dict,
    technology: str,
    furnace_group_dict: dict,
    factor_dict: dict,
    tech_processes_dict: dict,
):

    logger.info(
        f"-- Creating the Self Generation of Electricity calculations for {technology}"
    )

    df_dict_c = df_dict.copy()

    if technology in electricity_self_gen_group:
        self_gen_name = "Self-Generation Of Electricity"
        self_gen_df = df_dict_c[self_gen_name].copy()
        electricity_share_factor = tech_parameter_getter(
            bc_parameters, technology, "Share of electricity purchased in total demand"
        )

        if technology in furnace_group_dict["smelting_reduction"]:
            bof_gas = tech_process_getter(
                bc_processes, technology, process=self_gen_name, material="BOF gas"
            )
            all_electricity_values = get_all_electricity_values(
                bc_processes, technology, tech_processes_dict[technology], factor_dict
            )
            self_gen_df.loc[
                (self_gen_df["material_category"] == "BOF gas"), "value"
            ] = (sum(all_electricity_values) * (1 - electricity_share_factor) * bof_gas)
            thermal_coal = tech_process_getter(
                bc_processes, technology, process=self_gen_name, material="Thermal coal"
            )
            all_steam_values = get_all_steam_values(
                self_gen_df, technology, tech_processes_dict[technology], factor_dict
            )
            self_gen_df.loc[
                (self_gen_df["material_category"] == "Thermal coal"), "value"
            ] = (sum(all_steam_values) * thermal_coal)

        if technology in furnace_group_dict["blast_furnace"]:
            all_electricity_values = get_all_electricity_values(
                bc_processes, technology, tech_processes_dict[technology], factor_dict
            )
            cog = tech_process_getter(
                bc_processes, technology, process=self_gen_name, material="COG"
            )
            self_gen_df.loc[(self_gen_df["material_category"] == "COG"), "value"] = (
                sum(all_electricity_values) * (1 - electricity_share_factor) * cog
            )
            bf_gas = tech_process_getter(
                bc_processes, technology, process=self_gen_name, material="BF gas"
            )
            self_gen_df.loc[(self_gen_df["material_category"] == "BF gas"), "value"] = (
                sum(all_electricity_values) * (1 - electricity_share_factor) * bf_gas
            )

            if technology in [
                "BAT BF-BOF_bio PCI",
                "BAT BF-BOF_H2 PCI",
                "BAT BF-BOF+CCUS",
                "BAT BF-BOF+BECCUS",
                "BAT BF-BOF+CCU",
            ]:
                thermal_coal = tech_process_getter(
                    bc_processes,
                    technology,
                    process=self_gen_name,
                    material="Thermal coal",
                )
                all_steam_values = get_all_steam_values(
                    self_gen_df,
                    technology,
                    tech_processes_dict[technology],
                    factor_dict,
                )
                self_gen_df.loc[
                    (self_gen_df["material_category"] == "Thermal coal"), "value"
                ] = (sum(all_steam_values) * thermal_coal)

        df_dict_c[self_gen_name] = self_gen_df

    return df_dict_c


def full_model_flow(tech_name: str):

    logger.info(f"- Running the model flow for {tech_name}")
    process_prod_factor_mapper = create_production_factors(
        tech_name, FURNACE_GROUP_DICT, HARD_CODED_FACTORS
    )
    reformated_dict = create_mini_process_dfs(
        bc_processes, tech_name, TECHNOLOGY_PROCESSES, process_prod_factor_mapper
    )
    reformated_dict_c = reformated_dict.copy()
    reformated_dict_c = fix_exceptions(
        reformated_dict_c,
        tech_name,
        FURNACE_GROUP_DICT,
        process_prod_factor_mapper,
        TECHNOLOGY_PROCESSES,
    )
    reformated_dict_c = limestone_df_editor(
        reformated_dict_c, tech_name, FURNACE_GROUP_DICT, process_prod_factor_mapper
    )
    reformated_dict_c = ccs_df_editor(
        reformated_dict_c,
        tech_name,
        FURNACE_GROUP_DICT,
        process_prod_factor_mapper,
        EF_DICT,
    )
    reformated_dict_c = ccu_df_editor(
        reformated_dict_c, tech_name, FURNACE_GROUP_DICT, EF_DICT
    )
    reformated_dict_c = self_gen_df_editor(
        reformated_dict_c,
        tech_name,
        FURNACE_GROUP_DICT,
        process_prod_factor_mapper,
        TECHNOLOGY_PROCESSES,
    )
    combined_df = pd.concat(reformated_dict_c.values()).reset_index(drop=True)
    combined_df = format_combined_df(combined_df, PER_T_STEEL_DICT_UNITS)
    return combined_df


def generate_full_consumption_table(technology_list: list):
    logger.info("- Generating the full resource consumption table")
    summary_df_list = []
    for technology in technology_list:
        logger.info(f"*** Starting standardisation flow for {technology} ***")
        summary_df_list.append(full_model_flow(technology))
    return pd.concat(summary_df_list)


def concat_process_dfs(df_dict: pd.DataFrame, process_list: list):
    logger.info("- Concatenating all of the resource consumption tables")
    df_list = []
    for process in process_list:
        df_list.append(df_dict[process])
    concat_df = pd.concat(df_list)
    return concat_df


def fix_exceptions(
    df_dict: dict,
    technology: str,
    furnace_group_dict: dict,
    factor_dict: dict,
    process_dict: dict,
):

    logger.info(f"- Fixing specific values for {technology}")

    df_dict_c = df_dict.copy()

    if technology in ["Smelting Reduction"]:
        smelting_furnace_df = df_dict_c["Smelting Furnace"].copy()
        electricity_share_factor = tech_parameter_getter(
            bc_parameters, technology, "Share of electricity purchased in total demand"
        )
        smelting_electricity = tech_process_getter(
            bc_processes, technology, process="Smelting Furnace", material="Electricity"
        )
        smelting_furnace_df.loc[
            smelting_furnace_df["material_category"] == "Electricity", "value"
        ] = (
            smelting_electricity
            * factor_dict["Smelting Furnace"]
            * electricity_share_factor
        )
        df_dict_c["Smelting Furnace"] = smelting_furnace_df

    if technology in ["Smelting Reduction+CCUS"]:
        smelting_furnace_df = df_dict_c["Smelting Furnace"].copy()
        smelting_furnace_df.loc[
            smelting_furnace_df["material_category"] == "Coke", "value"
        ] = 0
        df_dict_c["Smelting Furnace"] = smelting_furnace_df

    if technology in furnace_group_dict["blast_furnace"]:

        # Electricity
        elec_values_dict = get_all_electricity_values(
            bc_processes,
            technology,
            process_dict[technology],
            factor_mapper=factor_dict,
            as_dict=True,
        )
        electricity_share = tech_parameter_getter(
            bc_parameters, technology, "Share of electricity purchased in total demand"
        )
        for process in elec_values_dict.keys():
            elec_values_dict[process] = elec_values_dict[process] * electricity_share
        for process in df_dict_c.keys():
            temp_process_df = df_dict_c[process]
            if "Electricity" in temp_process_df["material_category"].unique():
                if process == "Basic Oxygen Steelmaking + Casting":
                    temp_process_df.loc[
                        temp_process_df["process_detail"] == "Energy-oxygen furnace",
                        "value",
                    ] = elec_values_dict["Basic Oxygen Steelmaking + Casting - Oxygen"]
                    temp_process_df.loc[
                        temp_process_df["process_detail"] == "Energy-casting", "value"
                    ] = elec_values_dict["Basic Oxygen Steelmaking + Casting - Casting"]
                    df_dict_c[process] = temp_process_df
                else:
                    temp_process_df.loc[
                        temp_process_df["process_detail"] == process, "value"
                    ] = elec_values_dict[process]
                    df_dict_c[process] = temp_process_df

        # Coke
        blast_furnace_df = df_dict_c["Blast Furnace"].copy()
        lcv = tech_parameter_getter(
            bc_parameters, technology, "LCV of injected reductant"
        )
        bat_lcv = lcv = tech_parameter_getter(
            bc_parameters, "BAT BF-BOF", "LCV of injected reductant"
        )
        blast_furnace_factor = factor_dict["Blast Furnace"]
        coke = tech_process_getter(
            bc_processes, technology, process="Blast Furnace", material="Coke"
        )
        coke_lcv = tech_parameter_getter(bc_parameters, technology, "Coke LCV")
        coke_lcv_calculation = coke * coke_lcv * blast_furnace_factor
        blast_furnace_df.loc[
            blast_furnace_df["material_category"] == "Coke", "value"
        ] = coke_lcv_calculation

        if technology in ["BAT BF-BOF+BECCUS", "BAT BF-BOF_bio PCI"]:
            # All electricities
            # Biomass
            biomass = tech_process_getter(
                bc_processes,
                technology,
                process="Blast Furnace",
                process_detail="Tuyere injection",
                material="Biomass",
            )
            biomass_calculation = biomass * blast_furnace_factor * lcv / 1000
            blast_furnace_df.loc[
                (blast_furnace_df["process_detail"] == "Tuyere injection"), "value"
            ] = biomass_calculation

        if technology in ["BAT BF-BOF+CCU"]:
            # All electricities
            # Plastic Waste
            plastic_waste = tech_process_getter(
                bc_processes,
                technology,
                process="Blast Furnace",
                material="Plastic waste",
            )
            plastic_waste_calculation = (
                plastic_waste * blast_furnace_factor * lcv / 1000
            )
            blast_furnace_df.loc[
                blast_furnace_df["material_category"] == "Plastic waste", "value"
            ] = plastic_waste_calculation

        if technology in [
            "BAT BF-BOF",
            "Avg BF-BOF",
            "BAT BF-BOF+CCUS",
            "BAT BF-BOF_H2 PCI",
        ]:
            # All electricities
            # Thermal Coal
            thermal_coal = tech_process_getter(
                bc_processes,
                technology,
                process="Blast Furnace",
                material="Thermal coal",
            )
            thermal_coal_calculation = (
                thermal_coal * blast_furnace_factor * bat_lcv / 1000
            )
            blast_furnace_df.loc[
                blast_furnace_df["material_category"] == "Thermal coal", "value"
            ] = thermal_coal_calculation

            if technology in ["BAT BF-BOF_H2 PCI"]:
                # Hydrogen
                hydrogen = tech_process_getter(
                    bc_processes,
                    technology,
                    process="Blast Furnace",
                    material="Hydrogen",
                )
                hydrogen_calculation = hydrogen * blast_furnace_factor * lcv / 1000
                blast_furnace_df.loc[
                    blast_furnace_df["material_category"] == "Hydrogen", "value"
                ] = hydrogen_calculation

        df_dict_c["Blast Furnace"] = blast_furnace_df

    if technology in [
        "DRI-EAF_50% green H2",
        "DRI-EAF_50% bio-CH4",
        "DRI-Melt-BOF",
        "DRI-EAF+CCUS",
        "DRI-Melt-BOF+CCUS",
        "DRI-EAF",
    ]:
        # No change for the following technologies: DRI-EAF_100% green H2, DRI-Melt-BOF_100% zero-C H2, EAF

        # Coke
        shaft_furnace_df = df_dict_c["Shaft Furnace"].copy()
        shaft_furnace_factor = factor_dict["Shaft Furnace"]
        coke = tech_process_getter(
            bc_processes, technology, process="Shaft Furnace", material="Coke"
        )

        if technology in ["DRI-EAF+CCUS", "DRI-EAF_50% bio-CH4", "DRI-EAF"]:
            shaft_furnace_df.loc[
                shaft_furnace_df["material_category"] == "Coke", "value"
            ] = (coke * shaft_furnace_factor)

        if technology in ["DRI-Melt-BOF+CCUS"]:
            oxygen_consumption = tech_parameter_getter(
                bc_parameters, technology, "Oxygen consumption"
            )
            coke_calculation = coke * shaft_furnace_factor * oxygen_consumption

        if technology in ["DRI-EAF_50% green H2"]:
            # Coke
            shaft_furnace_df.loc[
                shaft_furnace_df["material_category"] == "Coke", "value"
            ] = 0

        if technology in ["DRI-Melt-BOF"]:
            iron_heat_capacity_solid = tech_parameter_getter(
                bc_parameters, technology, "Iron heat capacity - solid"
            )
            shaft_furnace_df.loc[
                shaft_furnace_df["material_category"] == "Coke", "value"
            ] = (coke * shaft_furnace_factor * iron_heat_capacity_solid)

        # Thermal Coal
        if technology in ["DRI-EAF+CCUS"]:
            shaft_furnace_df.loc[
                shaft_furnace_df["material_category"] == "Thermal coal", "value"
            ] = 0

        if technology in ["DRI-EAF"]:
            coal = tech_process_getter(
                bc_processes, technology, process="Shaft Furnace", material="Coal"
            )
            dri_metallic_fe = tech_parameter_getter(
                bc_parameters, technology, "DRI metallic Fe concentration"
            )
            shaft_furnace_df.loc[
                shaft_furnace_df["material_category"] == "Coal", "value"
            ] = (coal * shaft_furnace_factor * dri_metallic_fe)

        if technology in [
            "DRI-EAF_50% bio-CH4",
            "DRI-EAF_50% green H2",
            "DRI-Melt-BOF",
            "DRI-Melt-BOF+CCUS",
        ]:
            thermal_coal = tech_process_getter(
                bc_processes,
                technology,
                process="Shaft Furnace",
                material="Thermal coal",
            )

            if technology in ["DRI-EAF_50% bio-CH4"]:
                biomethane_share = tech_parameter_getter(
                    bc_parameters, technology, "Biomethane share in methane input"
                )
                shaft_furnace_df.loc[
                    shaft_furnace_df["material_category"] == "Thermal coal", "value"
                ] = (thermal_coal * shaft_furnace_factor * biomethane_share / 1000)

            if technology in ["DRI-EAF_50% green H2"]:
                h2_requirements = tech_parameter_getter(
                    bc_parameters, technology, "H2 required per 1 t of Fe"
                )
                shaft_furnace_df.loc[
                    shaft_furnace_df["material_category"] == "Thermal coal", "value"
                ] = (thermal_coal * shaft_furnace_factor * h2_requirements / 1000)

            if technology in ["DRI-Melt-BOF", "DRI-Melt-BOF+CCUS"]:
                oxygen_consumption = tech_parameter_getter(
                    bc_parameters, technology, "Oxygen consumption"
                )
                shaft_furnace_df.loc[
                    shaft_furnace_df["material_category"] == "Thermal coal", "value"
                ] = (thermal_coal * shaft_furnace_factor * oxygen_consumption / 1000)

        df_dict_c["Shaft Furnace"] = shaft_furnace_df

    if technology in ["Electrowinning-EAF", "Electrowinning-EAF"]:
        # Electrolyzer: Coke & Thermal Coal | can be left as zero
        pass

    return df_dict_c


# Define units dict
PER_T_STEEL_DICT_UNITS = {
    "Iron ore": "t / t steel",
    "Scrap": "t / t steel",
    "DRI": "t / t steel",
    "Met coal": "t / t steel",
    "Coke": "GJ / t steel",
    "Thermal coal": "GJ / t steel",
    "BF gas": "GJ / t steel",
    "COG": "GJ / t steel",
    "BOF gas": "GJ / t steel",
    "Natural gas": "GJ / t steel",
    "Plastic waste": "GJ / t steel",
    "Biomass": "GJ / t steel",
    "Biomethane": "GJ / t steel",
    "Hydrogen": "GJ / t steel",
    "Electricity": "GJ / t steel",
    "Steam": "GJ / t steel",
    "BF slag": "kg / t steel",
    "Other slag": "kg / t steel",
    "Process emissions": "t CO2 / t steel",
    "Emissivity wout CCS": "t CO2 / t steel",
    "Captured CO2": "t CO2 / t steel",
    "Used CO2": "t CO2 / t steel",
    "Emissivity": "t CO2 / t steel",
}


# Define Groups
bosc_factor_group = (
    FURNACE_GROUP_DICT["blast_furnace"]
    + FURNACE_GROUP_DICT["smelting_reduction"]
    + FURNACE_GROUP_DICT["dri-bof"]
)
eaf_factor_group = FURNACE_GROUP_DICT["dri-eaf"] + FURNACE_GROUP_DICT["eaf-all"]
electricity_and_steam_self_gen_group = FURNACE_GROUP_DICT["smelting_reduction"]
electricity_self_gen_group = (
    FURNACE_GROUP_DICT["blast_furnace"] + FURNACE_GROUP_DICT["smelting_reduction"]
)

HARD_CODED_FACTORS = {
    "dri": {"Coke Production": 0, "Sintering": 0},
    "eaf-basic": {
        "Coke Production": 1,
        "Sintering": 1,
        "Pelletisation": 1,
        "Blast Furnace": 1,
    },
    "eaf-advanced": {
        "Coke Production": 0,
        "Sintering": 0,
    },
    "smelting_reduction": {
        "Coke Production": 0,
    },
}

hard_coded_factor_exceptions = create_hardcoded_exceptions(
    HARD_CODED_FACTORS, FURNACE_GROUP_DICT
)
TECHNOLOGY_PROCESSES = create_tech_processes_list()


def standardise_business_cases(serialize_only: bool = False) -> pd.DataFrame:
    """Standardises the business cases for each technology into per t steel.

    Args:
        serialize_only (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A tabular dataframe containing the standardised business cases
    """
    full_summary_df = generate_full_consumption_table(TECH_REFERENCE_LIST)
    if serialize_only:
        serialise_file(full_summary_df, PKL_FOLDER, "standardised_business_cases")
        return
    return full_summary_df


# s1_emissions_factors = read_pickle_folder(PKL_FOLDER, "s1_emissions_factors")
# EF_DICT = dict(zip(s1_emissions_factors["Metric"], s1_emissions_factors["Value"]))
# business_cases = read_pickle_folder(PKL_FOLDER, "business_cases")
# bc_parameters, bc_processes = business_case_formatter_splitter(business_cases)
# processes = bc_processes["process"].unique()
# standardise_business_cases(serialize_only=True)
