"""Script to determine when investments will take place."""


import pandas as pd

from mppsteel.config.model_config import (
    PKL_DATA_FORMATTED,
)

from mppsteel.utility.function_timer_utility import timer_func
from mppsteel.utility.file_handling_utility import read_pickle_folder, serialize_file
from mppsteel.utility.log_utility import get_logger

from mppsteel.plant_classes.plant_investment_cycle_class import PlantInvestmentCycle

# Create logger
logger = get_logger(__name__)

@timer_func
def investment_cycle_flow(serialize: bool = False) -> pd.DataFrame:
    """Inintiates the complete investment cycle flow and serializes the resulting DataFrame.

    Args:
        serialize (bool, optional): Flag to only serialize the DataFrame to a pickle file and not return a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the Complete Investment Decision Cycle Reference.
    """
    steel_plant_df = read_pickle_folder(
        PKL_DATA_FORMATTED, "steel_plants_processed", "df"
    )

    PlantInvestmentCycles = PlantInvestmentCycle()
    steel_plant_names = steel_plant_df["plant_name"].to_list()
    start_plant_years = steel_plant_df["start_of_operation"].to_list()
    PlantInvestmentCycles.instantiate_plants(steel_plant_names, start_plant_years)
    PlantInvestmentCycles.test_cycle_lengths()
    print(PlantInvestmentCycles.plant_cycles_with_off_cycle)

    if serialize:
        logger.info("-- Serializing Investment Cycle Reference")
        serialize_file(
            PlantInvestmentCycles,
            PKL_DATA_FORMATTED,
            "plant_investment_cycle_container",
        )
    return PlantInvestmentCycles
