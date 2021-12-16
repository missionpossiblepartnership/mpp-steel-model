"""Script to determine when investments will take place."""

import random
import pandas as pd
from tqdm import tqdm

from mppsteel.model_config import (
    PKL_FOLDER, MODEL_YEAR_END, NET_ZERO_TARGET,
)

from mppsteel.model.solver import (
    generate_formatted_steel_plants,
)

from mppsteel.utility.utils import (
    serialize_file, get_logger
)
# Create logger
logger = get_logger("Investment Cycles")

def calculate_investment_years(
    op_start_year: int, cutoff_start_year: int = 2020,
    cutoff_end_year: int = MODEL_YEAR_END, inv_intervals: int = 20
):
    logger.info('Calculating investment years')
    x = op_start_year
    decision_years = []
    unique_investment_interval = inv_intervals + random.randrange(-3, 3, 1)
    while x < cutoff_end_year:
        if x < cutoff_start_year:
            x+=unique_investment_interval
        elif x >= cutoff_start_year:
            decision_years.append(x)
            x+=unique_investment_interval
    return decision_years

def add_off_cycle_investment_years(
    main_investment_cycle: list,
    start_buff: int = 3, end_buff: int = 8,
):
    inv_cycle_length = len(main_investment_cycle)
    range_list = []

    # For inv_cycle_length = 2
    first_year = main_investment_cycle[0]
    second_year = main_investment_cycle[1]
    first_range = range(first_year+start_buff, second_year-end_buff)
    range_list.append(first_year)
    range_list.append(first_range)
    range_list.append(second_year)

    if inv_cycle_length == 3:
        third_year = main_investment_cycle[2]
        second_range = range(second_year+start_buff, third_year-end_buff)
        range_list.append(second_range)
        range_list.append(third_year)

    if inv_cycle_length == 4:
        fourth_year = main_investment_cycle[3]
        third_range = range(third_year+start_buff, third_year-fourth_year)
        range_list.append(third_range)
        range_list.append(fourth_year)

    return range_list

def apply_investment_years(year_value):
    if pd.isna(year_value):
        return calculate_investment_years(2020)
    elif '(anticipated)' in str(year_value):
        year_value = year_value[:4]
        return calculate_investment_years(int(year_value))
    else:
        try:
            return calculate_investment_years(int(float(year_value)))
        except:
            return calculate_investment_years(int(year_value[:4]))

def create_investment_cycle_reference(plant_names: list, investment_years: list, year_end: int):
    logger.info('Creating the investment cycle reference table')
    zipped_plant_investments = zip(plant_names, investment_years)
    year_range = range(2020, year_end+1)
    df = pd.DataFrame(columns=['plant_name', 'year', 'switch_type'])

    for plant_name, investments in tqdm(zipped_plant_investments, total=len(plant_names), desc='Investment Cycles'):
        for year in year_range:
            row_dict = {'plant_name': plant_name, 'year': year, 'switch_type': 'no switch'}
            if year in [inv_year for inv_year in investments if isinstance(inv_year, int)]:
                row_dict['switch_type'] = 'main cycle'
            for range_object in [inv_range for inv_range in investments if isinstance(inv_range, range)]:
                if year in range_object:
                    row_dict['switch_type'] = 'trans switch'
            df=df.append(row_dict, ignore_index=True)
    return df.set_index(['year', 'switch_type'])

def create_investment_cycle_ref(steel_plant_df: pd.DataFrame):
    logger.info('Creating investment cycle')
    investment_years = steel_plant_df['start_of_operation'].apply(lambda year: apply_investment_years(year))
    investment_years_inc_off_cycle = [add_off_cycle_investment_years(inv_year) for inv_year in investment_years]
    return create_investment_cycle_reference(steel_plant_df['plant_name'].values, investment_years_inc_off_cycle, MODEL_YEAR_END)

def investment_cycle_flow(serialize_only: bool = False):
    steel_plants_aug = generate_formatted_steel_plants()
    plant_investment_cycles = create_investment_cycle_ref(steel_plants_aug)

    if serialize_only:
        logger.info(f'-- Serializing Investment Cycle Reference')
        serialize_file(plant_investment_cycles, PKL_FOLDER, "plant_investment_cycles")
    return plant_investment_cycles
