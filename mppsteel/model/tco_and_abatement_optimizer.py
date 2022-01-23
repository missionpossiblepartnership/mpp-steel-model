"""Script for the tco and abatament optimisation functions."""

import pandas as pd
import numpy as np

from mppsteel.utility.utils import (
    get_logger,
)


from mppsteel.model_config import (
    TCO_RANK_1_SCALER, TCO_RANK_2_SCALER,
    ABATEMENT_RANK_2, ABATEMENT_RANK_3,
)

# Create logger
logger = get_logger("TCO & Abataement Optimsation Functions")


def normalise_data(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))

def scale_data(df: pd.DataFrame, reverse: bool = False):
    df_c = df.copy()
    if reverse:
        df_c = 1 - normalise_data(df_c.values)
    else:
        df_c = normalise_data(df_c.values)
    return df_c

def tco_ranker_logic(x: float, min_value: float):
    if min_value is None: # check for this
        # print('NoneType value')
        return 1
    if x > min_value * TCO_RANK_2_SCALER:
        return 3
    elif x > min_value * TCO_RANK_1_SCALER:
        return 2
    else:
        return 1

def abatement_ranker_logic(x: float):
    if x < ABATEMENT_RANK_3:
        return 3
    elif x < ABATEMENT_RANK_2:
        return 2
    else:
        return 1

def min_ranker(df: pd.DataFrame, value_col: str, data_type: str, year: int, plant_name: str, start_tech: str, rank: bool = False):
    df_subset = df.loc[year, plant_name, start_tech].copy()
    if len(df_subset) == 1:
        return df_subset.reset_index().set_index('switch_tech')[value_col]
    df_subset = df_subset.reset_index().set_index('switch_tech')
    df_subset.sort_values(value_col, ascending=True, inplace=True)
    if rank:
        if data_type == 'tco':
            min_value = df_subset[value_col].min()
            df_subset['tco_rank_score'] = df_subset[value_col].apply(lambda x: tco_ranker_logic(x, min_value))
        elif data_type == 'abatement':
            df_subset['abatement_rank_score'] = df_subset[value_col].apply(lambda x: abatement_ranker_logic(x))
        return df_subset
    return df_subset

def get_best_choice(tco_df: pd.DataFrame, emissions_df: pd.DataFrame, plant_name: str, year: int, start_tech: str, solver_logic: str, weighting_dict: dict, technology_list: list):
    # Choose Scaling algorithm
    if solver_logic == 'scaled':
        tco_values = min_ranker(
            df=tco_df,
            data_type='tco',
            value_col='tco',
            year=year,
            plant_name=plant_name,
            start_tech=start_tech,
            rank=False)
        abatement_values = min_ranker(
            df=emissions_df,
            data_type='abatement',
            value_col='abated_emissions_combined',
            year=year,
            plant_name=plant_name,
            start_tech=start_tech,
            rank=False)
        tco_values = tco_values.filter(items=technology_list, axis=0)
        abatement_values = abatement_values.filter(items=technology_list, axis=0)
        tco_values['tco_scaled'] = scale_data(tco_values['tco'])
        tco_values.drop(tco_values.columns.difference(['tco_scaled']), 1, inplace=True)
        abatement_values['abatement_scaled'] = scale_data(abatement_values['abated_emissions_combined'], reverse=True)
        abatement_values.drop(abatement_values.columns.difference(['abatement_scaled']), 1, inplace=True)
        combined_scales = tco_values.join(abatement_values)
        combined_scales['overall_score'] = (combined_scales['tco_scaled'] * weighting_dict['tco'])+ (combined_scales['abatement_scaled'] * weighting_dict['emissions'])
        combined_scales.sort_values('overall_score', axis=0, inplace=True)
        return combined_scales.idxmin()['overall_score']
    
    # Choose ranking algorithm
    if solver_logic == 'ranked':
        tco_values = min_ranker(
            df=tco_df,
            data_type='tco',
            value_col='tco',
            year=year,
            plant_name=plant_name,
            start_tech=start_tech,
            rank=True)
        abatement_values = min_ranker(
            df=emissions_df,
            data_type='abatement',
            value_col='abated_emissions_combined',
            year=year,
            plant_name=plant_name,
            start_tech=start_tech,
            rank=True)
        tco_values = tco_values.filter(items=technology_list, axis=0)
        abatement_values = abatement_values.filter(items=technology_list, axis=0)
        tco_values.drop(tco_values.columns.difference(['tco_rank_score']), 1, inplace=True)
        abatement_values.drop(abatement_values.columns.difference(['abatement_rank_score']), 1, inplace=True)
        combined_ranks = tco_values.join(abatement_values)
        combined_ranks['overall_rank'] = (combined_ranks['tco_rank_score'] * weighting_dict['tco'])+ (combined_ranks['abatement_rank_score'] * weighting_dict['emissions'])
        combined_ranks.sort_values('overall_rank', axis=0, inplace=True)
        min_value = combined_ranks['overall_rank'].min()
        best_values = combined_ranks[combined_ranks['overall_rank'] == min_value]
        # pick the value with the least tco if ranked scores are tied
        if len(best_values) > 1:
            tco_values_best_tech = tco_df[tco_df['switch_tech'].isin(best_values.index)]
            tco_values_min = min_ranker(
                df=tco_values_best_tech,
                data_type='tco',
                value_col='tco',
                year=year,
                plant_name=plant_name,
                start_tech=start_tech,
                rank=False)
            return tco_values_min[['tco']].idxmin().values[0]
        return combined_ranks.idxmin()['overall_rank']
