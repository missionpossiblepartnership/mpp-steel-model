"""Class to manage plant choices"""


import pandas as pd

from mppsteel.utility.log_utility import get_logger

logger = get_logger(__name__)


class PlantChoices:
    """Description
    Class to manage the state of each plant's technology choices.

    Main Class attributes
        choices: Keeps track of each plants choice in every year. A dictionary in the form [year][plant_name] -> technology
        records: A list of DataFrames that record why certain technologies were chosen or not chosen. The list can be outputted to a combined DataFrame.
        active_check: A dictionary that keeps track of whether a plant is active or not. A dictionary in the form [year][plant_name] -> boolean check
    """

    def __init__(self):
        self.choices = {}
        self.choice_records = []
        self.rank_records = []
        self.active_check = {}

    def initiate_container(self, year_range: range):
        for year in year_range:
            self.choices[year] = {}
            self.active_check[year] = {}

    def update_choice(self, year: int, plant: str, tech: str):
        self.choices[year][plant] = tech
        if tech != "Close plant":
            self.active_check[year][plant] = True
        if tech == "Close plant":
            self.active_check[year][plant] = False

    def remove_choice(self, year: int, plant: str):
        del self.choices[year][plant]

    def get_choice(self, year: int, plant: str):
        return self.choices[year][plant]

    def return_choices(self, year: int = None):
        return self.choices[year] if year else self.choices

    def return_nans(self, year: int):
        return [
            plant for plant in self.choices[year] if pd.isna(self.choices[year][plant])
        ]

    def update_records(self, record_type: str, df_entry: pd.DataFrame):
        if record_type == "choice":
            self.choice_records.append(df_entry)
        elif record_type == "rank":
            self.rank_records.append(df_entry)

    def output_records_to_df(self, record_type: str):
        if record_type == "choice":
            df = pd.DataFrame(self.choice_records).reset_index(drop=True)
            return df.drop_duplicates(keep="last")
        elif record_type == "rank":
            df = pd.DataFrame(self.rank_records).reset_index(drop=True)
            df = df[~df.index.duplicated(keep="first")]
            return combine_tech_ranks(df)


def combine_tech_ranks(tr_df: pd.DataFrame):
    container = [pd.concat(tr_df.values[count]) for count in range(len(tr_df.values))]
    if len(container) == 0:
        return pd.DataFrame(columns=["year", "start_tech"])
    df = pd.concat(container)
    return df.sort_values(by=["year", "start_tech"], ascending=True).reset_index()
