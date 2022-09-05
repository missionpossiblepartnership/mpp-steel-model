"""Class for the Plant ID container."""

import pandas as pd
from mppsteel.utility.utils import generate_random_string_with_prefix


class PlantIdContainer:
    """A class to manage the creation and track the existence of Plant IDs.
    Plant Ids are stored in a list container.
    New plant Ids are generated and checked if they exist already in the list container before being added to the list.
    """

    def __init__(self):
        self.id_container = []

    def __repr__(self) -> str:
        return "PlantIdContainer"

    def __str__(self) -> str:
        return "Instance of PlantIdContainer"

    def add_id(self, plant_id: str) -> None:
        self.id_container.append(plant_id)

    def check_id_exists(self, plant_id: str) -> bool:
        return plant_id in self.id_container

    def remove_id(self, plant_id: str) -> None:
        if self.check_id_exists(plant_id):
            self.id_container.remove(plant_id)
            print(f"Plant ID removed {plant_id}")

    def generate_plant_id(self, add_to_container: bool = False) -> str:
        unmatched = True
        while unmatched:
            new_id = generate_random_string_with_prefix()
            if new_id not in self.id_container:
                if add_to_container:
                    self.add_id(new_id)
                unmatched = False
        return new_id

    def return_ids(self) -> None:
        self.remove_duplicate_ids()
        return self.id_container

    def clear_ids(self) -> None:
        self.id_container = []

    def remove_duplicate_ids(self) -> None:
        self.id_container = list(set(self.id_container))

    def add_steel_plant_ids(self, plant_df: pd.DataFrame) -> pd.DataFrame:
        plant_df["plant_id"].apply(lambda x: self.add_id(x))
