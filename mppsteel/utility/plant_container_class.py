"""Class for the Plant ID container."""

import random
import string
import pandas as pd

class PlantIdContainer:
    def __init__(self):
        self.id_container = []
        
    def __repr__(self):
        return "PlantIdContainer"
        
    def __str__(self):
        return "Instance of PlantIdContainer"

    def add_id(self, plant_id: str) -> None:
        self.id_container.append(plant_id)

    def check_id_exists(self, plant_id: str) -> None:
        return plant_id in self.id_container

    def remove_id(self, plant_id: str) -> None:
        if self.check_id_exists(plant_id):
            self.id_container.remove(plant_id)
            print(f'Plant ID removed {plant_id}')

    def generate_plant_id(self, add_to_container: bool = False, chars: list = string.digits, n: int = 5):
        rand_id = 'MPP' + ''.join(random.choice(chars) for _ in range(n))
        match_state = True
        while match_state:
            if rand_id not in self.id_container:
                if add_to_container:
                    self.add_id(rand_id)    
                match_state = False
        return rand_id

    def return_ids(self) -> None:
        self.remove_duplicate_ids()
        return self.id_container
    
    def clear_ids(self) -> None:
        self.id_container = []
        
    def remove_duplicate_ids(self) -> None:
        self.id_container = list(set(self.id_container))
        
    def remove_duplicate_ids(self) -> None:
            self.id_container = list(set(self.id_container))

    def add_steel_plant_ids(self, plant_df: pd.DataFrame):
        plant_df['plant_id'].apply(lambda x: self.add_id(x))
