import itertools

from tqdm import tqdm
from mppsteel.config.model_config import (
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION, 
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION, 
    MODEL_YEAR_RANGE
)
from mppsteel.config.model_scenarios import TECH_MORATORIUM
from mppsteel.config.reference_lists import RESOURCE_CATEGORY_MAPPER, TECH_REFERENCE_LIST
from mppsteel.data_load_and_format.data_interface import create_capex_opex_dict
from mppsteel.data_load_and_format.reg_steel_demand_formatter import get_steel_demand
from mppsteel.data_load_and_format.steel_plant_formatter import create_active_check_col, steel_plant_processor
from mppsteel.data_preprocessing.variable_plant_cost_archetypes import PlantVariableCostsInput, format_variable_costs, plant_variable_costs
from mppsteel.model_solver.solver_classes import CapacityContainerClass, MarketContainerClass, PlantChoices, UtilizationContainerClass, create_wsa_2020_utilization_dict
from mppsteel.trade_module.trade_flow import trade_flow
import pandas as pd


def make_business_case(material_category, value, technology):
    """
    Create a business case with a given material_category and value.
    """
    return {
        "technology": technology,
        "material_category": material_category,
        "metric_type": "Purchased energy",
        "unit": "GJ/t steel",
        "value": value,
    }

def make_business_cases(values):
    return pd.DataFrame(
        [
            make_business_case(material_category, value, technology)
            for material_category, value, technology in values
        ]
    )


def make_input_data(feedstock_dict, static_energy_prices, business_cases, year, country_code, kwargs):
    """
    Create the input data for the plant_variable_costs function.
    """
    year_country = year, country_code
    input_kwargs = (
            dict(
                product_range_year_country=[year_country],
                business_cases=business_cases,
                resource_category_mapper=RESOURCE_CATEGORY_MAPPER.copy(),
                static_energy_prices=static_energy_prices,
                feedstock_dict=feedstock_dict,
                country_codes=[country_code],
                year_range=range(year, year + 1),
            )
            | kwargs
        )
    return PlantVariableCostsInput(**input_kwargs)

def get_feedstock_dict():
    return {
        "Plastic waste": 6.527621014136413,
        "Iron ore": 97.73,
        "Scrap": 224.46000000000004,
        "DRI": 300.67,
        "Coal": 121.86,
        "BF slag": -27.5,
        "Other slag": 0.0,
    }

def get_static_energy_prices():
    material_categories = [
        "Natural gas - low",
        "Natural gas - high",
        "Met coal",
        "Thermal coal",
        "COG",
        "Coke",
        "BF gas",
        "BOF gas",
        "Steam",
    ]
    energy_price_rows = [[mc, 2026, 0.0] for mc in material_categories]
    return pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])

def test_trade_flow():
    YEAR = 2020
    steel_demand_df = get_steel_demand(scenario_dict=TECH_MORATORIUM, from_csv=True)
    market_container = MarketContainerClass()
    regions = list(steel_demand_df["region"].unique())
    regions.remove("World")
    market_container.full_instantiation(MODEL_YEAR_RANGE, regions)
    utilization_container = UtilizationContainerClass()
    utilization_container.initiate_container(MODEL_YEAR_RANGE, regions)
    capacity_container = CapacityContainerClass()
    capacity_container.instantiate_container(MODEL_YEAR_RANGE)
    plant_df = steel_plant_processor(from_csv=True)
    
    country_codes = plant_df["country_code"].unique()

    variable_cost_df_list = []
    country_tech_product = list(itertools.product(country_codes, TECH_REFERENCE_LIST))
    for country_code, technology in tqdm(
        country_tech_product, total=len(country_tech_product), desc=""
    ):
        input_data = make_input_data(
            get_feedstock_dict(),
            get_static_energy_prices(),
            make_business_cases([
                ("Electricity", 1.2, technology),
                ("Scrap", 1.2, technology),
            ]),
            YEAR,
            country_code,
            kwargs={"bio_model_prices_ref": {(YEAR, country_code): 1.2}}
        )
        variable_cost_df_list.append(plant_variable_costs(input_data))
    combined_variable_cost_data = pd.concat(variable_cost_df_list).reset_index(drop=True)
    variable_cost_df = format_variable_costs(combined_variable_cost_data, group_data=True)
    capex_dict = create_capex_opex_dict(from_csv=True)
    tech_choices_ref = PlantChoices()
    tech_choices_ref.initiate_container(MODEL_YEAR_RANGE)
    wsa_dict = create_wsa_2020_utilization_dict(from_csv=True)
    plant_df["active_check"] = plant_df.apply(
        create_active_check_col, year=YEAR, axis=1
    )
    
    active_plant_df = plant_df[plant_df["active_check"] == True].copy()
    capacity_container.map_capacities(active_plant_df, YEAR)
    capacity_container.set_average_plant_capacity(active_plant_df)
    for row in active_plant_df.itertuples():
        tech_choices_ref.update_choice(
            YEAR, row.plant_name, row.initial_technology
        )
        utilization_container.assign_year_utilization(YEAR, wsa_dict)



    trade_flow(
        market_container,
        utilization_container,
        capacity_container,
        steel_demand_df,
        variable_cost_df,
        active_plant_df,
        capex_dict,
        tech_choices_ref.return_choices(),
        YEAR,
        CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
        CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    )
