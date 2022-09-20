import pytest

from mppsteel.config.model_config import (
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION,
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION,
    MODEL_YEAR_RANGE,
)
from mppsteel.config.model_scenarios import TECH_MORATORIUM
from mppsteel.config.reference_lists import REGION_LIST, RESOURCE_CATEGORY_MAPPER
from mppsteel.data_load_and_format.data_interface import (
    create_business_case_reference,
    create_capex_opex_dict,
)
from mppsteel.data_load_and_format.reg_steel_demand_formatter import get_steel_demand
from mppsteel.data_load_and_format.steel_plant_formatter import (
    create_active_check_col,
    steel_plant_processor,
)
from mppsteel.data_preprocessing.variable_plant_cost_archetypes import (
    PlantVariableCostsInput,
    format_variable_costs,
    plant_variable_costs,
)
from mppsteel.plant_classes.plant_choices_class import PlantChoices
from mppsteel.plant_classes.capacity_container_class import CapacityContainerClass
from mppsteel.model_solver.market_container_class import MarketContainerClass
from mppsteel.plant_classes.regional_utilization_class import (
    UtilizationContainerClass,
    create_wsa_2020_utilization_dict,
)
from mppsteel.trade_module.trade_flow import trade_flow
import pandas as pd


def make_business_case(material_category: str, value: float, technology: str) -> dict:
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


def make_business_cases(values: list) -> pd.DataFrame:
    return pd.DataFrame(
        [
            make_business_case(material_category, value, technology)
            for material_category, value, technology in values
        ]
    )


def make_input_data(
    feedstock_dict: dict,
    static_energy_prices: pd.DataFrame,
    business_cases: pd.DataFrame,
    year: int,
    country_code: str,
    kwargs: dict,
):
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


def get_feedstock_dict() -> dict:
    return {
        "Plastic waste": 6.527621014136413,
        "Iron ore": 97.73,
        "Scrap": 224.46000000000004,
        "DRI": 300.67,
        "Coal": 121.86,
        "BF slag": -27.5,
        "Other slag": 0.0,
    }


def get_static_energy_prices() -> pd.DataFrame:
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
    plant_df = steel_plant_processor(scenario_dict=TECH_MORATORIUM, from_csv=True)
    business_case_df, _ = create_business_case_reference(serialize=False, from_csv=True)
    business_case_df.reset_index(inplace=True)

    country_codes = plant_df["country_code"].unique()
    variable_cost_df_list = []
    for country_code in country_codes:
        input_data = make_input_data(
            get_feedstock_dict(),
            get_static_energy_prices(),
            business_case_df,
            YEAR,
            country_code,
            kwargs={"bio_model_prices_ref": {(YEAR, country_code): 1.2}},
        )
        variable_cost_df_list.append(plant_variable_costs(input_data))
    combined_variable_cost_data = pd.concat(variable_cost_df_list).reset_index(
        drop=True
    )
    variable_cost_df = format_variable_costs(
        combined_variable_cost_data, group_data=True
    )
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
        tech_choices_ref.update_choice(YEAR, row.plant_name, row.initial_technology)
        utilization_container.assign_year_utilization(YEAR, wsa_dict)
    production_demand_dict = trade_flow(
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
    for region in REGION_LIST:
        assert production_demand_dict[region] == pytest.approx(
            RESULT_DICT[region]
        ), f"Region: {region} test failed"


RESULT_DICT = {
    "Africa": {
        "year": 2020,
        "region": "Africa",
        "capacity": 35.577,
        "initial_utilized_capacity": 21.3462,
        "demand": 17.340066,
        "initial_balance": 4.006133999999999,
        "initial_utilization": 0.6,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": -7.071000000000001,
        "plants_required": 0,
        "plants_to_close": 3,
        "new_total_capacity": 28.505999999999997,
        "new_utilized_capacity": 17.340066,
        "new_balance": 0.0,
        "new_utilization": 0.6082953062513156,
        "unit": "Mt",
        "cases": [
            "R0: CHEAP EXCESS SUPPLY -> export",
            "R4-B: EXCESS SUPPLY, MIN UTILZATION -> close plants",
        ],
    },
    "China": {
        "year": 2020,
        "region": "China",
        "capacity": 1129.5539082252988,
        "initial_utilized_capacity": 1061.7806737317808,
        "demand": 1064.732,
        "initial_balance": -2.9513262682191908,
        "initial_utilization": 0.94,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 1129.5539082252988,
        "new_utilized_capacity": 1064.732,
        "new_balance": 0.0,
        "new_utilization": 0.9426128246263661,
        "unit": "Mt",
        "cases": ["R0: Domestic Producer -> Adjust Utilization"],
    },
    "CIS": {
        "year": 2020,
        "region": "CIS",
        "capacity": 147.67344617882824,
        "initial_utilized_capacity": 100.41794340160321,
        "demand": 100.215529,
        "initial_balance": 0.20241440160320678,
        "initial_utilization": 0.68,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 147.67344617882824,
        "new_utilized_capacity": 100.215529,
        "new_balance": 0.0,
        "new_utilization": 0.6786293107742737,
        "unit": "Mt",
        "cases": [
            "R0: CHEAP EXCESS SUPPLY -> export",
            "R1: Reducing excess production via lowering utilization",
        ],
    },
    "Europe": {
        "year": 2020,
        "region": "Europe",
        "capacity": 191.141,
        "initial_utilized_capacity": 143.35575,
        "demand": 143.475987,
        "initial_balance": -0.12023700000000304,
        "initial_utilization": 0.75,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 191.141,
        "new_utilized_capacity": 143.475987,
        "new_balance": 0.0,
        "new_utilization": 0.7506290487127304,
        "unit": "Mt",
        "cases": [
            "R0: INSUFFICIENT SUPPLY, EXPENSIVE REGION, MAX UTILIZATION -> import"
        ],
    },
    "India": {
        "year": 2020,
        "region": "India",
        "capacity": 125.26951337329909,
        "initial_utilized_capacity": 100.21561069863928,
        "demand": 100.25647,
        "initial_balance": -0.0408593013607117,
        "initial_utilization": 0.8,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 125.26951337329909,
        "new_utilized_capacity": 100.25647,
        "new_balance": 0.0,
        "new_utilization": 0.8003261711509884,
        "unit": "Mt",
        "cases": ["R0: Domestic Producer -> Adjust Utilization"],
    },
    "Japan, South Korea, and Taiwan": {
        "year": 2020,
        "region": "Japan, South Korea, and Taiwan",
        "capacity": 215.6194820596094,
        "initial_utilized_capacity": 170.33939082709142,
        "demand": 171.224164,
        "initial_balance": -0.8847731729085808,
        "initial_utilization": 0.79,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 215.6194820596094,
        "new_utilized_capacity": 171.224164,
        "new_balance": 0.0,
        "new_utilization": 0.7941034008822263,
        "unit": "Mt",
        "cases": [
            "R0: INSUFFICIENT SUPPLY, EXPENSIVE REGION, MAX UTILIZATION -> import"
        ],
    },
    "Middle East": {
        "year": 2020,
        "region": "Middle East",
        "capacity": 71.64942256637168,
        "initial_utilized_capacity": 45.13913621681416,
        "demand": 45.360151,
        "initial_balance": -0.22101478318584356,
        "initial_utilization": 0.63,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 71.64942256637168,
        "new_utilized_capacity": 45.360151,
        "new_balance": 0.0,
        "new_utilization": 0.633084669426067,
        "unit": "Mt",
        "cases": ["R0: Domestic Producer -> Adjust Utilization"],
    },
    "NAFTA": {
        "year": 2020,
        "region": "NAFTA",
        "capacity": 144.736,
        "initial_utilized_capacity": 99.86783999999999,
        "demand": 100.520687,
        "initial_balance": -0.6528470000000084,
        "initial_utilization": 0.69,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 144.736,
        "new_utilized_capacity": 100.520687,
        "new_balance": 0.0,
        "new_utilization": 0.6945106055162503,
        "unit": "Mt",
        "cases": [
            "R0: INSUFFICIENT SUPPLY, EXPENSIVE REGION, MAX UTILIZATION -> import"
        ],
    },
    "RoW": {
        "year": 2020,
        "region": "RoW",
        "capacity": 78.66635080480934,
        "initial_utilized_capacity": 47.1998104828856,
        "demand": 47.084392,
        "initial_balance": 0.11541848288560175,
        "initial_utilization": 0.6,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": -2.357,
        "plants_required": 0,
        "plants_to_close": 1,
        "new_total_capacity": 76.30935080480934,
        "new_utilized_capacity": 47.084392,
        "new_balance": 0.0,
        "new_utilization": 0.6170199523835097,
        "unit": "Mt",
        "cases": ["R0: EXPENSIVE EXCESS SUPPLY -> close plant"],
    },
    "South and Central America": {
        "year": 2020,
        "region": "South and Central America",
        "capacity": 60.35648205960941,
        "initial_utilized_capacity": 39.23171333874612,
        "demand": 39.195935,
        "initial_balance": 0.03577833874611969,
        "initial_utilization": 0.65,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 60.35648205960941,
        "new_utilized_capacity": 39.195935,
        "new_balance": 0.0,
        "new_utilization": 0.6494072163001352,
        "unit": "Mt",
        "cases": [
            "R0: CHEAP EXCESS SUPPLY -> export",
            "R1: Reducing excess production via lowering utilization",
        ],
    },
    "Southeast Asia": {
        "year": 2020,
        "region": "Southeast Asia",
        "capacity": 71.71096411921883,
        "initial_utilized_capacity": 50.91478452464536,
        "demand": 51.005065,
        "initial_balance": -0.09028047535463912,
        "initial_utilization": 0.71,
        "avg_plant_capacity": 2.357,
        "new_capacity_required": 0,
        "plants_required": 0,
        "plants_to_close": 0,
        "new_total_capacity": 71.71096411921883,
        "new_utilized_capacity": 51.00506500000001,
        "new_balance": 7.105427357601002e-15,
        "new_utilization": 0.7112589494014409,
        "unit": "Mt",
        "cases": ["R0: Domestic Producer -> Adjust Utilization"],
    },
}
