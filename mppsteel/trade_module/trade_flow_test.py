import pytest

from mppsteel.config.model_config import (
    CAPACITY_UTILIZATION_CUTOFF_FOR_CLOSING_PLANT_DECISION, 
    CAPACITY_UTILIZATION_CUTOFF_FOR_NEW_PLANT_DECISION, 
    MODEL_YEAR_RANGE
)
from mppsteel.config.model_scenarios import TECH_MORATORIUM
from mppsteel.config.reference_lists import REGION_LIST, RESOURCE_CATEGORY_MAPPER
from mppsteel.data_load_and_format.data_interface import create_business_case_reference, create_capex_opex_dict
from mppsteel.data_load_and_format.reg_steel_demand_formatter import get_steel_demand
from mppsteel.data_load_and_format.steel_plant_formatter import create_active_check_col, steel_plant_processor
from mppsteel.data_preprocessing.variable_plant_cost_archetypes import PlantVariableCostsInput, format_variable_costs, plant_variable_costs
from mppsteel.plant_classes.plant_choices_class import PlantChoices
from mppsteel.plant_classes.capacity_container_class import CapacityContainerClass
from mppsteel.model_solver.market_container_class import MarketContainerClass
from mppsteel.plant_classes.regional_utilization_class import (
    UtilizationContainerClass, create_wsa_2020_utilization_dict
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


def make_input_data(feedstock_dict: dict, static_energy_prices: pd.DataFrame, business_cases: pd.DataFrame, year: int, country_code: str, kwargs: dict):
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
    plant_df = steel_plant_processor(from_csv=True)
    business_case_df, _ = create_business_case_reference(from_csv=True)
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
        assert production_demand_dict[region] == pytest.approx(RESULT_DICT[region]), f"Region: {region} failed"

RESULT_DICT = {
        'Africa': {'avg_plant_capacity': 2.357,
            'capacity': 35.577,
            'demand': 40.30256228,
            'initial_balance': -6.504412279999997,
            'initial_utilization': 0.95,
            'initial_utilized_capacity': 33.79815,
            'new_balance': 0.21303771999999555,
            'new_capacity_required': 6.504412279999997,
            'new_total_capacity': 42.647999999999996,
            'new_utilization': 0.9499999999999998,
            'new_utilized_capacity': 40.51559999999999,
            'plants_required': 3,
            'plants_to_close': 0,
            'region': 'Africa',
            'unit': 'Mt',
            'year': 2020},
        'CIS': {'avg_plant_capacity': 2.357,
                'capacity': 147.67344617882821,
                'demand': 62.90581066,
                'initial_balance': 67.04682197736882,
                'initial_utilization': 0.88,
                'initial_utilized_capacity': 129.95263263736882,
                'new_balance': 7.105427357601002e-15,
                'new_capacity_required': -42.83042841216154,
                'new_total_capacity': 102.89044617882821,
                'new_utilization': 0.6113863142421103,
                'new_utilized_capacity': 62.90581066000001,
                'plants_required': 0,
                'plants_to_close': 19,
                'region': 'CIS',
                'unit': 'Mt',
                'year': 2020},
        'China': {'avg_plant_capacity': 2.357,
                'capacity': 1129.5539082252988,
                'demand': 1004.000731,
                'initial_balance': -9.993291761737055,
                'initial_utilization': 0.88,
                'initial_utilized_capacity': 994.0074392382629,
                'new_balance': 48.923210914054955,
                'new_capacity_required': 0,
                'new_total_capacity': 1129.5539082252988,
                'new_utilization': 0.9321590888639912,
                'new_utilized_capacity': 1052.923941914055,
                'plants_required': 0,
                'plants_to_close': 0,
                'region': 'China',
                'unit': 'Mt',
                'year': 2020},
        'Europe': {'avg_plant_capacity': 2.357,
                'capacity': 191.141,
                'demand': 207.8270382,
                'initial_balance': -66.38269820000002,
                'initial_utilization': 0.74,
                'initial_utilized_capacity': 141.44433999999998,
                'new_balance': -26.243088200000017,
                'new_capacity_required': 0,
                'new_total_capacity': 191.141,
                'new_utilization': 0.95,
                'new_utilized_capacity': 181.58395,
                'plants_required': 0,
                'plants_to_close': 0,
                'region': 'Europe',
                'unit': 'Mt',
                'year': 2020},
        'India': {'avg_plant_capacity': 2.357,
                'capacity': 125.26951337329906,
                'demand': 119.475475,
                'initial_balance': -30.53412050495767,
                'initial_utilization': 0.71,
                'initial_utilized_capacity': 88.94135449504233,
                'new_balance': 1.7697127046341024,
                'new_capacity_required': 0.46943729536589274,
                'new_total_capacity': 127.62651337329906,
                'new_utilization': 0.95,
                'new_utilized_capacity': 121.2451877046341,
                'plants_required': 1,
                'plants_to_close': 0,
                'region': 'India',
                'unit': 'Mt',
                'year': 2020},
        'Japan, South Korea, and Taiwan': {'avg_plant_capacity': 2.357,
                                        'capacity': 215.6194820596094,
                                        'demand': 145.1815759,
                                        'initial_balance': -15.80988666423437,
                                        'initial_utilization': 0.6,
                                        'initial_utilized_capacity': 129.37168923576564,
                                        'new_balance': 0.0,
                                        'new_capacity_required': 0,
                                        'new_total_capacity': 215.6194820596094,
                                        'new_utilization': 0.6733230898860226,
                                        'new_utilized_capacity': 145.1815759,
                                        'plants_required': 0,
                                        'plants_to_close': 0,
                                        'region': 'Japan, South Korea, and Taiwan',
                                        'unit': 'Mt',
                                        'year': 2020},
        'Middle East': {'avg_plant_capacity': 2.357,
                        'capacity': 71.64942256637168,
                        'demand': 55.01922818,
                        'initial_balance': -9.163597737522117,
                        'initial_utilization': 0.64,
                        'initial_utilized_capacity': 45.85563044247788,
                        'new_balance': 13.047723258053097,
                        'new_capacity_required': 0,
                        'new_total_capacity': 71.64942256637168,
                        'new_utilization': 0.95,
                        'new_utilized_capacity': 68.0669514380531,
                        'plants_required': 0,
                        'plants_to_close': 0,
                        'region': 'Middle East',
                        'unit': 'Mt',
                        'year': 2020},
        'NAFTA': {'avg_plant_capacity': 2.357,
                'capacity': 144.736,
                'demand': 149.4419476,
                'initial_balance': -35.1005076,
                'initial_utilization': 0.79,
                'initial_utilized_capacity': 114.34143999999999,
                'new_balance': -11.942747600000018,
                'new_capacity_required': 0,
                'new_total_capacity': 144.736,
                'new_utilization': 0.95,
                'new_utilized_capacity': 137.49919999999997,
                'plants_required': 0,
                'plants_to_close': 0,
                'region': 'NAFTA',
                'unit': 'Mt',
                'year': 2020},
        'RoW': {'avg_plant_capacity': 2.357,
                'capacity': 78.66635080480934,
                'demand': 28.617724,
                'initial_balance': 18.582086482885604,
                'initial_utilization': 0.6,
                'initial_utilized_capacity': 47.1998104828856,
                'new_balance': 3.552713678800501e-15,
                'new_capacity_required': -30.970144138142672,
                'new_total_capacity': 45.668350804809336,
                'new_utilization': 0.626642379146003,
                'new_utilized_capacity': 28.617724000000003,
                'plants_required': 0,
                'plants_to_close': 14,
                'region': 'RoW',
                'unit': 'Mt',
                'year': 2020},
        'South and Central America': {'avg_plant_capacity': 2.357,
                                    'capacity': 60.35648205960941,
                                    'demand': 45.91537291,
                                    'initial_balance': -7.287224391849975,
                                    'initial_utilization': 0.64,
                                    'initial_utilized_capacity': 38.62814851815003,
                                    'new_balance': 0.0,
                                    'new_capacity_required': 0,
                                    'new_total_capacity': 60.35648205960941,
                                    'new_utilization': 0.7607364005187206,
                                    'new_utilized_capacity': 45.91537291,
                                    'plants_required': 0,
                                    'plants_to_close': 0,
                                    'region': 'South and Central America',
                                    'unit': 'Mt',
                                    'year': 2020},
        'Southeast Asia': {'avg_plant_capacity': 2.357,
                        'capacity': 71.71096411921881,
                        'demand': 93.89326471,
                        'initial_balance': -25.76784879674213,
                        'initial_utilization': 0.95,
                        'initial_utilized_capacity': 68.12541591325787,
                        'new_balance': -25.76784879674213,
                        'new_capacity_required': 0,
                        'new_total_capacity': 71.71096411921881,
                        'new_utilization': 0.95,
                        'new_utilized_capacity': 68.12541591325787,
                        'plants_required': 0,
                        'plants_to_close': 0,
                        'region': 'Southeast Asia',
                        'unit': 'Mt',
                        'year': 2020}
    }
