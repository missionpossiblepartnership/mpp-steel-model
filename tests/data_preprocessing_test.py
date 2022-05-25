import pandas as pd
import pytest

# from mppsteel.config.model_scenarios import DEFAULT_SCENARIO
# from mppsteel.config.model_config import USD_TO_EUR_CONVERSION_DEFAULT
from mppsteel.data_preprocessing.variable_plant_cost_archetypes import PlantVariableCostsInput, plant_variable_costs


# @pytest.fixture
# def input_data():
#     scenario_dict = DEFAULT_SCENARIO.copy()
#     scenario_dict["usd_to_eur"] = USD_TO_EUR_CONVERSION_DEFAULT
#     scenario_dict["eur_to_usd"] = 1.0 / scenario_dict["usd_to_eur"]
#     return PlantVariableCostsInput.from_filesystem(scenario_dict)
#
#
# def test_plant_variable_costs(input_data):
#     df = plant_variable_costs(input_data)
#     assert False


@pytest.fixture
def business_case():
    return {
        "technology": "Avg BF-BOF",
        "material_category": "Electricity",
        "metric_type": "Purchased energy",
        "unit": "GJ/t steel",
        "value": 0.0,
    }


def test_plant_variable_costs_electricity(business_case):
    """
    Assert that cost is calculated correctly for electricity material_category.
    """
    value, power_grid_price = 1.0, 0.5
    expected = value * power_grid_price
    year_country = 2020, "DEU"
    business_case |= {"value": value, "material_category": "Electricity"}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        power_grid_prices_ref={year_country: power_grid_price},
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected

