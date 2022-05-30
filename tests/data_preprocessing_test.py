import pandas as pd
import pytest

from mppsteel.data_preprocessing.variable_plant_cost_archetypes import PlantVariableCostsInput, plant_variable_costs


@pytest.fixture
def business_case():
    return {
        "technology": "Avg BF-BOF",
        "material_category": "Electricity",
        "metric_type": "Purchased energy",
        "unit": "GJ/t steel",
        "value": 0.0,
    }


@pytest.fixture
def make_business_case():
    business_case_template = {
        "technology": "Avg BF-BOF",
        "material_category": "Electricity",
        "metric_type": "Purchased energy",
        "unit": "GJ/t steel",
        "value": 0.0,
    }

    def _make_business_case(material_category, value):
        return business_case_template.copy() | {"value": value, "material_category": material_category}

    return _make_business_case


@pytest.fixture
def make_input_data():
    def _make_input_data(business_cases, year, country_code):
        year_country = year, country_code
        return PlantVariableCostsInput(
            product_range_year_country=[year_country],
            business_cases=business_cases,
            steel_plant_region_ng_dict={"DEU": 0},
        )

    return _make_input_data


def test_plant_variable_costs_emissivity(make_business_case, make_input_data):
    """
    Assert that cost is calculated correctly for the emissivity material_category.
    This category is special, because it is not handled by the model
    and should trigger the default price of 0.
    """
    value, price,  = 1.0, 0.0
    expected = value * price
    business_cases = pd.DataFrame([make_business_case("Emissivity", value)])
    input_data = make_input_data(business_cases, 2020, "DEU")
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


def test_plant_variable_costs_electricity(business_case):
    """
    Assert that cost is calculated correctly for the electricity material_category.
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


def test_plant_variable_costs_hydrogen(business_case):
    """
    Assert that cost is calculated correctly for the hydrogen material_category.
    """
    value, h2_price = 1.0, 0.5
    expected = value * h2_price
    year_country = 2020, "DEU"
    business_case |= {"value": value, "material_category": "Hydrogen"}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        h2_prices_ref={year_country: h2_price},
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


def test_plant_variable_costs_biomass(business_case):
    """
    Assert that cost is calculated correctly for the biomass material_category.
    """
    value, bio_price = 1.0, 0.5
    expected = value * bio_price
    year_country = 2020, "DEU"
    business_case |= {"value": value, "material_category": "Biomass"}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        bio_model_prices_ref={year_country: bio_price},
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


def test_plant_variable_costs_captured_co2(business_case):
    """
    Assert that cost is calculated correctly for the captured_co2 material_category.
    """
    value, transport_price, storage_price = 1.0, 0.25, 0.25
    expected = value * (transport_price + storage_price)
    year_country = 2020, "DEU"
    business_case |= {"value": value, "material_category": "Captured CO2"}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        ccs_model_transport_ref={year_country[-1]: transport_price},
        ccs_model_storage_ref={year_country[-1]: storage_price},
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


def test_plant_variable_costs_natural_gas(business_case):
    """
    Assert that cost is calculated correctly for the natural gas material_category.
    """
    value, energy_price, year, country_code = 1.0, 0.5, 2020, "DEU"
    year_country = year, country_code
    expected = value * energy_price
    energy_price_rows = [
        ["Natural gas - low", year, energy_price],
        ["Natural gas - high", year, energy_price],
    ]
    static_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    business_case |= {"value": value, "material_category": "Natural gas"}
    business_cases = pd.DataFrame([business_case])
    for ng_flag in (0, 1):
        # 1: low, 0: high
        input_data = PlantVariableCostsInput(
            product_range_year_country=[year_country],
            business_cases=business_cases,
            static_energy_prices=static_energy_prices,
            steel_plant_region_ng_dict={"DEU": ng_flag},
        )
        df = plant_variable_costs(input_data)
        actual = df.cost.values[0]
        assert actual == expected


def test_plant_variable_costs_plastic_waste(business_case):
    """
    Assert that cost is calculated correctly for the plastic_waste material_category.
    """
    value, plastic_waste_price = 1.0, 0.5
    expected = value * plastic_waste_price
    year_country = 2020, "DEU"
    business_case |= {"value": value, "material_category": "Plastic waste"}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        feedstock_dict={"Plastic waste": plastic_waste_price},
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


def test_plant_variable_costs_thermal_coal(business_case):
    """
    Assert that cost is calculated correctly for the thermal coal material_category.
    """
    value, energy_price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Thermal coal"
    year_country = year, country_code
    expected = value * energy_price
    energy_price_rows = [
        [material_category, year, energy_price],
    ]
    static_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    business_case |= {"value": value, "material_category": material_category}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        static_energy_prices=static_energy_prices,
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


def test_plant_variable_costs_iron_ore(business_case):
    """
    Assert that cost is calculated correctly for the iron ore material_category.
    """
    value, iron_ore_price = 1.0, 0.5
    material_category = "Iron ore"
    expected = value * iron_ore_price
    year_country = 2020, "DEU"
    business_case |= {"value": value, "material_category": material_category}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        feedstock_dict={material_category: iron_ore_price},
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


def test_plant_variable_costs_bf_slag(business_case):
    """
    Assert that cost is calculated correctly for the bf_slag material_category.
    """
    value, price = 1.0, 0.5
    material_category = "BF slag"
    expected = value * price
    year_country = 2020, "DEU"
    business_case |= {"value": value, "material_category": material_category}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        feedstock_dict={material_category: price},
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


def test_plant_variable_costs_steam(business_case):
    """
    Assert that cost is calculated correctly for the steam material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Steam"
    year_country = year, country_code
    expected = value * price
    energy_price_rows = [
        [material_category, year, price],
    ]
    static_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    business_case |= {"value": value, "material_category": material_category}
    business_cases = pd.DataFrame([business_case])
    input_data = PlantVariableCostsInput(
        product_range_year_country=[year_country],
        business_cases=business_cases,
        static_energy_prices=static_energy_prices,
        steel_plant_region_ng_dict={"DEU": 0},
    )
    df = plant_variable_costs(input_data)
    actual = df.cost.values[0]
    assert actual == expected


