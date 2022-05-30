import pandas as pd
import pytest

from mppsteel.data_preprocessing.variable_plant_cost_archetypes import (
    PlantVariableCostsInput,
    plant_variable_costs,
)


@pytest.fixture
def make_business_case():
    """
    Create a business case with a given material_category and value.
    """
    business_case_template = {
        "technology": "Avg BF-BOF",
        "material_category": "Electricity",
        "metric_type": "Purchased energy",
        "unit": "GJ/t steel",
        "value": 0.0,
    }

    def _make_business_case(material_category, value):
        return business_case_template.copy() | {
            "value": value,
            "material_category": material_category,
        }

    return _make_business_case


@pytest.fixture
def make_business_cases(make_business_case):
    """
    Create a DataFrame from a list of business cases which where created from a
    list of given material_categories and values.
    """

    def _make_business_cases(values):
        return pd.DataFrame(
            [
                make_business_case(material_category, value)
                for material_category, value in values
            ]
        )

    return _make_business_cases


@pytest.fixture
def make_input_data():
    """
    Create the input data for the plant_variable_costs function.
    """

    def _make_input_data(business_cases, year, country_code, **kwargs):
        year_country = year, country_code
        input_kwargs = (
            dict(
                product_range_year_country=[year_country],
                business_cases=business_cases,
                steel_plant_region_ng_dict={"DEU": 0},
            )
            | kwargs
        )
        return PlantVariableCostsInput(**input_kwargs)

    return _make_input_data


def test_plant_variable_costs_emissivity(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the emissivity material_category.
    This category is special, because it is not handled by the model
    and should trigger the default price of 0.
    """
    value, price, year, country_code = 1.0, 0.0, 2020, "DEU"
    input_data = make_input_data(
        make_business_cases([("Emissivity", value)]), year, country_code
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_electricity(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the electricity material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    power_grid_prices_ref = {(year, country_code): price}
    input_data = make_input_data(
        make_business_cases([("Electricity", value)]),
        year,
        country_code,
        power_grid_prices_ref=power_grid_prices_ref,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_hydrogen(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the hydrogen material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    h2_prices_ref = {(year, country_code): price}
    input_data = make_input_data(
        make_business_cases([("Hydrogen", value)]),
        year,
        country_code,
        h2_prices_ref=h2_prices_ref,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_biomass(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the biomass material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    bio_model_prices_ref = {(year, country_code): price}
    input_data = make_input_data(
        make_business_cases([("Biomass", value)]),
        year,
        country_code,
        bio_model_prices_ref=bio_model_prices_ref,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_captured_co2(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the captured_co2 material_category.
    """
    value, transport_price, storage_price, year, country_code = (
        1.0,
        0.25,
        0.25,
        2020,
        "DEU",
    )
    ccs_model_transport_ref = {country_code: transport_price}
    ccs_model_storage_ref = {country_code: storage_price}
    input_data = make_input_data(
        make_business_cases([("Captured CO2", value)]),
        year,
        country_code,
        ccs_model_transport_ref=ccs_model_transport_ref,
        ccs_model_storage_ref=ccs_model_storage_ref,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * (transport_price + storage_price)


def test_plant_variable_costs_natural_gas(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the natural gas material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    energy_price_rows = [
        ["Natural gas - low", year, price],
        ["Natural gas - high", year, price],
    ]
    static_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    for ng_flag in (0, 1):
        input_data = make_input_data(
            make_business_cases([("Natural gas", value)]),
            year,
            country_code,
            static_energy_prices=static_energy_prices,
            steel_plant_region_ng_dict={country_code: ng_flag},
        )
        df = plant_variable_costs(input_data)
        assert df.cost.values[0] == value * price


def test_plant_variable_costs_plastic_waste(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the plastic_waste material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    input_data = make_input_data(
        make_business_cases([("Plastic waste", value)]),
        year,
        country_code,
        feedstock_dict={"Plastic waste": price},
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_thermal_coal(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the thermal coal material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Thermal coal"
    energy_price_rows = [
        [material_category, year, price],
    ]
    static_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    input_data = make_input_data(
        make_business_cases([("Thermal coal", value)]),
        year,
        country_code,
        static_energy_prices=static_energy_prices,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_iron_ore(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the iron ore material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Iron ore"
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        feedstock_dict={material_category: price},
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_bf_slag(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the bf_slag material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "BF slag"
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        feedstock_dict={material_category: price},
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_steam(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the steam material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Steam"
    energy_price_rows = [
        [material_category, year, price],
    ]
    static_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        static_energy_prices=static_energy_prices,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price
