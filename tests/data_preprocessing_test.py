import pandas as pd
import pytest

from mppsteel.config.reference_lists import RESOURCE_CATEGORY_MAPPER

from mppsteel.data_preprocessing.variable_plant_cost_archetypes import (
    PlantVariableCostsInput,
    plant_variable_costs,
)


@pytest.fixture
def feedstock_dict():
    return {
        "Plastic waste": 6.527621014136413,
        "Iron ore": 97.73,
        "Scrap": 224.46000000000004,
        "DRI": 300.67,
        "Coal": 121.86,
        "BF slag": -27.5,
        "Other slag": 0.0,
    }


@pytest.fixture
def static_energy_prices():
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
    static_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    return static_energy_prices


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
def make_input_data(feedstock_dict, static_energy_prices):
    """
    Create the input data for the plant_variable_costs function.
    """

    def _make_input_data(business_cases, year, country_code, **kwargs):
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
    material_category = "Natural gas"
    fossil_fuel_ref = {(year, country_code, material_category): price}
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        fossil_fuel_ref=fossil_fuel_ref,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_plastic_waste(
    make_business_cases, make_input_data, feedstock_dict
):
    """
    Assert that cost is calculated correctly for the plastic_waste material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    input_data = make_input_data(
        make_business_cases([("Plastic waste", value)]),
        year,
        country_code,
        feedstock_dict=feedstock_dict | {"Plastic waste": price},
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_thermal_coal(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the thermal coal material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Thermal coal"
    fossil_fuel_ref = {(year, country_code, material_category): price}
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        fossil_fuel_ref=fossil_fuel_ref,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_met_coal(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the met coal material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Met coal"
    fossil_fuel_ref = {(year, country_code, material_category): price}
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        fossil_fuel_ref=fossil_fuel_ref,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_bf_gas(
    make_business_cases, make_input_data, static_energy_prices
):
    """
    Assert that cost is calculated correctly for the BF gas material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "BF gas"
    energy_price_rows = [
        [material_category, year, price],
    ]
    test_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    static_energy_prices = pd.concat([test_energy_prices, static_energy_prices])
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        static_energy_prices=static_energy_prices,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_iron_ore(
    make_business_cases, make_input_data, feedstock_dict
):
    """
    Assert that cost is calculated correctly for the iron ore material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Iron ore"
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        feedstock_dict=feedstock_dict | {material_category: price},
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_bf_slag(
    make_business_cases, make_input_data, feedstock_dict
):
    """
    Assert that cost is calculated correctly for the bf_slag material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "BF slag"
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        feedstock_dict=feedstock_dict | {material_category: price},
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price


def test_plant_variable_costs_steam(
    make_business_cases, make_input_data, static_energy_prices
):
    """
    Assert that cost is calculated correctly for the steam material_category.
    """
    value, price, year, country_code = 1.0, 0.5, 2020, "DEU"
    material_category = "Steam"
    energy_price_rows = [
        [material_category, year, price],
    ]
    test_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    static_energy_prices = pd.concat([test_energy_prices, static_energy_prices])
    input_data = make_input_data(
        make_business_cases([(material_category, value)]),
        year,
        country_code,
        static_energy_prices=static_energy_prices,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == value * price
