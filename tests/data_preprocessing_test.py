import pandas as pd
import pytest

from mppsteel.config.reference_lists import RESOURCE_CATEGORY_MAPPER

from mppsteel.data_preprocessing.variable_plant_cost_archetypes import (
    PlantVariableCostsInput,
    plant_variable_costs,
)


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


@pytest.fixture
def feedstock_dict():
    return get_feedstock_dict()


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
    static_energy_prices = pd.DataFrame(
        energy_price_rows, columns=["Metric", "Year", "Value"]
    ).set_index(["Metric", "Year"])
    return static_energy_prices


@pytest.fixture
def static_energy_prices():
    return get_static_energy_prices()


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


class Param:
    def __init__(
        self,
        *,
        material_category: str = "Electricity",
        value: float = 1.0,
        price: float = 0.5,
        year: int = 2020,
        country_code: str = "DEU",
        input_kwargs: dict[str, str] = {},
    ):
        self.material_category = material_category
        self.value = value
        self.price = price
        self.year = year
        self.country_code = country_code
        self._input_kwargs = input_kwargs

    @property
    def year_country_to_price(self) -> dict[tuple[int, str], float]:
        return {(self.year, self.country_code): self.price}

    @property
    def year_country_material_category_to_price(
        self,
    ) -> dict[tuple[int, str, str], float]:
        return {(self.year, self.country_code, self.material_category): self.price}

    @property
    def feedstock_dict(self) -> dict[str, float]:
        return get_feedstock_dict() | {self.material_category: self.price}

    @property
    def static_energy_prices(self) -> pd.DataFrame:
        energy_price_rows = [
            [self.material_category, self.year, self.price],
        ]
        test_energy_prices = pd.DataFrame(
            energy_price_rows, columns=["Metric", "Year", "Value"]
        ).set_index(["Metric", "Year"])
        return pd.concat([test_energy_prices, get_static_energy_prices()])

    @property
    def input_kwargs(self):
        """
        Create input_kwargs for the make_input_data fixture.
        The keys in self._input_kwargs stay the same, but the values
        are mapped on properties of the Param object.
        """
        return {k: getattr(self, v) for k, v in self._input_kwargs.items()}


@pytest.mark.parametrize(
    "param",
    [
        Param(
            material_category="Emissivity",
            price=0.0,
            input_kwargs={"power_grid_prices_ref": "year_country_to_price"},
        ),
        Param(
            material_category="Electricity",
            input_kwargs={"power_grid_prices_ref": "year_country_to_price"},
        ),
        Param(
            material_category="Hydrogen",
            input_kwargs={"h2_prices_ref": "year_country_to_price"},
        ),
        Param(
            material_category="Biomass",
            input_kwargs={"bio_model_prices_ref": "year_country_to_price"},
        ),
        Param(
            material_category="Plastic waste",
            input_kwargs={"feedstock_dict": "feedstock_dict"},
        ),
        Param(
            material_category="Iron ore",
            input_kwargs={"feedstock_dict": "feedstock_dict"},
        ),
        Param(
            material_category="BF slag",
            input_kwargs={"feedstock_dict": "feedstock_dict"},
        ),
        Param(
            material_category="Natural gas",
            input_kwargs={"fossil_fuel_ref": "year_country_material_category_to_price"},
        ),
        Param(
            material_category="Thermal coal",
            input_kwargs={"fossil_fuel_ref": "year_country_material_category_to_price"},
        ),
        Param(
            material_category="Met coal",
            input_kwargs={"fossil_fuel_ref": "year_country_material_category_to_price"},
        ),
        Param(
            material_category="BF gas",
            input_kwargs={"static_energy_prices": "static_energy_prices"},
        ),
        Param(
            material_category="Steam",
            input_kwargs={"static_energy_prices": "static_energy_prices"},
        ),
    ],
)
def test_plant_variable_costs_material_category(
    param,
    make_business_cases,
    make_input_data,
):
    """
    Assert that cost is calculated correctly for different material categories.
    """
    input_data = make_input_data(
        make_business_cases([(param.material_category, param.value)]),
        param.year,
        param.country_code,
        **param.input_kwargs,
    )
    df = plant_variable_costs(input_data)
    assert df.cost.values[0] == param.value * param.price


def test_plant_variable_costs_captured_co2(make_business_cases, make_input_data):
    """
    Assert that cost is calculated correctly for the captured_co2 material_category.
    """
    transport_price, storage_price = 0.25, 0.25
    value, year, country_code = (1.0, 2020, "DEU")
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
