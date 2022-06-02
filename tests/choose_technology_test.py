import pytest

from mppsteel.config.model_scenarios import DEFAULT_SCENARIO
from mppsteel.config.model_config import USD_TO_EUR_CONVERSION_DEFAULT, PROJECT_PATH

from mppsteel.model_solver.solver import choose_technology

@pytest.fixture
def scenario_dict():
    scenario_dict = DEFAULT_SCENARIO.copy()
    scenario_dict["usd_to_eur"] = USD_TO_EUR_CONVERSION_DEFAULT
    scenario_dict["eur_to_usd"] = 1.0 / scenario_dict["usd_to_eur"]
    return scenario_dict


def test_choose_technology(scenario_dict):
    foo = choose_technology(scenario_dict)
    print(foo)
    assert True
