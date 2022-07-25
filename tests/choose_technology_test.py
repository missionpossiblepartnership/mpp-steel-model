import pytest

from mppsteel.config.model_scenarios import DEFAULT_SCENARIO
from mppsteel.config.model_config import USD_TO_EUR_CONVERSION_DEFAULT

from mppsteel.model_solver.solver_flow import choose_technology_core, ChooseTechnologyInput

@pytest.fixture
def scenario_dict():
    scenario_dict = DEFAULT_SCENARIO.copy()
    scenario_dict["usd_to_eur"] = USD_TO_EUR_CONVERSION_DEFAULT
    scenario_dict["eur_to_usd"] = 1.0 / scenario_dict["usd_to_eur"]
    return scenario_dict


def test_choose_technology_empty():
    """Make sure core function runs with minimal input as defined in the input class."""
    cti = ChooseTechnologyInput()
    result = choose_technology_core(cti)
    assert "tech_choice_dict" in result