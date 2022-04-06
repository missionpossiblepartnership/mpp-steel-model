"""Scenario references for model runs"""

from mppsteel.config.model_config import MODEL_YEAR_END, MODEL_YEAR_START


COST_SCENARIO_MAPPER = {
    "low": "Min",
    "average": "Baseline",
    "high": "Max",
}

STEEL_DEMAND_SCENARIO_MAPPER = {"bau": "BAU", "high": "High Circ", "average": "Average"}

TECH_SWITCH_SCENARIOS = {
    "max_abatement": {"tco": 0, "emissions": 1},
    "lowest_cost": {"tco": 1, "emissions": 0},
    "equal_weight": {"tco": 0.5, "emissions": 0.5},
}

GREEN_PREMIUM_SCENARIOS = {
    "off": (0, 0),
    "low": (0.01, 0.03),
    "average": (0.025, 0.05),
    "high": (0.05, 0.08),
}

CARBON_TAX_SCENARIOS = {
    "off": (0, 0),
    "low": (0, 30),
    "average": (0, 100),
    "high": (0, 210),
}

GRID_DECARBONISATION_SCENARIOS = {
    "low": "Accelerated ",
    "high": "Central",
}

BIOMASS_SCENARIOS = {
    "average": "Medium",
}

CCUS_SCENARIOS = {"high": "high", "low": "low"}

SOLVER_LOGICS = {"rank": "ranked", "scale": "scaled", 'bins': 'scaled_bins'}

SCENARIO_SETTINGS = {
    "tech_moratorium": [True, False],
    "enforce_constraints": [True, False],
    "transitional_switch": [True, False],
    "carbon_tax": CARBON_TAX_SCENARIOS.keys(),
    "green_premium_scenario": GREEN_PREMIUM_SCENARIOS.keys(),
    "electricity_cost_scenario": COST_SCENARIO_MAPPER.keys(),
    "grid_scenario": GRID_DECARBONISATION_SCENARIOS.keys(),
    "hydrogen_cost_scenario": COST_SCENARIO_MAPPER.keys(),
    "biomass_cost_scenario": BIOMASS_SCENARIOS.keys(),
    "ccus_cost_scenario": CCUS_SCENARIOS.keys(),
    "steel_demand_scenario": STEEL_DEMAND_SCENARIO_MAPPER.keys(),
    "tech_switch_scenario": TECH_SWITCH_SCENARIOS.keys(),
    "solver_logic": SOLVER_LOGICS.keys(),
    "trade_active": [True, False],
}

DEFAULT_SCENARIO = {
    "scenario_name": "default",
    "tech_moratorium": True,  # bool
    "enforce_constraints": True,  # bool
    "transitional_switch": True,  #  bool
    "carbon_tax_scenario": "off",  # off / low / average / high
    "green_premium_scenario": "off",  # off / low / average / high
    "electricity_cost_scenario": "average",  # low / average / high
    "grid_scenario": "low",  # low / high
    "hydrogen_cost_scenario": "average",  # low / average / high
    "biomass_cost_scenario": "average",  # average
    "ccus_cost_scenario": "low",  # low / average
    "steel_demand_scenario": "average",  # bau / average / high
    "tech_switch_scenario": "lowest_cost",  # max_abatement / lowest_cost / equal_weight
    "solver_logic": "scale",  # scale / rank / bins
    "trade_active": True, # bool
}
TECH_MORATORIUM = {
    "scenario_name": "tech_moratorium",
    'tech_moratorium': True,
    'enforce_constraints': True,
    'transitional_switch': True,
    'carbon_tax_scenario': 'off',
    'green_premium_scenario': 'off',
    'electricity_cost_scenario': 'low',
    'grid_scenario': 'low',
    'hydrogen_cost_scenario': 'average',
    'biomass_cost_scenario': 'average',
    'ccus_cost_scenario': 'low',
    'steel_demand_scenario': 'bau',
    'tech_switch_scenario': 'lowest_cost',
    'solver_logic': 'rank',
    'trade_active': True,
}
CARBON_COST = {
    "scenario_name": "carbon_cost",
    'tech_moratorium': False,
    'enforce_constraints': True,
    'transitional_switch': True,
    'carbon_tax_scenario': 'average',
    'green_premium_scenario': 'off',
    'electricity_cost_scenario': 'average',
    'grid_scenario': 'low',
    'hydrogen_cost_scenario': 'average',
    'biomass_cost_scenario': 'average',
    'ccus_cost_scenario': 'low',
    'steel_demand_scenario': 'bau',
    'tech_switch_scenario': 'lowest_cost',
    'solver_logic': 'rank',
    'trade_active': True,
}
BAU_SCENARIO = {
    "scenario_name": "baseline",
    'tech_moratorium': False,
    'enforce_constraints': True,
    'transitional_switch': True,
    'carbon_tax_scenario': 'off',
    'green_premium_scenario': 'off',
    'electricity_cost_scenario': 'average',
    'grid_scenario': 'low',
    'hydrogen_cost_scenario': 'average',
    'biomass_cost_scenario': 'average',
    'ccus_cost_scenario': 'low',
    'steel_demand_scenario': 'bau',
    'tech_switch_scenario': 'lowest_cost',
    'solver_logic': 'rank',
    'trade_active': True,
}
BAU_HIGH_CIRC_SCENARIO = {
    "scenario_name": "baseline_high_circ",
    'tech_moratorium': False,
    'enforce_constraints': True,
    'transitional_switch': True,
    'carbon_tax_scenario': 'off',
    'green_premium_scenario': 'off',
    'electricity_cost_scenario': 'average',
    'grid_scenario': 'low',
    'hydrogen_cost_scenario': 'average',
    'biomass_cost_scenario': 'average',
    'ccus_cost_scenario': 'low',
    'steel_demand_scenario': 'high',
    'tech_switch_scenario': 'lowest_cost',
    'solver_logic': 'rank',
    'trade_active': True,
}
ABATEMENT_SCENARIO = {
    "scenario_name": "abatement",
    'tech_moratorium': False,
    'enforce_constraints': True,
    'transitional_switch': True,
    'carbon_tax_scenario': 'off',
    'green_premium_scenario': 'off',
    'electricity_cost_scenario': 'average',
    'grid_scenario': 'low',
    'hydrogen_cost_scenario': 'average',
    'biomass_cost_scenario': 'average',
    'ccus_cost_scenario': 'low',
    'steel_demand_scenario': 'bau',
    'tech_switch_scenario': 'max_abatement',
    'solver_logic': 'rank',
    'trade_active': True,
}
ABATEMENT_HIGH_CIRC_SCENARIO = {
    "scenario_name": "fastest_abatement",
    'tech_moratorium': False,
    'enforce_constraints': True,
    'transitional_switch': True,
    'carbon_tax_scenario': 'off',
    'green_premium_scenario': 'off',
    'electricity_cost_scenario': 'average',
    'grid_scenario': 'low',
    'hydrogen_cost_scenario': 'average',
    'biomass_cost_scenario': 'average',
    'ccus_cost_scenario': 'low',
    'steel_demand_scenario': 'high',
    'tech_switch_scenario': 'max_abatement',
    'solver_logic': 'rank',
    'trade_active': True,
}

SCENARIO_OPTIONS = {
    "default": DEFAULT_SCENARIO,
    'tech_moratorium': TECH_MORATORIUM,
    'carbon_cost': CARBON_COST,
    'baseline': BAU_SCENARIO,
    'baseline_high_circ': BAU_HIGH_CIRC_SCENARIO,
    'abatement': ABATEMENT_SCENARIO,
    'fastest_abatement': ABATEMENT_HIGH_CIRC_SCENARIO
}
