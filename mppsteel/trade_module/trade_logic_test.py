import math
from mppsteel.trade_module.trade_logic import (
    calculate_plants_to_close,
    ClosePlantsContainer,
)


def test_calculate_plants_to_close():
    initial_capacity = 100
    production = 40
    avg_plant_capacity_value = 2.5
    util_min = 0.6
    calculation = calculate_plants_to_close(
        region="NAFTA",
        initial_capacity=initial_capacity,
        production=production,
        avg_plant_capacity_value=avg_plant_capacity_value,
        util_min=util_min,
    )
    capacity_required = production / util_min
    excess_capacity = capacity_required - initial_capacity
    plants_to_close = math.ceil(-excess_capacity / avg_plant_capacity_value)
    capacity_to_close = plants_to_close * avg_plant_capacity_value
    new_total_capacity = initial_capacity - (plants_to_close * avg_plant_capacity_value)
    new_min_utilization_required = production / new_total_capacity

    expected_result = ClosePlantsContainer(
        plants_to_close,
        new_total_capacity,
        new_min_utilization_required,
        new_total_capacity * new_min_utilization_required,
        -capacity_to_close,
    )
    assert calculation == expected_result
