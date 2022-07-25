from mppsteel.trade_module.trade_logic import calculate_plants_to_close, ClosePlantsContainer

def test_calculate_plants_to_close():
    calculation = calculate_plants_to_close(
        region="NAFTA",
        initial_capacity=100, 
        production=40,
        avg_plant_capacity_value=2.5,
        util_min=0.6
    )
    expected_result = ClosePlantsContainer(
        8,
        60,
        40/0.6,
        40,
        100-(40/0.6)
    )
    assert calculation == expected_result
