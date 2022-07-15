from mppsteel.data_preprocessing.investment_cycles import (
    calculate_investment_years,
    net_zero_year_bring_forward
)

def test_calculate_investment_years():
    test_outcome = calculate_investment_years(
        op_start_year=2006,
        cycle_length=23,
        cutoff_end_year=2050,
        cutoff_start_year=2020,
        bring_forward_before_net_zero_year=True
    )
    expected_outcome = [2029, 2049]
    assert test_outcome == expected_outcome

def test_net_zero_year_bring_forward():
    test_outcome = net_zero_year_bring_forward(2053)
    expected_outcome = 2049
    assert test_outcome == expected_outcome