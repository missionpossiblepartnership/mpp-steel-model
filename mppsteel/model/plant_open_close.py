"""Module that determines functionality for opening and closing plants"""

def plant_closure_check(
    utilization_rate: float, cutoff: float, current_tech: str
) -> str:
    """Function that checks whether a plant in a given region should close.

    Args:
        utilization_rate (float): _description_
        cutoff (float): _description_
        current_tech (str): _description_

    Returns:
        str: _description_
    """
    return "Close plant" if utilization_rate < cutoff else current_tech
