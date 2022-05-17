"""Script to transform units"""

UNIT_FACTORS = {
    "mwh_gj": 3.6,
}

REVERSE_FACTORS = {
    f"{key.split('_')[1]}_{key.split('_')[0]}": value
    for key, value in UNIT_FACTORS.items()
}

UNIT_FACTORS_COMBINED = {**UNIT_FACTORS, **REVERSE_FACTORS}


def transform_units(val: float, unit_ref: str, base: str) -> float:
    """Tranforms a value from one unit to another.

    Args:
        val (float): The value that you want to transform.
        unit_ref (str): The reference to the transformation you want to create.
        base (str): The initial magnitude of the value w.r.t. the desired value -> `smaller` or `larger`.

    Returns:
        float: The transformed unit.
    """
    factor = UNIT_FACTORS_COMBINED[unit_ref]
    if base == "smaller":
        return val / factor
    if base == "larger":
        return val * factor
