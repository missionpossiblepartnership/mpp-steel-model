"""Script to transform units"""

def mwh_gj(val: float, base: str):
    factor = 3.6
    if base == 'smaller':
        return val / factor
    if base == 'larger':
        return val * factor
