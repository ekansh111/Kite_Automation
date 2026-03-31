"""
Dynamic daily volatility target computation from account capital and allocation weights.

Implements the spreadsheet formula chain:
  daily_vol = total_capital × vol_pct × Π(weights) / 16

Where 16 = √256 is the annualization factor (trading days).
"""

ANNUALIZATION_FACTOR = 16

WEIGHT_KEYS = [
    "sector_weight",
    "sector_DM",
    "sub_sector_weight",
    "sub_sector_DM",
    "sub_class_weight",
    "asset_weight",
    "asset_DM",
    "instrument_DM",
    "neg_skew_discount",
]


def compute_daily_vol_target(total_capital, vol_target_pct, vol_weights):
    """
    Compute daily volatility target in INR from capital and weight chain.

    Parameters
    ----------
    total_capital : float
        Total account capital (e.g. 9999999).
    vol_target_pct : float
        Annual volatility target as a decimal (e.g. 0.50 for 50%).
    vol_weights : dict
        Allocation weight chain. Keys not present default to 1.0.

    Returns
    -------
    float
        Daily volatility target in INR.
    """
    base = total_capital * vol_target_pct
    product = 1.0
    for key in WEIGHT_KEYS:
        product *= vol_weights.get(key, 1.0)
    return base * product / ANNUALIZATION_FACTOR
