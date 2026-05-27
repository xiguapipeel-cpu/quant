"""Frozen strategy versions used for out-of-sample tracking."""

import json


MAJOR_CAPITAL_FROZEN_VERSION = "major_capital_accumulation_v202604_frozen"


MAJOR_CAPITAL_FROZEN_PARAMS = {
    "enable_signal_f": False,
    "atr_trail_k": 2.0,
    "trail_stage2_gain": 0.05,
    "trail_stage2_k": 1.5,
    "trail_stage3_gain": 0.15,
    "trail_stage3_k": 1.0,
    "signal_a_min_rsi": 55.0,
    "signal_a_break_high_lookback": 30,
    "breakout_atr_k": 0.0,
    "vol_raw_percentile": 0.0,
}


def major_capital_param_snapshot() -> str:
    """Stable JSON snapshot for each recorded signal event."""
    return json.dumps(MAJOR_CAPITAL_FROZEN_PARAMS, ensure_ascii=False, sort_keys=True)
