"""Execution-layer rules for major capital signals.

These rules decide how to act after a frozen BUY signal appears. They do not
change the WATCH/BUY shape definition.
"""

NEXT_OPEN_MAX_GAP_UP_PCT = 0.05
NEXT_OPEN_REQUIRE_ABOVE_SIGNAL_PRICE = True
MAX_SINGLE_LOSS_PCT = 0.10


def evaluate_next_open(signal_price: float, next_open: float) -> tuple[bool, str, float]:
    """Return (allowed, reason, gap_pct) for next-open execution."""
    if signal_price <= 0 or next_open <= 0:
        return False, "执行过滤: 信号价或次开价无效", 0.0

    gap_pct = next_open / signal_price - 1.0
    if gap_pct > NEXT_OPEN_MAX_GAP_UP_PCT:
        return False, f"执行过滤: 次日高开{gap_pct:+.1%} > {NEXT_OPEN_MAX_GAP_UP_PCT:.0%}，不追", gap_pct

    if NEXT_OPEN_REQUIRE_ABOVE_SIGNAL_PRICE and next_open < signal_price:
        return False, f"执行过滤: 次开{next_open:.2f}跌回信号价{signal_price:.2f}下方", gap_pct

    return True, f"执行通过: 次开{next_open:.2f} gap={gap_pct:+.1%}", gap_pct
