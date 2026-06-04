"""Execution-layer rules for major capital signals.

These rules decide how to act after a frozen BUY signal appears. They do not
change the WATCH/BUY shape definition.
"""
from datetime import date

NEXT_OPEN_MAX_GAP_UP_PCT = 0.05
NEXT_OPEN_REQUIRE_ABOVE_SIGNAL_PRICE = True
MAX_SINGLE_LOSS_PCT = 0.10

# 推送去重：已持有(open)的票不再推买入信号；离场后该天数内进入冷却期，
# 不重复推同一只（避免"已离场的票仍天天推送"）。pattern_outcome/scan_results 仍完整记录。
EXIT_COOLDOWN_DAYS = 20

# ── 分批进场：默认次日半仓，站稳突破位/继续放量再补半仓 ──
STAGED_ENTRY_ENABLED = True
FIRST_TRANCHE_PCT = 0.5          # 首批仓位比例（半仓）
ADD_WINDOW_DAYS = 5              # 补仓观察窗口（首批入场后 N 个交易日内）
ADD_VOL_EXPAND_RATIO = 1.0      # 补仓量能确认：当日量 ≥ 首批入场日量 × 该比值（站稳同时放量更佳）


def evaluate_second_half(
    signal_price: float,
    entry_date_close: float,
    bar_close: float,
    bar_volume: float = 0.0,
    entry_volume: float = 0.0,
) -> tuple[bool, str]:
    """判断某个交易日是否满足补半仓条件（站稳突破位 或 继续放量）。

    bar_close   当日收盘价
    bar_volume  当日成交量
    entry_volume 首批入场日成交量（量能对比基准，可为 0 表示不校验）
    返回 (should_add, reason)。
    """
    ref = signal_price if signal_price and signal_price > 0 else entry_date_close
    if ref <= 0 or bar_close <= 0:
        return False, "补仓判断: 价格无效"

    held = bar_close >= ref                                   # 收盘站稳突破位
    expanding = (entry_volume > 0 and bar_volume >= entry_volume * ADD_VOL_EXPAND_RATIO)

    if held and expanding:
        return True, f"补半仓: 收盘{bar_close:.2f}站稳突破位{ref:.2f} 且放量"
    if held:
        return True, f"补半仓: 收盘{bar_close:.2f}站稳突破位{ref:.2f}"
    return False, f"维持半仓: 收盘{bar_close:.2f}跌回突破位{ref:.2f}下方"


# ── 排序买入：同日信号过多时只入排名前 N 个 ──────────────
MAX_NEW_ENTRIES_PER_DAY = 3

# 多因子优先级权重（各因子先归一化到 0~1 再加权求和）
RANK_WEIGHTS = {
    "confidence":        0.30,   # 策略内置置信度
    "rsi":               0.15,   # RSI 越高趋势确认越强（55~75 区间映射）
    "watch_days":        0.15,   # 建仓/观察天数越长越扎实
    "yy_ratio":          0.15,   # 阳阴量比，主力吸筹强度
    "bb_narrow":         0.05,   # 布林收口（突破前蓄势）
    "breakout_strength": 0.10,   # 突破当日涨幅强度
    "amount_wan":        0.10,   # 成交额（流动性，避免买进无法成交的票）
}


def _norm(value, lo: float, hi: float) -> float:
    """线性归一化到 0~1，越界裁剪。value 为 None 时返回 0。"""
    if value is None or hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (float(value) - lo) / (hi - lo)))


def rank_score(stock: dict) -> float:
    """对单只 BUY 候选计算多因子优先级综合分（0~1）。

    不改变入选条件，仅用已有指标决定同日多信号时的买入优先级。
    """
    meta = stock.get("buy_meta") or {}
    w = RANK_WEIGHTS

    confidence = stock.get("confidence", 0) or 0
    rsi        = meta.get("rsi")
    watch_days = meta.get("watch_days") or meta.get("accumulation_days")
    yy_ratio   = meta.get("yy_ratio")
    bb_narrow  = 1.0 if meta.get("bb_narrow") else 0.0
    bstrength  = meta.get("breakout_strength")
    amount_wan = stock.get("amount_wan")

    score = (
        w["confidence"]        * _norm(confidence, 0.3, 1.0)
        + w["rsi"]             * _norm(rsi, 55.0, 75.0)
        + w["watch_days"]      * _norm(watch_days, 15.0, 40.0)
        + w["yy_ratio"]        * _norm(yy_ratio, 1.0, 1.5)
        + w["bb_narrow"]       * bb_narrow
        + w["breakout_strength"] * _norm(bstrength, 3.0, 8.0)
        + w["amount_wan"]      * _norm(amount_wan, 300.0, 5000.0)
    )
    return round(score, 4)


def rank_buy_signals(buy_stocks: list[dict]) -> list[dict]:
    """按多因子综合分降序排列 BUY 候选；写入 rank_score 字段后返回新列表。"""
    ranked = sorted(
        buy_stocks,
        key=lambda s: (s.get("signal_date", ""), rank_score(s)),
        reverse=True,
    )
    for s in ranked:
        s["rank_score"] = rank_score(s)
    return ranked


# ── 灾难止损：大盘急跌 / 趋势走弱时提示减仓清仓 ──────────
MARKET_5D_DROP_PCT = -0.05      # 指数近 5 个交易日跌幅 ≤ -5% → 急跌
MARKET_MA_FAST = 20             # 大盘快线
MARKET_MA_SLOW = 60             # 大盘慢线
MARKET_MAX_STALE_DAYS = 7       # 指数数据最新日期落后 as_of 超过该天数 → 视为过期，跳过判断


def _ma(closes: list, period: int):
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def evaluate_market_regime(idx_bars: list[dict], as_of_date=None) -> tuple[bool, str]:
    """根据大盘指数日线判断是否触发减仓/清仓提示。

    输入 idx_bars 为按日期升序的指数日线（需含 'close'，最好含 'trade_date'）。
    as_of_date：判断基准日（YYYY-MM-DD 或 date），用于校验指数数据是否过期。
    返回 (warn, reason)：warn=True 表示建议减仓/清仓。
    触发条件（任一）：
      1. 近 5 个交易日累计跌幅 ≤ MARKET_5D_DROP_PCT（急跌）
      2. 大盘 MA20 重新跌破 MA60（趋势走弱）
    数据过期保护：若指数最新日期落后 as_of_date 超过 MARKET_MAX_STALE_DAYS 天，
    直接跳过（不基于陈旧数据发误导推送）。
    """
    closes = [float(b["close"]) for b in idx_bars if b.get("close") is not None]
    if len(closes) < 6:
        return False, "大盘数据不足，跳过 regime 判断"

    # 数据新鲜度校验：避免用陈旧指数数据（如未每日同步）得出错误的"趋势走弱"
    if as_of_date and idx_bars:
        latest_raw = idx_bars[-1].get("trade_date")
        if latest_raw:
            try:
                latest_d = date.fromisoformat(str(latest_raw)[:10])
                asof_d = as_of_date if isinstance(as_of_date, date) else date.fromisoformat(str(as_of_date)[:10])
                stale = (asof_d - latest_d).days
                if stale > MARKET_MAX_STALE_DAYS:
                    return False, f"指数数据已过期（最新 {latest_d}，落后 {stale} 天未更新），跳过大盘判断"
            except (ValueError, TypeError):
                pass

    reasons = []

    # 1. 近 5 个交易日跳水
    drop_5d = closes[-1] / closes[-6] - 1.0
    if drop_5d <= MARKET_5D_DROP_PCT:
        reasons.append(f"大盘5日跳水{drop_5d:+.1%}")

    # 2. MA20 跌破 MA60
    ma_fast = _ma(closes, MARKET_MA_FAST)
    ma_slow = _ma(closes, MARKET_MA_SLOW)
    if ma_fast is not None and ma_slow is not None and ma_fast < ma_slow:
        reasons.append(f"大盘MA{MARKET_MA_FAST}({ma_fast:.0f})<MA{MARKET_MA_SLOW}({ma_slow:.0f})趋势走弱")

    if reasons:
        return True, "⚠️ 减仓/清仓提示: " + " + ".join(reasons)
    return False, "大盘 regime 正常"


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
