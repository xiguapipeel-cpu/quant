"""
shadow_pullback_scan.py — 回踩确认买点「影子信号」跟踪（建议2，只跟踪不交易）
================================================================================
专业建议2：放量突破后出现缩量回踩，只要不跌破核心均线（如 30/60 日线），
则是成本更优的二次上车机会。现有主力建仓策略只有「突破当日」右侧单一买点，
缺这个回踩买点。

本脚本把回踩买点作为**影子信号**，写入 pattern_outcome（独立 strategy 命名空间），
由 pattern_tracker 跟踪其信号日次开盘价后 5/10/30/60 日真实走势，用来**验证回踩
买点的胜率/收益**——但完全不登记持仓、不进标的池、不推送、不影响任何现有交易。

攒够样本（建议 1-2 个月）后，对比回踩买点 vs 突破买点的命中表现，再决定是否纳入。

回踩确认买点定义（day i）：
  1. 前 PB_WINDOW 天内存在「放量突破日」t：close[t] > 前 30 日最高 且 当日涨幅 ≥ 4%
  2. i 日已从突破日回落：close[i] < close[t]
  3. 守住生命线：close[i] ≥ MA(CORE_MA)[i]（不跌破核心均线）
  4. 回踩到均线附近：low[i] ≤ MA(CORE_MA)[i] × NEAR_MA
  5. 缩量回踩：volume[i] < volume[t] × SHRINK_RATIO

用法：
  python -m scripts.shadow_pullback_scan                 # 回填近 180 天 + 跟踪
  python -m scripts.shadow_pullback_scan --days 90
  python -m scripts.shadow_pullback_scan --no-update     # 只写事件不跑 tracker
  python -m scripts.shadow_pullback_scan --report        # 查看影子信号效力统计
"""
import argparse
import asyncio
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.stock_dao import get_daily_history
from db.pattern_dao import upsert_event, aggregate_stats
from db.mysql_pool import close_pool
from utils.logger import setup_logger

logger = setup_logger("shadow_pullback")

SHADOW_STRATEGY = "major_capital_pullback_shadow"

# ── 回踩买点参数（仅影子信号用；不进主策略形态参数）──
PB_WINDOW = 10            # 突破后多少个交易日内出现回踩低点
CORE_MA = 30              # 核心均线（生命线）
BREAKOUT_PCT = 0.04       # 突破日涨幅下限
BREAK_HIGH_LOOKBACK = 30  # 突破需 close > 前 N 日最高
SHRINK_RATIO = 0.8        # 回踩缩量：回踩日量 < 突破日量 × 该值
NEAR_MA = 1.03            # 回踩到均线附近：low ≤ MA × 该值
# ── 二次确认（回踩企稳后重新转强才算上车点）──
CONFIRM_WINDOW = 5        # 回踩低点后多少个交易日内出现二次确认
CONFIRM_PCT = 0.02        # 二次确认日涨幅下限（阳线转强）
CONFIRM_VOL_EXPAND = 1.0  # 二次确认放量：确认日量 > 回踩低点日量 × 该值


def _sma(arr, n, i):
    if i < n - 1:
        return None
    seg = arr[i - n + 1: i + 1]
    if any(v is None for v in seg):
        return None
    return sum(seg) / n


def detect_pullbacks(bars: list[dict], only_since: str | None = None) -> list[dict]:
    """带二次确认的回踩买点：突破 → 缩量回踩不破核心均线 → 重新放量阳线转强。
    信号落在「二次确认日」（企稳上车点），而非回踩半山腰。
    返回 [{date, breakout_date, pullback_date, reason, price}]。"""
    if len(bars) < CORE_MA + 5:
        return []
    closes = [float(b["close"]) for b in bars]
    opens = [float(b["open_price"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    vols = [float(b.get("volume") or 0) for b in bars]
    dates = [str(b["trade_date"]) for b in bars]
    n = len(bars)
    ma = [_sma(closes, CORE_MA, i) for i in range(n)]

    out = []
    seen_dates: set = set()
    # 遍历突破日 t
    for t in range(BREAK_HIGH_LOOKBACK + 1, n):
        prev_high = max(highs[t - BREAK_HIGH_LOOKBACK: t])
        chg_t = closes[t] / closes[t - 1] - 1.0 if closes[t - 1] else 0
        if not (closes[t] > prev_high and chg_t >= BREAKOUT_PCT) or vols[t] <= 0:
            continue
        # 在 t 之后 PB_WINDOW 内找「缩量回踩低点 p」（触均线不破 + 缩量）
        for p in range(t + 1, min(t + PB_WINDOW, n - 1) + 1):
            if ma[p] is None or ma[p] <= 0:
                continue
            is_pullback = (lows[p] <= ma[p] * NEAR_MA and closes[p] >= ma[p]
                           and closes[p] < closes[t] and vols[p] < vols[t] * SHRINK_RATIO)
            if not is_pullback:
                continue
            # 在 p 之后 CONFIRM_WINDOW 内找「二次确认日 i」（放量阳线转强、守线、重新向上）
            for i in range(p + 1, min(p + CONFIRM_WINDOW, n - 1) + 1):
                if ma[i] is None:
                    continue
                is_confirm = (closes[i] > opens[i]
                              and (closes[i] / closes[i - 1] - 1.0 if closes[i - 1] else 0) >= CONFIRM_PCT
                              and vols[i] > vols[p] * CONFIRM_VOL_EXPAND
                              and closes[i] >= ma[i]
                              and closes[i] > closes[p])
                if is_confirm:
                    d = dates[i]
                    if d not in seen_dates and not (only_since and d < only_since):
                        seen_dates.add(d)
                        out.append({
                            "date": d,
                            "breakout_date": dates[t],
                            "pullback_date": dates[p],
                            "price": closes[i],
                            "reason": (f"回踩二次确认: 突破{dates[t]}(+{chg_t*100:.1f}%)→缩量回踩{dates[p]}"
                                       f"(守MA{CORE_MA}{ma[p]:.2f})→{d}放量阳线转强 收{closes[i]:.2f}"),
                        })
                    break  # 该回踩段只记首个确认日
            break  # 该突破日只用首个回踩低点
    return out


async def scan(backfill_days: int, do_update: bool):
    from backtest.screener import DynamicScreener, SCREEN_PRESETS
    params = SCREEN_PRESETS["major_capital_accumulation"]["params"]
    candidates = await DynamicScreener(**params).screen(use_cache_hours=2)
    logger.info(f"[影子-回踩] 候选股 {len(candidates)} 只，回填近 {backfill_days} 天")

    since = (date.today() - timedelta(days=backfill_days)).isoformat()
    data_start = (date.today() - timedelta(days=backfill_days + 200)).isoformat()
    end = date.today().isoformat()

    n_events = n_stocks = 0
    for stk in candidates:
        code = stk.get("code")
        if not code:
            continue
        bars = await get_daily_history(code, data_start, end)
        if not bars or len(bars) < CORE_MA + 5:
            continue
        sigs = detect_pullbacks(bars, only_since=since)
        if not sigs:
            continue
        n_stocks += 1
        for sg in sigs:
            await upsert_event(
                strategy=SHADOW_STRATEGY,
                code=code,
                name=stk.get("name") or code,
                signal_date=sg["date"],
                signal_type="PULLBACK",
                signal_reason=sg["reason"],
                confidence=0.0,
                strategy_version="pullback_shadow_v1",
                scan_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                signal_meta={
                    "source": "shadow_pullback_scan",
                    "breakout_date": sg["breakout_date"],
                    "signal_price": sg["price"],
                    "core_ma": CORE_MA,
                },
            )
            n_events += 1
    logger.info(f"[影子-回踩] 写入 {n_events} 个回踩事件（{n_stocks} 只股票）")

    if do_update and n_events:
        logger.info("[影子-回踩] 跟踪后续走势（pattern_tracker）...")
        from scripts.pattern_tracker import refresh_all_pending
        done, _ = await refresh_all_pending(SHADOW_STRATEGY)
        logger.info(f"[影子-回踩] 跟踪刷新 {done} 条")


async def report():
    st = await aggregate_stats(SHADOW_STRATEGY)
    print("\n=== 回踩影子信号效力（major_capital_pullback_shadow）===")
    if not st or not st.get("total"):
        print("暂无已跟踪样本。先运行 scan + pattern_tracker。")
        return
    def pct(v): return "—" if v is None else f"{float(v)*100:+.2f}%"
    print(f"样本数: {int(st['total'])}")
    print(f"5 日胜率 {pct(st.get('win_5d'))}  均收 {pct(st.get('avg_5d'))}")
    print(f"10日胜率 {pct(st.get('win_10d'))}  均收 {pct(st.get('avg_10d'))}")
    print(f"30日胜率 {pct(st.get('win_30d'))}  均收 {pct(st.get('avg_30d'))}")
    print(f"60日胜率 {pct(st.get('win_60d'))}  均收 {pct(st.get('avg_60d'))}")
    print(f"平均峰值 {pct(st.get('avg_peak'))}  平均谷值 {pct(st.get('avg_trough'))}")
    print("\n对照：可在「历史命中表现」用主策略数据对比突破买点胜率。")


async def main(args):
    if args.report:
        await report()
    else:
        if args.reset:
            from scripts.pattern_tracker import reset_all
            n = await reset_all(SHADOW_STRATEGY)
            logger.info(f"[影子-回踩] 已清空旧影子事件 {n} 条")
        await scan(args.days, not args.no_update)
        await report()
    await close_pool()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="回踩确认买点影子信号（只跟踪不交易）")
    ap.add_argument("--days", type=int, default=180, help="回填历史天数（默认180）")
    ap.add_argument("--no-update", action="store_true", help="只写事件，不跑 tracker 跟踪")
    ap.add_argument("--report", action="store_true", help="只看影子信号效力统计")
    ap.add_argument("--reset", action="store_true", help="重跑前清空旧影子事件（定义变更后用）")
    asyncio.run(main(ap.parse_args()))
