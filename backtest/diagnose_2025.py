"""
深度诊断：为什么本地DB回测在2025年没有交易
完整模拟 MajorCapitalBT._check_buy 逻辑，逐日打印状态
"""
import asyncio
import sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backtest.bt_major_capital import load_all_db_data, get_all_db_stocks

# ─── 参数（与策略默认值完全一致）────────────────────────────────
MAX_ABOVE_LOW_PCT   = 20.0
MA_CONVERGE_PCT     = 5.0
MA_SLOPE_MAX        = 0.002
RSI_WATCH_MIN       = 25
RSI_WATCH_MAX       = 62
VOL_YANG_YIN_MIN    = 1.03
MIN_WATCH_DAYS      = 20
LOW_LOOKBACK        = 60
VOL_LOOKBACK        = 30
BREAKOUT_PCT        = 3.0
BREAKOUT_VOL_RATIO  = 1.5
BB_PERIOD           = 20
BB_MULT             = 2.0
BB_NARROW_RATIO     = 0.75
MA_SLOPE_UP_MIN     = 0.0003
VOL_SHRINK_DAYS     = 15
VOL_EXPAND_DAYS     = 5
VOL_SHRINK_MAX      = 0.80
VOL_EXPAND_MIN      = 1.2
VOL_EXPAND_MAX      = 2.5
MA_DIVERGE_LOOKBACK = 5
MACD_FAST = 12; MACD_SLOW = 26; MACD_SIG = 9
RSI_BUY_MAX         = 70
BREAKOUT_MAX_PCT    = 8.0
TREND_FILTER        = True

# ─── 测试的股票和时间范围 ────────────────────────────────────────
WARMUP_START = "2024-01-01"
START        = "2025-01-01"
END          = "2026-04-10"
TEST_CODES   = ["000001", "000002", "600036", "601318", "000858"]


def wilder_rsi(closes, period=14):
    """与策略完全一致的 RSI 实现"""
    if len(closes) < period + 1:
        return [float('nan')] * len(closes)
    rsi_arr = [float('nan')] * period
    gains, losses = 0.0, 0.0
    for i in range(period):
        diff = closes[i+1] - closes[i]
        if diff > 0: gains += diff
        else: losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    rs = avg_gain / avg_loss if avg_loss > 1e-9 else 1e9
    rsi_arr.append(round(100 - 100/(1+rs), 2))
    for i in range(period+1, len(closes)):
        diff = closes[i] - closes[i-1]
        g = max(diff, 0); l = max(-diff, 0)
        avg_gain = (avg_gain * (period-1) + g) / period
        avg_loss = (avg_loss * (period-1) + l) / period
        rs = avg_gain / avg_loss if avg_loss > 1e-9 else 1e9
        rsi_arr.append(round(100 - 100/(1+rs), 2))
    return rsi_arr


def sma(arr, period):
    result = [float('nan')] * len(arr)
    for i in range(period-1, len(arr)):
        result[i] = sum(arr[i-period+1:i+1]) / period
    return result


def ema(arr, period):
    result = [float('nan')] * len(arr)
    k = 2/(period+1)
    started = False
    for i, v in enumerate(arr):
        if not started:
            result[i] = v
            started = True
        else:
            prev = result[i-1]
            if prev != prev:  # NaN
                result[i] = v
            else:
                result[i] = v * k + prev * (1-k)
    return result


def bollinger(closes, period=20, mult=2.0):
    tops, bots, mids = [], [], []
    for i in range(len(closes)):
        if i < period - 1:
            tops.append(float('nan')); bots.append(float('nan')); mids.append(float('nan'))
            continue
        window = closes[i-period+1:i+1]
        mid = sum(window)/period
        std = (sum((x-mid)**2 for x in window)/period)**0.5
        tops.append(mid + mult*std)
        bots.append(mid - mult*std)
        mids.append(mid)
    return tops, bots, mids


def near_low(closes, highs, lows, i, lookback=60):
    start = max(0, i - lookback + 1)
    window_lows = lows[start:i+1]
    if not window_lows: return None
    lo = min(window_lows)
    if lo <= 0: return None
    return (closes[i] - lo) / lo * 100


def ma_convergence(ma5, ma10, ma20, i):
    v5, v10, v20 = ma5[i], ma10[i], ma20[i]
    if any(v != v for v in [v5, v10, v20]): return None
    mid = (v5 + v10 + v20) / 3
    if mid <= 0: return None
    return max(abs(v5-mid), abs(v10-mid), abs(v20-mid)) / mid * 100


def ma_slope(ma_arr, i, period=5):
    if i < period: return None
    base = ma_arr[i-period]
    curr = ma_arr[i]
    if base != base or curr != curr or base <= 0: return None
    return (curr - base) / base / period


def yang_yin_ratio(closes, opens, volumes, i, lookback=30):
    start = max(0, i - lookback + 1)
    yang_vols = [volumes[j] for j in range(start, i+1) if closes[j] >= opens[j]]
    yin_vols  = [volumes[j] for j in range(start, i+1) if closes[j] < opens[j]]
    if not yang_vols or not yin_vols: return None
    return (sum(yang_vols)/len(yang_vols)) / (sum(yin_vols)/len(yin_vols))


def vol_ratio(volumes, i, period=5):
    if i < period: return None
    avg = sum(volumes[i-period:i]) / period
    return volumes[i] / avg if avg > 1e-9 else None


def check_ma_diverge(ma5, ma10, ma20, i, lookback=5):
    if i < lookback: return False, ""
    v5, v10, v20 = ma5[i], ma10[i], ma20[i]
    if any(v != v for v in [v5, v10, v20]): return False, ""
    if not (v5 > v10 > v20): return False, ""
    p5 = ma5[i-lookback]; p10 = ma10[i-lookback]; p20 = ma20[i-lookback]
    if any(v != v for v in [p5, p10, p20]): return False, ""
    if p20 <= 0 or p10 <= 0 or p5 <= 0: return False, ""
    s5 = (v5 - p5) / p5 / lookback
    s10 = (v10 - p10) / p10 / lookback
    s20 = (v20 - p20) / p20 / lookback
    if s5 > s10 > s20 and s5 > 0:
        return True, f"三线多头发散加速 s5={s5:.5f}>s10={s10:.5f}>s20={s20:.5f}"
    return False, ""


def check_vol_shrink_expand(closes, opens, volumes, i,
                             shrink_days=15, expand_days=5,
                             shrink_max=0.80, expand_min=1.2, expand_max=2.5):
    need = shrink_days + expand_days
    if i < need: return False, ""
    base_start = i - need
    base_end   = i - expand_days
    base_vols = volumes[base_start:base_end - shrink_days]
    if not base_vols: return False, ""
    avg_base = sum(base_vols)/len(base_vols)
    if avg_base <= 0: return False, ""
    shrink_vols = volumes[base_end - shrink_days:base_end]
    expand_vols = volumes[base_end:i+1]
    if not shrink_vols or not expand_vols: return False, ""
    avg_shrink = sum(shrink_vols)/len(shrink_vols)
    avg_expand = sum(expand_vols)/len(expand_vols)
    if avg_shrink <= 0: return False, ""
    sr = avg_shrink / avg_base
    er = avg_expand / avg_shrink
    if sr <= shrink_max and expand_min <= er <= expand_max:
        return True, f"量先萎缩后温和放大 sr={sr:.2f} er={er:.2f}"
    return False, ""


def diagnose_stock(code, df, start_date):
    dates   = list(df.index)
    closes  = list(df['close'])
    opens   = list(df['open'])
    highs   = list(df['high'])
    lows    = list(df['low'])
    volumes = list(df['volume'])

    n = len(dates)
    ma5_arr  = sma(closes, 5)
    ma10_arr = sma(closes, 10)
    ma20_arr = sma(closes, 20)
    ma60_arr = sma(closes, 60)
    rsi_arr  = wilder_rsi(closes, 14)
    bb_tops, bb_bots, bb_mids = bollinger(closes, BB_PERIOD, BB_MULT)

    # MACD
    ema12 = ema(closes, MACD_FAST)
    ema26 = ema(closes, MACD_SLOW)
    dif_arr = [a - b if a==a and b==b else float('nan') for a,b in zip(ema12, ema26)]
    dea_arr = ema(dif_arr, MACD_SIG)

    acc_days = 0
    watch_start = None

    buy_blocked = {"near_low": 0, "conv": 0, "slope": 0, "rsi_range": 0,
                   "yy": 0, "not_enough_days": 0, "no_trigger": 0,
                   "rsi_buy_max": 0, "day_pct": 0, "trend_filter": 0}
    buy_fired = 0
    max_streak = 0
    streak_start = None

    print(f"\n{'='*70}")
    print(f"股票: {code}  总 bar 数: {n}  起始分析: {start_date}")
    print(f"{'='*70}")

    for i in range(n):
        dt = dates[i]
        dt_str = str(dt)[:10]

        # 只分析 start_date 以后的数据
        if dt_str < start_date:
            continue

        close = closes[i]
        open_ = opens[i]
        rsi   = rsi_arr[i]
        dif   = dif_arr[i]
        dea   = dea_arr[i]
        ma20  = ma20_arr[i]
        ma60  = ma60_arr[i]

        if any(v != v for v in [rsi, dif, dea, ma20]):
            continue

        # ── is_accumulating ──
        nl  = near_low(closes, highs, lows, i, LOW_LOOKBACK)
        conv= ma_convergence(ma5_arr, ma10_arr, ma20_arr, i)
        slp = ma_slope(ma20_arr, i, 5)
        yy  = yang_yin_ratio(closes, opens, volumes, i, VOL_LOOKBACK)

        cond_near_low = nl is not None and nl <= MAX_ABOVE_LOW_PCT
        cond_conv     = conv is not None and conv <= MA_CONVERGE_PCT
        cond_slope    = slp is None or abs(slp) <= MA_SLOPE_MAX
        cond_rsi      = RSI_WATCH_MIN <= rsi <= RSI_WATCH_MAX
        cond_yy       = yy is not None and yy >= VOL_YANG_YIN_MIN

        is_acc = cond_near_low and cond_conv and cond_slope and cond_rsi and cond_yy

        if is_acc:
            acc_days += 1
            if watch_start is None:
                watch_start = dt_str
                streak_start = dt_str
            if acc_days > max_streak:
                max_streak = acc_days
        else:
            if acc_days > 0:
                fail_reasons = []
                if not cond_near_low: fail_reasons.append(f"near_low={nl:.1f}%" if nl else "near_low=None")
                if not cond_conv:     fail_reasons.append(f"conv={conv:.1f}%" if conv else "conv=None")
                if not cond_slope:    fail_reasons.append(f"slope={slp:.5f}" if slp else "slope=None")
                if not cond_rsi:      fail_reasons.append(f"rsi={rsi:.1f}")
                if not cond_yy:       fail_reasons.append(f"yy={yy:.2f}" if yy else "yy=None")
                if acc_days >= MIN_WATCH_DAYS:
                    print(f"  [{dt_str}] 建仓期中断！(已满足{acc_days}天≥{MIN_WATCH_DAYS}) 原因: {' '.join(fail_reasons)}")
                else:
                    pass  # 未达到门槛的中断不打印
            acc_days = 0
            watch_start = None
            streak_start = None
            continue

        # 不够天数
        if watch_start is None or acc_days < MIN_WATCH_DAYS:
            continue

        # ── 触发信号检测 ──
        trigger = ""
        trigger_strength = 0

        # A: 放量大阳线突破
        is_bull = close >= open_
        pct_chg = (close - closes[i-1]) / closes[i-1] * 100 if i > 0 and closes[i-1] > 0 else 0
        vr = vol_ratio(volumes, i, 5)
        if is_bull and pct_chg >= BREAKOUT_PCT and vr and vr >= BREAKOUT_VOL_RATIO:
            trigger = f"[A]放量突破+{pct_chg:.1f}% 量比{vr:.1f}x"
            trigger_strength = 3

        # B: 突破布林上轨
        if not trigger:
            bb_top = bb_tops[i]
            if bb_top == bb_top and close > bb_top:
                trigger = f"[B]突破布林上轨{bb_top:.2f}"
                trigger_strength = 3

        # E: 三线多头发散
        if not trigger:
            nl_e = nl
            in_range_e = nl_e is not None and nl_e <= MAX_ABOVE_LOW_PCT * 1.5
            if in_range_e:
                div_ok, div_desc = check_ma_diverge(ma5_arr, ma10_arr, ma20_arr, i, MA_DIVERGE_LOOKBACK)
                if div_ok:
                    trigger = f"[E]{div_desc}"
                    trigger_strength = 3

        # F: 量萎缩后放大
        if not trigger:
            in_range_f = nl is not None and nl <= MAX_ABOVE_LOW_PCT * 1.5
            if in_range_f:
                vsb_ok, vsb_desc = check_vol_shrink_expand(closes, opens, volumes, i)
                if vsb_ok:
                    trigger = f"[F]{vsb_desc}"
                    trigger_strength = 3

        # C: 均线多头发散 + MA20 斜率转正
        if not trigger:
            ma5  = ma5_arr[i]
            ma10 = ma10_arr[i]
            ma20_slope_now = ma_slope(ma20_arr, i, 5)
            if (ma5 == ma5 and ma10 == ma10
                    and ma5 > ma10 > ma20
                    and ma20_slope_now is not None and ma20_slope_now >= MA_SLOPE_UP_MIN):
                prev_slope = None
                if i >= 10:
                    b = ma20_arr[i-10]; m = ma20_arr[i-5]
                    if b == b and m == m and b > 0:
                        prev_slope = (m - b) / b / 5
                if prev_slope is not None and prev_slope < MA_SLOPE_UP_MIN:
                    trigger = f"[C]均线多头发散 MA20↑{ma20_slope_now:.4f}"
                    trigger_strength = 2

        # D: MACD零轴上方金叉
        if not trigger and i > 0:
            prev_dif = dif_arr[i-1]; prev_dea = dea_arr[i-1]
            if (prev_dif == prev_dif and prev_dea == prev_dea
                    and prev_dif <= prev_dea and dif > dea and dif >= 0):
                trigger = "[D]MACD零轴上方金叉"
                trigger_strength = 2

        if not trigger:
            buy_blocked["no_trigger"] += 1
            continue

        # RSI 过滤
        if rsi > RSI_BUY_MAX:
            buy_blocked["rsi_buy_max"] += 1
            continue

        # 单日涨幅过滤
        day_pct = (close - closes[i-1]) / closes[i-1] * 100 if i > 0 and closes[i-1] > 0 else 0
        if day_pct > BREAKOUT_MAX_PCT:
            buy_blocked["day_pct"] += 1
            continue

        # 趋势过滤
        if TREND_FILTER and ma60 == ma60 and ma20 <= ma60:
            late_stage = trigger.startswith("[E]") or trigger.startswith("[F]")
            if late_stage:
                slope_now = ma_slope(ma20_arr, i, 5)
                if not (slope_now is not None and slope_now >= MA_SLOPE_UP_MIN):
                    buy_blocked["trend_filter"] += 1
                    print(f"  [{dt_str}] 趋势过滤拦截(E/F 斜率不足): MA20={ma20:.2f} MA60={ma60:.2f} slope={slope_now} 信号={trigger}")
                    continue
            else:
                buy_blocked["trend_filter"] += 1
                print(f"  [{dt_str}] 趋势过滤拦截: MA20={ma20:.2f} MA60={ma60:.2f} 信号={trigger}")
                continue

        # ── 可以买入 ──
        buy_fired += 1
        print(f"  ★ [{dt_str}] 买入信号! 累计{acc_days}天 {trigger} RSI={rsi:.1f} nl={nl:.1f}% yy={yy:.2f}")

    print(f"\n  汇总: 最长建仓期={max_streak}天 | 买入信号={buy_fired}次")
    print(f"  拦截: {buy_blocked}")


async def main():
    print("加载DB数据中...")
    stocks = await get_all_db_stocks(WARMUP_START, END)
    if not stocks:
        print("ERROR: 数据库无数据")
        return
    codes = [s['code'] for s in stocks if s['code'] in TEST_CODES]
    missing = [c for c in TEST_CODES if c not in [s['code'] for s in stocks]]
    if missing:
        print(f"注意: 以下股票在DB中未找到: {missing}")
    if not codes:
        # 取前5只
        codes = [s['code'] for s in stocks[:5]]
    print(f"分析股票: {codes}")

    db_data = await load_all_db_data(codes, WARMUP_START, END)

    for code in codes:
        df = db_data.get(code)
        if df is None or len(df) < 80:
            print(f"{code}: 数据不足({len(df) if df is not None else 0} bars)")
            continue
        diagnose_stock(code, df, START)


if __name__ == '__main__':
    asyncio.run(main())
