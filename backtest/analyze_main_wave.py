"""
主升浪前建仓期数据特征分析
分析16只标的在主升浪启动前的建仓期特征，用于优化主力建仓策略参数
"""

import asyncio
import json
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 绕过代理
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
           "http_proxy", "https_proxy", "all_proxy"):
    os.environ.pop(_k, None)

from backtest.data_loader import BacktestDataLoader

# ══════════════════════════════════════════════════════════════
# 16只标的主升浪区间定义
# ══════════════════════════════════════════════════════════════

TARGETS = [
    {"code": "300456", "name": "赛微电子", "market": "SZ", "wave_start": "2025-04", "wave_end": "2026-02"},
    {"code": "002155", "name": "湖南黄金", "market": "SZ", "wave_start": "2025-08", "wave_end": "2026-03"},
    {"code": "601069", "name": "西部黄金", "market": "SH", "wave_start": "2024-12", "wave_end": "2026-03"},
    {"code": "600143", "name": "金发科技", "market": "SH", "wave_start": "2025-03", "wave_end": "2025-10"},
    {"code": "002361", "name": "神剑股份", "market": "SZ", "wave_start": "2025-07", "wave_end": "2026-01"},
    {"code": "600310", "name": "广西能源", "market": "SH", "wave_start": "2025-09", "wave_end": "2026-04"},
    {"code": "002790", "name": "瑞尔特",   "market": "SZ", "wave_start": "2025-06", "wave_end": "2025-11"},
    {"code": "002050", "name": "三花智控", "market": "SZ", "wave_start": "2025-02", "wave_end": "2026-01"},
    {"code": "000536", "name": "华映科技", "market": "SZ", "wave_start": "2024-04", "wave_end": "2024-12"},
    {"code": "002721", "name": "金一文化", "market": "SZ", "wave_start": "2024-12", "wave_end": "2025-08"},
    {"code": "300611", "name": "美力科技", "market": "SZ", "wave_start": "2024-09", "wave_end": "2025-04"},
    {"code": "002402", "name": "和而泰",   "market": "SZ", "wave_start": "2025-03", "wave_end": "2025-11"},
    {"code": "002440", "name": "闰土股份", "market": "SZ", "wave_start": "2025-08", "wave_end": "2026-03"},
    {"code": "300053", "name": "航宇微",   "market": "SZ", "wave_start": "2025-06", "wave_end": "2026-03"},
    {"code": "603538", "name": "美诺华",   "market": "SH", "wave_start": "2025-09", "wave_end": "2026-04"},
    {"code": "002192", "name": "融捷股份", "market": "SZ", "wave_start": "2025-04", "wave_end": "2026-04"},
]


def get_date_ranges(target):
    """计算数据加载范围：主升浪前1年（建仓期分析+指标预热）+ 主升浪期间"""
    # wave_start/wave_end 格式: "YYYY-MM" 如 "2025-04"
    ws_str = target["wave_start"]  # e.g. "2025-04"
    we_str = target["wave_end"]    # e.g. "2026-02"

    wave_start = f"{ws_str}-01"

    parts = we_str.split("-")
    y, m = int(parts[0]), int(parts[1])
    if m == 12:
        wave_end = f"{y+1}-01-01"
    else:
        wave_end = f"{y}-{m+1:02d}-01"

    # 建仓期：主升浪前1年（确保指标计算有足够预热）
    ws = datetime.strptime(wave_start, "%Y-%m-%d")
    accum_start = (ws - timedelta(days=365)).strftime("%Y-%m-%d")

    return accum_start, wave_start, wave_end


# ══════════════════════════════════════════════════════════════
# 指标计算函数
# ══════════════════════════════════════════════════════════════

def calc_rsi(closes, period=14):
    """Wilder RSI"""
    rsi_vals = [None] * len(closes)
    if len(closes) < period + 1:
        return rsi_vals
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        diff = closes[i] - closes[i-1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    avg_gain = gains / period
    avg_loss = losses / period
    rs = avg_gain / avg_loss if avg_loss > 1e-9 else 1e9
    rsi_vals[period] = round(100 - 100 / (1 + rs), 2)

    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i-1]
        avg_gain = (avg_gain * (period - 1) + max(diff, 0)) / period
        avg_loss = (avg_loss * (period - 1) + max(-diff, 0)) / period
        rs = avg_gain / avg_loss if avg_loss > 1e-9 else 1e9
        rsi_vals[i] = round(100 - 100 / (1 + rs), 2)
    return rsi_vals


def calc_sma(vals, period):
    result = [None] * len(vals)
    for i in range(period - 1, len(vals)):
        result[i] = sum(vals[i-period+1:i+1]) / period
    return result


def calc_ema(vals, period):
    result = [None] * len(vals)
    k = 2 / (period + 1)
    start = None
    for i in range(len(vals)):
        if vals[i] is None:
            continue
        if start is None:
            result[i] = vals[i]
            start = i
        else:
            result[i] = vals[i] * k + result[i-1] * (1 - k)
    return result


def calc_macd(closes, fast=12, slow=26, signal=9):
    ema_fast = calc_ema(closes, fast)
    ema_slow = calc_ema(closes, slow)
    dif = [None] * len(closes)
    for i in range(len(closes)):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            dif[i] = ema_fast[i] - ema_slow[i]
    dea = calc_ema(dif, signal)
    return dif, dea


def calc_bollinger(closes, period=20, std_dev=2):
    mid = calc_sma(closes, period)
    top = [None] * len(closes)
    bot = [None] * len(closes)
    bw = [None] * len(closes)
    for i in range(period - 1, len(closes)):
        std = np.std(closes[i-period+1:i+1], ddof=0)
        top[i] = mid[i] + std_dev * std
        bot[i] = mid[i] - std_dev * std
        if mid[i] > 0:
            bw[i] = (top[i] - bot[i]) / mid[i] * 100
    return mid, top, bot, bw


# ══════════════════════════════════════════════════════════════
# 主升浪前建仓期特征分析
# ══════════════════════════════════════════════════════════════

def analyze_accumulation(bars, wave_start_date):
    """
    分析主升浪启动前的建仓期特征
    bars: 完整的日线数据
    wave_start_date: 主升浪起始日期
    """
    dates = [b["date"] for b in bars]
    opens = [b["open"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    closes = [b["close"] for b in bars]
    volumes = [b["volume"] for b in bars]
    amounts = [b.get("amount", 0) for b in bars]

    # 找到主升浪起始位置
    wave_idx = None
    for i, d in enumerate(dates):
        if d >= wave_start_date:
            wave_idx = i
            break
    if wave_idx is None or wave_idx < 80:
        return None

    # 计算指标
    ma5 = calc_sma(closes, 5)
    ma10 = calc_sma(closes, 10)
    ma20 = calc_sma(closes, 20)
    ma60 = calc_sma(closes, 60)
    rsi_vals = calc_rsi(closes)
    dif, dea = calc_macd(closes)
    bb_mid, bb_top, bb_bot, bb_bw = calc_bollinger(closes)

    # ═══ 分析建仓期（主升浪前120个交易日范围） ═══
    accum_start = max(60, wave_idx - 120)
    accum_end = wave_idx

    result = {
        "accum_bars": accum_end - accum_start,
        "wave_start_date": dates[wave_idx] if wave_idx < len(dates) else wave_start_date,
    }

    # ── 1. 价格位置特征 ──
    # 分析建仓期价格相对60日低点的位置
    above_low_pcts = []
    for i in range(accum_start, accum_end):
        lookback = min(60, i)
        low_60 = min(lows[i-lookback:i+1])
        if low_60 > 0:
            above_low_pcts.append((closes[i] - low_60) / low_60 * 100)
    result["near_low_mean"] = np.mean(above_low_pcts) if above_low_pcts else None
    result["near_low_median"] = np.median(above_low_pcts) if above_low_pcts else None
    result["near_low_max"] = np.max(above_low_pcts) if above_low_pcts else None

    # ── 2. 均线收敛特征 ──
    convergences = []
    for i in range(accum_start, accum_end):
        if ma5[i] and ma10[i] and ma20[i]:
            vals = [ma5[i], ma10[i], ma20[i]]
            mid_val = sorted(vals)[1]
            if mid_val > 0:
                convergences.append((max(vals) - min(vals)) / mid_val * 100)
    result["ma_conv_mean"] = np.mean(convergences) if convergences else None
    result["ma_conv_median"] = np.median(convergences) if convergences else None
    result["ma_conv_min"] = np.min(convergences) if convergences else None

    # ── 3. MA20斜率特征 ──
    ma20_slopes = []
    for i in range(accum_start, accum_end):
        if i >= 5 and ma20[i] and ma20[i-5] and ma20[i-5] > 0:
            slope = (ma20[i] - ma20[i-5]) / ma20[i-5] / 5
            ma20_slopes.append(slope)
    result["ma20_slope_mean"] = np.mean(ma20_slopes) if ma20_slopes else None
    result["ma20_slope_std"] = np.std(ma20_slopes) if ma20_slopes else None
    result["ma20_slope_min"] = np.min(ma20_slopes) if ma20_slopes else None
    result["ma20_slope_max"] = np.max(ma20_slopes) if ma20_slopes else None

    # ── 4. RSI特征 ──
    rsi_in_accum = [rsi_vals[i] for i in range(accum_start, accum_end) if rsi_vals[i] is not None]
    result["rsi_mean"] = np.mean(rsi_in_accum) if rsi_in_accum else None
    result["rsi_median"] = np.median(rsi_in_accum) if rsi_in_accum else None
    result["rsi_min"] = np.min(rsi_in_accum) if rsi_in_accum else None
    result["rsi_max"] = np.max(rsi_in_accum) if rsi_in_accum else None

    # ── 5. 阳阴量比特征 ──
    yang_yin_ratios = []
    for i in range(accum_start, accum_end):
        lookback = min(30, i)
        yang_vols, yin_vols = [], []
        for j in range(lookback):
            idx = i - j
            if closes[idx] >= opens[idx]:
                yang_vols.append(volumes[idx])
            else:
                yin_vols.append(volumes[idx])
        if yang_vols and yin_vols:
            ratio = (sum(yang_vols)/len(yang_vols)) / (sum(yin_vols)/len(yin_vols))
            yang_yin_ratios.append(ratio)
    result["yy_ratio_mean"] = np.mean(yang_yin_ratios) if yang_yin_ratios else None
    result["yy_ratio_median"] = np.median(yang_yin_ratios) if yang_yin_ratios else None
    result["yy_ratio_min"] = np.min(yang_yin_ratios) if yang_yin_ratios else None

    # ── 6. 布林带宽度特征 ──
    bb_bw_in_accum = [bb_bw[i] for i in range(accum_start, accum_end) if bb_bw[i] is not None]
    result["bb_bw_mean"] = np.mean(bb_bw_in_accum) if bb_bw_in_accum else None
    result["bb_bw_min"] = np.min(bb_bw_in_accum) if bb_bw_in_accum else None

    # ── 7. 成交量特征（相对均量） ──
    vol_ratios = []
    for i in range(accum_start, accum_end):
        if i >= 20:
            avg_vol = np.mean(volumes[i-20:i])
            if avg_vol > 0:
                vol_ratios.append(volumes[i] / avg_vol)
    result["vol_ratio_mean"] = np.mean(vol_ratios) if vol_ratios else None
    result["vol_ratio_std"] = np.std(vol_ratios) if vol_ratios else None

    # ── 7b. 缩幅放量特征（K线柱体小 + 成交量放大）══════════════════
    # 这是主力建仓的典型信号：价格波动小但换手大，说明有大资金在悄悄吸筹
    small_body_big_vol_days = []    # 记录每个缩幅放量日的详情
    small_body_big_vol_count = 0
    total_valid_days = 0

    # 先计算建仓期的平均振幅和平均实体，用于动态阈值
    body_pcts_all = []
    range_pcts_all = []
    for i in range(accum_start, accum_end):
        if closes[i-1] > 0:
            body_pct = abs(closes[i] - opens[i]) / closes[i-1] * 100
            range_pct = (highs[i] - lows[i]) / closes[i-1] * 100
            body_pcts_all.append(body_pct)
            range_pcts_all.append(range_pct)
    avg_body_pct = np.mean(body_pcts_all) if body_pcts_all else 2.0
    avg_range_pct = np.mean(range_pcts_all) if range_pcts_all else 3.0

    for i in range(accum_start, accum_end):
        if i < 20 or closes[i-1] <= 0:
            continue
        total_valid_days += 1

        # K线实体大小（相对前收）
        body = abs(closes[i] - opens[i])
        body_pct = body / closes[i-1] * 100
        # K线振幅
        range_pct = (highs[i] - lows[i]) / closes[i-1] * 100
        # 成交量比（相对20日均量）
        avg_vol = np.mean(volumes[i-20:i])
        if avg_vol <= 0:
            continue
        vol_r = volumes[i] / avg_vol

        # 缩幅放量条件：
        #   1. 实体小于平均实体（价格变动小）
        #   2. 成交量 ≥ 1.5倍均量（放量）
        is_small_body = body_pct <= avg_body_pct * 0.8  # 实体小于均值80%
        is_big_vol = vol_r >= 1.5                         # 量比≥1.5

        if is_small_body and is_big_vol:
            small_body_big_vol_count += 1
            small_body_big_vol_days.append({
                "idx": i,
                "date": dates[i],
                "body_pct": round(body_pct, 3),
                "range_pct": round(range_pct, 3),
                "vol_ratio": round(vol_r, 2),
                "is_yang": closes[i] >= opens[i],
            })

    sbv_freq = small_body_big_vol_count / total_valid_days if total_valid_days > 0 else 0
    result["sbv_count"] = small_body_big_vol_count
    result["sbv_frequency"] = round(sbv_freq, 4)  # 出现频率
    result["sbv_total_days"] = total_valid_days
    result["avg_body_pct"] = round(avg_body_pct, 3)
    result["avg_range_pct"] = round(avg_range_pct, 3)

    # 缩幅放量日的统计特征
    if small_body_big_vol_days:
        sbv_vols = [d["vol_ratio"] for d in small_body_big_vol_days]
        sbv_bodies = [d["body_pct"] for d in small_body_big_vol_days]
        sbv_yang_count = sum(1 for d in small_body_big_vol_days if d["is_yang"])
        result["sbv_vol_ratio_mean"] = round(np.mean(sbv_vols), 2)
        result["sbv_vol_ratio_max"] = round(np.max(sbv_vols), 2)
        result["sbv_body_pct_mean"] = round(np.mean(sbv_bodies), 3)
        result["sbv_yang_ratio"] = round(sbv_yang_count / len(small_body_big_vol_days), 2)

        # 缩幅放量在建仓期最后30天的集中度（建仓末期更密集=即将突破）
        last_30_start = max(accum_start, accum_end - 30)
        sbv_in_last30 = sum(1 for d in small_body_big_vol_days if d["idx"] >= last_30_start)
        result["sbv_last30_count"] = sbv_in_last30
        result["sbv_last30_ratio"] = round(sbv_in_last30 / len(small_body_big_vol_days), 2) if small_body_big_vol_days else 0
    else:
        result["sbv_vol_ratio_mean"] = None
        result["sbv_vol_ratio_max"] = None
        result["sbv_body_pct_mean"] = None
        result["sbv_yang_ratio"] = None
        result["sbv_last30_count"] = 0
        result["sbv_last30_ratio"] = 0

    # ── 7c. 连续缩幅放量天数统计 ──
    # 检查建仓期中是否有连续多天的缩幅放量集群
    max_consec_sbv = 0
    cur_consec = 0
    for i in range(accum_start, accum_end):
        if i < 20 or closes[i-1] <= 0:
            cur_consec = 0
            continue
        body_pct = abs(closes[i] - opens[i]) / closes[i-1] * 100
        avg_vol = np.mean(volumes[i-20:i])
        vol_r = volumes[i] / avg_vol if avg_vol > 0 else 0
        if body_pct <= avg_body_pct * 0.8 and vol_r >= 1.3:  # 连续检测用更宽松阈值
            cur_consec += 1
            max_consec_sbv = max(max_consec_sbv, cur_consec)
        else:
            cur_consec = 0
    result["max_consec_sbv_days"] = max_consec_sbv

    # ── 8. 突破日特征（主升浪启动的第一波）──
    # 找到主升浪起始附近的首个大阳线
    breakout_info = None
    for i in range(wave_idx, min(wave_idx + 30, len(closes))):
        if i < 1:
            continue
        pct_chg = (closes[i] - closes[i-1]) / closes[i-1] * 100
        if pct_chg >= 3.0 and closes[i] > opens[i]:
            avg_vol_5 = np.mean(volumes[max(0,i-5):i]) if i >= 5 else volumes[i]
            vr = volumes[i] / avg_vol_5 if avg_vol_5 > 0 else 0
            breakout_info = {
                "date": dates[i],
                "pct_change": round(pct_chg, 2),
                "vol_ratio": round(vr, 2),
                "rsi_at_breakout": rsi_vals[i],
                "days_from_wave_start": i - wave_idx,
            }
            # MA关系
            if ma20[i] and ma60[i]:
                breakout_info["ma20_above_ma60"] = ma20[i] > ma60[i]
            if dif[i] is not None and dea[i] is not None:
                breakout_info["macd_dif"] = round(dif[i], 4)
                breakout_info["macd_above_zero"] = dif[i] > 0
            break
    result["breakout"] = breakout_info

    # ── 9. 建仓期持续时间（MA收敛天数统计） ──
    # 统计建仓期中连续满足收敛条件的最长天数
    consecutive_days = 0
    max_consecutive = 0
    for i in range(accum_start, accum_end):
        if (ma5[i] and ma10[i] and ma20[i] and
            rsi_vals[i] is not None):
            vals = [ma5[i], ma10[i], ma20[i]]
            mid_val = sorted(vals)[1]
            conv = (max(vals) - min(vals)) / mid_val * 100 if mid_val > 0 else 999

            # 宽松条件检测建仓
            slope = None
            if i >= 5 and ma20[i] and ma20[i-5] and ma20[i-5] > 0:
                slope = (ma20[i] - ma20[i-5]) / ma20[i-5] / 5

            is_accum = (
                conv <= 8.0 and  # 放宽到8%来看
                (slope is None or abs(slope) <= 0.08) and
                25 <= rsi_vals[i] <= 65
            )
            if is_accum:
                consecutive_days += 1
                max_consecutive = max(max_consecutive, consecutive_days)
            else:
                consecutive_days = 0
        else:
            consecutive_days = 0
    result["max_consecutive_accum_days"] = max_consecutive

    # ── 10. 主升浪涨幅统计 ──
    wave_end_idx = min(len(closes) - 1, wave_idx + 200)
    wave_high = max(highs[wave_idx:wave_end_idx+1])
    wave_low = closes[wave_idx]
    result["wave_gain_pct"] = round((wave_high - wave_low) / wave_low * 100, 1) if wave_low > 0 else 0

    # ── 11. MA20>MA60 在建仓期末段的状态 ──
    ma20_above_ma60_count = 0
    check_range = range(max(accum_start, accum_end - 20), accum_end)
    for i in check_range:
        if ma20[i] and ma60[i] and ma20[i] > ma60[i]:
            ma20_above_ma60_count += 1
    result["ma20_above_ma60_ratio"] = ma20_above_ma60_count / len(check_range) if len(check_range) > 0 else 0

    return result


async def main():
    loader = BacktestDataLoader()

    print("=" * 80)
    print("主升浪前建仓期数据特征分析")
    print("=" * 80)

    all_results = []

    for t in TARGETS:
        accum_start, wave_start, wave_end = get_date_ranges(t)
        print(f"\n{'─'*60}")
        print(f"📊 {t['name']}({t['code']}) | 主升浪: {wave_start} ~ {wave_end}")

        bars = await loader.load_daily_bars(t["code"], t["market"], accum_start, wave_end)
        if not bars or len(bars) < 80:
            print(f"  ❌ 数据不足 (获取{len(bars) if bars else 0}条)")
            continue

        result = analyze_accumulation(bars, wave_start)
        if result is None:
            print(f"  ❌ 分析失败（建仓期数据不足）")
            continue

        result["code"] = t["code"]
        result["name"] = t["name"]
        all_results.append(result)

        print(f"  建仓期: {result['accum_bars']}个交易日")
        print(f"  主升浪涨幅: {result['wave_gain_pct']}%")
        print(f"  价格位置(近60日低点): 均值{result['near_low_mean']:.1f}% 中位{result['near_low_median']:.1f}% 最大{result['near_low_max']:.1f}%")
        print(f"  MA收敛度: 均值{result['ma_conv_mean']:.2f}% 中位{result['ma_conv_median']:.2f}% 最小{result['ma_conv_min']:.2f}%")
        print(f"  MA20斜率: 均值{result['ma20_slope_mean']:.5f} 范围[{result['ma20_slope_min']:.5f}, {result['ma20_slope_max']:.5f}]")
        print(f"  RSI: 均值{result['rsi_mean']:.1f} 范围[{result['rsi_min']:.1f}, {result['rsi_max']:.1f}]")
        print(f"  阳阴量比: 均值{result['yy_ratio_mean']:.3f} 中位{result['yy_ratio_median']:.3f} 最小{result['yy_ratio_min']:.3f}")
        print(f"  布林带宽: 均值{result['bb_bw_mean']:.2f}% 最小{result['bb_bw_min']:.2f}%")
        print(f"  成交量比: 均值{result['vol_ratio_mean']:.2f} 标差{result['vol_ratio_std']:.2f}")
        print(f"  ── 缩幅放量特征 ──")
        print(f"  缩幅放量天数: {result['sbv_count']}/{result['sbv_total_days']}天 (频率{result['sbv_frequency']:.1%})")
        if result.get("sbv_vol_ratio_mean"):
            print(f"  缩幅放量日量比: 均值{result['sbv_vol_ratio_mean']:.2f}x 最大{result['sbv_vol_ratio_max']:.2f}x")
            print(f"  缩幅放量日实体: 均值{result['sbv_body_pct_mean']:.3f}% (整体均实体{result['avg_body_pct']:.3f}%)")
            print(f"  缩幅放量阳线占比: {result['sbv_yang_ratio']:.0%}")
            print(f"  最后30天集中度: {result['sbv_last30_count']}天/{result['sbv_count']}天 ({result['sbv_last30_ratio']:.0%})")
            print(f"  最长连续缩幅放量: {result['max_consec_sbv_days']}天")
        print(f"  最长连续建仓天数(宽松): {result['max_consecutive_accum_days']}天")
        print(f"  建仓末期MA20>MA60比例: {result['ma20_above_ma60_ratio']:.1%}")

        if result.get("breakout"):
            bo = result["breakout"]
            print(f"  突破日: {bo['date']} 涨幅{bo['pct_change']}% 量比{bo['vol_ratio']}x RSI={bo.get('rsi_at_breakout','N/A')}")
            print(f"         MA20>MA60={bo.get('ma20_above_ma60','N/A')} MACD>0={bo.get('macd_above_zero','N/A')}")

    # ══════════════════════════════════════════════════════════════
    # 汇总统计
    # ══════════════════════════════════════════════════════════════
    if not all_results:
        print("\n没有足够的数据进行分析")
        return

    print("\n" + "=" * 80)
    print("汇总统计（全部标的）")
    print("=" * 80)

    def stat(key, fmt=".2f"):
        vals = [r[key] for r in all_results if r.get(key) is not None]
        if not vals:
            return "N/A"
        return f"均值={np.mean(vals):{fmt}} 中位={np.median(vals):{fmt}} 范围=[{np.min(vals):{fmt}}, {np.max(vals):{fmt}}] (n={len(vals)})"

    print(f"\n📈 主升浪涨幅: {stat('wave_gain_pct', '.1f')}")
    print(f"\n🔍 建仓期特征:")
    print(f"  价格位置(近低点%):   {stat('near_low_mean', '.1f')}")
    print(f"  MA收敛度(%):         {stat('ma_conv_mean', '.2f')}")
    print(f"  MA20斜率:            {stat('ma20_slope_mean', '.5f')}")
    print(f"  RSI均值:             {stat('rsi_mean', '.1f')}")
    print(f"  RSI最小值:           {stat('rsi_min', '.1f')}")
    print(f"  RSI最大值:           {stat('rsi_max', '.1f')}")
    print(f"  阳阴量比:            {stat('yy_ratio_mean', '.3f')}")
    print(f"  阳阴量比最小值:      {stat('yy_ratio_min', '.3f')}")
    print(f"  布林带宽(%):         {stat('bb_bw_mean', '.2f')}")
    print(f"  最长连续建仓天数:    {stat('max_consecutive_accum_days', '.0f')}")
    print(f"  建仓末期MA20>MA60:   {stat('ma20_above_ma60_ratio', '.1%')}")

    print(f"\n📦 缩幅放量特征 (K线柱体小+成交量放大):")
    print(f"  出现频率:            {stat('sbv_frequency', '.1%')}")
    print(f"  出现天数:            {stat('sbv_count', '.0f')}")
    print(f"  量比均值:            {stat('sbv_vol_ratio_mean', '.2f')}")
    print(f"  量比最大:            {stat('sbv_vol_ratio_max', '.2f')}")
    print(f"  实体均值(%):         {stat('sbv_body_pct_mean', '.3f')}")
    print(f"  阳线占比:            {stat('sbv_yang_ratio', '.0%')}")
    print(f"  最后30天集中度:      {stat('sbv_last30_ratio', '.0%')}")
    print(f"  最长连续天数:        {stat('max_consec_sbv_days', '.0f')}")

    # 突破日特征汇总
    breakouts = [r["breakout"] for r in all_results if r.get("breakout")]
    if breakouts:
        print(f"\n⚡ 突破日特征 (n={len(breakouts)}):")
        pcts = [b["pct_change"] for b in breakouts]
        vrs = [b["vol_ratio"] for b in breakouts]
        rsis = [b["rsi_at_breakout"] for b in breakouts if b.get("rsi_at_breakout")]
        ma_flags = [b.get("ma20_above_ma60") for b in breakouts if b.get("ma20_above_ma60") is not None]
        macd_flags = [b.get("macd_above_zero") for b in breakouts if b.get("macd_above_zero") is not None]

        print(f"  涨幅: 均值={np.mean(pcts):.2f}% 中位={np.median(pcts):.2f}% 范围=[{np.min(pcts):.2f}%, {np.max(pcts):.2f}%]")
        print(f"  量比: 均值={np.mean(vrs):.2f}x 中位={np.median(vrs):.2f}x 范围=[{np.min(vrs):.2f}x, {np.max(vrs):.2f}x]")
        if rsis:
            print(f"  RSI:  均值={np.mean(rsis):.1f} 中位={np.median(rsis):.1f} 范围=[{np.min(rsis):.1f}, {np.max(rsis):.1f}]")
        if ma_flags:
            print(f"  MA20>MA60: {sum(1 for x in ma_flags if x)}/{len(ma_flags)} ({sum(1 for x in ma_flags if x)/len(ma_flags):.0%})")
        if macd_flags:
            print(f"  MACD>0:    {sum(1 for x in macd_flags if x)}/{len(macd_flags)} ({sum(1 for x in macd_flags if x)/len(macd_flags):.0%})")

    # ══════════════════════════════════════════════════════════════
    # 与当前策略参数对比 & 优化建议
    # ══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("当前策略参数 vs 数据特征 → 优化建议")
    print("=" * 80)

    current_params = {
        "max_above_low_pct": 20.0,
        "ma_converge_pct": 5.0,
        "ma_slope_max": 0.05,
        "rsi_watch_min": 25.0,
        "rsi_watch_max": 62.0,
        "vol_yang_yin_min": 1.03,
        "min_watch_days": 20,
        "breakout_pct": 4.0,
        "breakout_vol_ratio": 2.0,
        "rsi_buy_max": 70.0,
        "breakout_max_pct": 8.0,
        "trend_filter (MA20>MA60)": True,
    }

    # 计算覆盖率
    near_low_means = [r["near_low_mean"] for r in all_results if r.get("near_low_mean") is not None]
    conv_means = [r["ma_conv_mean"] for r in all_results if r.get("ma_conv_mean") is not None]
    slope_maxs = [r["ma20_slope_max"] for r in all_results if r.get("ma20_slope_max") is not None]
    rsi_mins = [r["rsi_min"] for r in all_results if r.get("rsi_min") is not None]
    rsi_maxs = [r["rsi_max"] for r in all_results if r.get("rsi_max") is not None]
    yy_mins = [r["yy_ratio_min"] for r in all_results if r.get("yy_ratio_min") is not None]
    consec_days = [r["max_consecutive_accum_days"] for r in all_results]
    ma_ratios = [r["ma20_above_ma60_ratio"] for r in all_results if r.get("ma20_above_ma60_ratio") is not None]

    print(f"\n{'参数':<30} {'当前值':<12} {'数据特征':<40} {'建议'}")
    print("─" * 120)

    # max_above_low_pct
    if near_low_means:
        covered = sum(1 for v in near_low_means if v <= 20) / len(near_low_means)
        print(f"{'max_above_low_pct':<30} {'20%':<12} {'均值建仓期平均 ' + f'{np.mean(near_low_means):.1f}% 覆盖率{covered:.0%}':<40} "
              f"{'→ 适当放宽' if covered < 0.7 else '→ 合适'}")

    # ma_converge_pct
    if conv_means:
        covered = sum(1 for v in conv_means if v <= 5) / len(conv_means)
        print(f"{'ma_converge_pct':<30} {'5%':<12} {'均值建仓期平均 ' + f'{np.mean(conv_means):.2f}% 覆盖率{covered:.0%}':<40} "
              f"{'→ 适当放宽' if covered < 0.7 else '→ 合适'}")

    # ma_slope_max
    if slope_maxs:
        covered = sum(1 for v in slope_maxs if v <= 0.05) / len(slope_maxs)
        print(f"{'ma_slope_max':<30} {'0.05':<12} {'最大斜率 ' + f'{np.mean(slope_maxs):.5f} 覆盖率{covered:.0%}':<40} "
              f"{'→ 适当放宽' if covered < 0.7 else '→ 合适'}")

    # RSI范围
    if rsi_mins:
        covered = sum(1 for v in rsi_mins if v >= 25) / len(rsi_mins)
        print(f"{'rsi_watch_min':<30} {'25':<12} {'最小RSI均值 ' + f'{np.mean(rsi_mins):.1f} 覆盖率{covered:.0%}':<40} "
              f"{'→ 适当降低' if covered < 0.7 else '→ 合适'}")
    if rsi_maxs:
        covered = sum(1 for v in rsi_maxs if v <= 62) / len(rsi_maxs)
        print(f"{'rsi_watch_max':<30} {'62':<12} {'最大RSI均值 ' + f'{np.mean(rsi_maxs):.1f} 覆盖率{covered:.0%}':<40} "
              f"{'→ 适当放宽' if covered < 0.7 else '→ 合适'}")

    # 阳阴量比
    if yy_mins:
        covered = sum(1 for v in yy_mins if v >= 1.03) / len(yy_mins)
        print(f"{'vol_yang_yin_min':<30} {'1.03':<12} {'最小值均值 ' + f'{np.mean(yy_mins):.3f} 覆盖率{covered:.0%}':<40} "
              f"{'→ 适当降低' if covered < 0.7 else '→ 合适'}")

    # 建仓天数
    if consec_days:
        covered = sum(1 for v in consec_days if v >= 20) / len(consec_days)
        print(f"{'min_watch_days':<30} {'20天':<12} {'最长连续天数均值 ' + f'{np.mean(consec_days):.0f} 覆盖率{covered:.0%}':<40} "
              f"{'→ 适当降低' if covered < 0.7 else '→ 合适'}")

    # 突破日参数
    if breakouts:
        pcts = [b["pct_change"] for b in breakouts]
        vrs = [b["vol_ratio"] for b in breakouts]

        pct_covered = sum(1 for v in pcts if v >= 4.0) / len(pcts)
        print(f"{'breakout_pct':<30} {'4.0%':<12} {'突破涨幅均值 ' + f'{np.mean(pcts):.2f}% 覆盖率{pct_covered:.0%}':<40} "
              f"{'→ 适当降低' if pct_covered < 0.7 else '→ 合适'}")

        vr_covered = sum(1 for v in vrs if v >= 2.0) / len(vrs)
        print(f"{'breakout_vol_ratio':<30} {'2.0x':<12} {'突破量比均值 ' + f'{np.mean(vrs):.2f}x 覆盖率{vr_covered:.0%}':<40} "
              f"{'→ 适当降低' if vr_covered < 0.7 else '→ 合适'}")

    # trend_filter
    if ma_ratios:
        high_ratio = sum(1 for v in ma_ratios if v >= 0.5) / len(ma_ratios)
        print(f"{'trend_filter(MA20>MA60)':<30} {'True':<12} {'建仓末期满足比例 ' + f'{np.mean(ma_ratios):.0%} 高比例占比{high_ratio:.0%}':<40} "
              f"{'→ 可能过严' if high_ratio < 0.6 else '→ 合适'}")

    # 保存分析结果
    output_path = ROOT / "backtest_reports" / "main_wave_analysis.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n详细分析结果已保存: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
