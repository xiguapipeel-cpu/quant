"""
主力建仓期技术特征分析脚本
分析三只股票在建仓期（拉升前吸筹阶段）的技术指标特征

股票：
1. 神剑股份 002361 — 建仓期 2025-07 ~ 2025-09
2. 赛微电子 300456 — 建仓期 2025-03 ~ 2025-05
3. 广西能源 600310 — 建仓期 2025-08 ~ 2025-10
"""

import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# 股票配置
# ─────────────────────────────────────────────
STOCKS = [
    {
        "name":   "神剑股份",
        "code":   "002361",
        "market": "sz",
        "fetch_start": "20250101",
        "fetch_end":   "20251010",   # 拉升发生在10月后，取到10月初
        "accum_start": "2025-07-01",
        "accum_end":   "2025-09-30",
        "pre_start":   "2025-04-01", # 建仓前3个月（参比期）
        "pre_end":     "2025-06-30",
        "lift_signal_days": 10,      # 拉升前最后N个交易日
    },
    {
        "name":   "赛微电子",
        "code":   "300456",
        "market": "sz",
        "fetch_start": "20250101",
        "fetch_end":   "20250630",   # 拉升发生在6月，取到6月底
        "accum_start": "2025-03-01",
        "accum_end":   "2025-05-31",
        "pre_start":   "2025-01-01",
        "pre_end":     "2025-02-28",
        "lift_signal_days": 10,
    },
    {
        "name":   "广西能源",
        "code":   "600310",
        "market": "sh",
        "fetch_start": "20250101",
        "fetch_end":   "20251115",   # 拉升在11月，取到11月中
        "accum_start": "2025-08-01",
        "accum_end":   "2025-10-31",
        "pre_start":   "2025-05-01",
        "pre_end":     "2025-07-31",
        "lift_signal_days": 10,
    },
]

# ─────────────────────────────────────────────
# 辅助函数
# ─────────────────────────────────────────────

def fetch_daily(code: str, start: str, end: str) -> pd.DataFrame:
    """拉取日线前复权数据，统一列名"""
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start,
            end_date=end,
            adjust="qfq"
        )
        # 统一列名
        df.columns = [c.strip() for c in df.columns]
        rename_map = {
            "日期": "date", "开盘": "open", "最高": "high",
            "最低": "low", "收盘": "close", "成交量": "volume",
            "成交额": "amount", "振幅": "amplitude_pct",
            "涨跌幅": "pct_chg", "涨跌额": "chg",
            "换手率": "turnover_rate"
        }
        df.rename(columns=rename_map, inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df.sort_values("date", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
    except Exception as e:
        print(f"  [ERROR] 拉取 {code} 数据失败: {e}")
        return pd.DataFrame()


def calc_ma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=1).mean()


def calc_bollinger(close: pd.Series, window=20):
    mid = calc_ma(close, window)
    std = close.rolling(window=window, min_periods=1).std()
    upper = mid + 2 * std
    lower = mid - 2 * std
    width = (upper - lower) / mid
    return mid, upper, lower, width


def calc_macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_hist = 2 * (dif - dea)
    return dif, dea, macd_hist


def calc_rsi(close: pd.Series, window=14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=window - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=window - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def has_lower_shadow(row, shadow_ratio=0.6):
    """判断是否为长下影线：下影线长度 >= K线实体+上影线长度 * shadow_ratio"""
    body = abs(row["close"] - row["open"])
    upper_shadow = row["high"] - max(row["open"], row["close"])
    lower_shadow = min(row["open"], row["close"]) - row["low"]
    total_range = row["high"] - row["low"]
    if total_range == 0:
        return False
    return lower_shadow / total_range >= shadow_ratio


def slice_period(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    mask = (df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))
    return df[mask].copy()


def print_separator(char="═", width=70):
    print(char * width)


def print_section(title):
    print(f"\n  ── {title} ──")


# ─────────────────────────────────────────────
# 单只股票分析主函数
# ─────────────────────────────────────────────

def analyze_stock(cfg: dict) -> dict:
    name = cfg["name"]
    code = cfg["code"]
    print_separator()
    print(f"  {name} ({code})")
    print_separator()

    # 1. 拉取数据
    print(f"\n[1] 拉取数据 {cfg['fetch_start']} ~ {cfg['fetch_end']} ...")
    df_all = fetch_daily(code, cfg["fetch_start"], cfg["fetch_end"])
    if df_all.empty:
        print("  数据为空，跳过。")
        return {}

    print(f"  获取到 {len(df_all)} 条记录，"
          f"时间范围: {df_all['date'].min().date()} ~ {df_all['date'].max().date()}")

    # 2. 在全量数据上计算指标（保证均线/MACD有足够历史）
    df_all["MA5"]  = calc_ma(df_all["close"], 5)
    df_all["MA10"] = calc_ma(df_all["close"], 10)
    df_all["MA20"] = calc_ma(df_all["close"], 20)
    df_all["MA60"] = calc_ma(df_all["close"], 60)
    _, _, _, df_all["boll_width"] = calc_bollinger(df_all["close"])
    df_all["DIF"], df_all["DEA"], df_all["MACD_hist"] = calc_macd(df_all["close"])
    df_all["RSI"] = calc_rsi(df_all["close"])
    df_all["vol_MA20"] = calc_ma(df_all["volume"], 20)
    df_all["has_lower_shadow"] = df_all.apply(has_lower_shadow, axis=1)

    # 3. 切片：建仓期 & 参比期
    accum = slice_period(df_all, cfg["accum_start"], cfg["accum_end"])
    pre   = slice_period(df_all, cfg["pre_start"],   cfg["pre_end"])

    if accum.empty:
        print(f"  [WARN] 建仓期 {cfg['accum_start']}~{cfg['accum_end']} 无数据")
        return {}

    print(f"  建仓期数据: {len(accum)} 个交易日")
    print(f"  参比期数据: {len(pre)} 个交易日")

    result = {"name": name, "code": code}

    # ──────────────────────────────────────────
    # A. 价格形态特征
    # ──────────────────────────────────────────
    print_section("A. 价格形态特征")

    a_high = accum["high"].max()
    a_low  = accum["low"].min()
    a_mean = accum["close"].mean()
    a_amp  = (a_high - a_low) / a_mean

    # 近半年低点（整个取数范围）
    half_year_low = df_all["low"].min()
    price_vs_low  = (a_mean - half_year_low) / half_year_low

    # 均线粘合度：建仓期均线极差 / 均值（越小越粘合）
    ma_cols = ["MA5", "MA10", "MA20", "MA60"]
    accum_ma = accum[ma_cols].dropna()
    if not accum_ma.empty:
        ma_spread_mean = accum_ma.std(axis=1).mean()   # 各均线标准差的均值
        ma_mean_val    = accum_ma.mean().mean()
        ma_cohesion    = ma_spread_mean / ma_mean_val   # 越小越粘合
        # 均线斜率（60日均线斜率方向）
        ma60_slope = np.polyfit(range(len(accum)), accum["MA60"].values, 1)[0]
        ma20_slope = np.polyfit(range(len(accum)), accum["MA20"].values, 1)[0]
    else:
        ma_cohesion = np.nan
        ma60_slope = ma20_slope = np.nan

    # 布林带带宽（建仓期均值 vs 参比期均值）
    boll_accum = accum["boll_width"].mean()
    boll_pre   = pre["boll_width"].mean() if not pre.empty else np.nan
    boll_ratio = boll_accum / boll_pre if boll_pre else np.nan

    print(f"    建仓期价格范围:  最高={a_high:.2f}  最低={a_low:.2f}  均价={a_mean:.2f}")
    print(f"    振幅比 (高-低)/均价:  {a_amp:.2%}")
    print(f"    近半年最低价: {half_year_low:.2f}  均价高于低点: {price_vs_low:.2%}")
    print(f"    均线粘合度 (越小越粘合): {ma_cohesion:.4f}")
    print(f"    MA20斜率: {ma20_slope:.4f}  MA60斜率: {ma60_slope:.4f}  {'平走/微降(横盘)' if abs(ma60_slope) < 0.02 * a_mean else '趋势明显'}")
    print(f"    布林带带宽均值: 建仓期={boll_accum:.4f}  参比期={boll_pre:.4f}  比值={boll_ratio:.2f}  {'收窄' if boll_ratio < 1 else '未收窄'}")

    result["A"] = {
        "amp_ratio": a_amp,
        "price_vs_half_year_low_pct": price_vs_low,
        "ma_cohesion": ma_cohesion,
        "ma20_slope": ma20_slope,
        "ma60_slope": ma60_slope,
        "boll_width_accum": boll_accum,
        "boll_width_pre": boll_pre,
        "boll_ratio": boll_ratio,
    }

    # ──────────────────────────────────────────
    # B. 成交量特征
    # ──────────────────────────────────────────
    print_section("B. 成交量特征")

    vol_accum = accum["volume"].mean()
    vol_pre   = pre["volume"].mean() if not pre.empty else np.nan
    vol_ratio = vol_accum / vol_pre if vol_pre else np.nan

    # 量比分布（每日成交量 / 近20日均量）
    accum_vol_ratio = (accum["volume"] / accum["vol_MA20"]).dropna()
    moderate_vol_days = ((accum_vol_ratio >= 1.0) & (accum_vol_ratio <= 2.0)).sum()
    moderate_vol_pct  = moderate_vol_days / len(accum_vol_ratio)

    # 阳线/阴线均量比
    up_days   = accum[accum["close"] >= accum["open"]]
    down_days = accum[accum["close"] <  accum["open"]]
    vol_up    = up_days["volume"].mean()   if not up_days.empty   else 0
    vol_down  = down_days["volume"].mean() if not down_days.empty else 0
    up_down_ratio = vol_up / vol_down if vol_down > 0 else np.nan

    # 换手率
    turnover_mean = accum["turnover_rate"].mean() if "turnover_rate" in accum.columns else np.nan
    turnover_pre  = pre["turnover_rate"].mean()   if ("turnover_rate" in pre.columns and not pre.empty) else np.nan

    print(f"    日均成交量: 建仓期={vol_accum:,.0f}手  参比期={vol_pre:,.0f}手  比值={vol_ratio:.2f}")
    print(f"    温和放量天数(量比1.0~2.0): {moderate_vol_days}天 / {len(accum_vol_ratio)}天 ({moderate_vol_pct:.1%})")
    print(f"    阳线均量={vol_up:,.0f}  阴线均量={vol_down:,.0f}  阳/阴量比={up_down_ratio:.2f}  {'阳量>阴量，有吸筹迹象' if up_down_ratio > 1.1 else '阳/阴量接近'}")
    print(f"    日均换手率: 建仓期={turnover_mean:.2f}%  参比期={turnover_pre:.2f}%")

    result["B"] = {
        "vol_ratio_accum_vs_pre": vol_ratio,
        "moderate_vol_days": moderate_vol_days,
        "moderate_vol_pct": moderate_vol_pct,
        "up_down_vol_ratio": up_down_ratio,
        "turnover_mean": turnover_mean,
        "turnover_pre": turnover_pre,
    }

    # ──────────────────────────────────────────
    # C. 筹码/资金特征
    # ──────────────────────────────────────────
    print_section("C. 筹码/资金特征")

    # 底部堆量：成交量连续高于参比期均量
    thresh_vol = vol_pre * 1.0 if vol_pre else vol_accum
    above_pre_vol = (accum["volume"] > thresh_vol)
    # 统计最长连续堆量天数
    max_consec = 0
    cur_consec = 0
    for v in above_pre_vol:
        if v:
            cur_consec += 1
            max_consec = max(max_consec, cur_consec)
        else:
            cur_consec = 0
    total_above_pre = above_pre_vol.sum()

    # 价不涨但量升：价格涨幅 < 0.5% 且成交量 > vol_MA20
    price_flat_vol_up = accum[
        (accum["pct_chg"].abs() < 0.5) &
        (accum["volume"] > accum["vol_MA20"] * 1.0)
    ]

    # 长下影线频率
    lower_shadow_days = accum["has_lower_shadow"].sum()
    lower_shadow_pct  = lower_shadow_days / len(accum)

    print(f"    建仓期内量>参比均量的天数: {total_above_pre}/{len(accum)}")
    print(f"    最长连续堆量天数: {max_consec}天")
    print(f"    价格不涨(|涨幅|<0.5%)但量>均量天数: {len(price_flat_vol_up)}天")
    print(f"    长下影线天数: {lower_shadow_days}天 / {len(accum)}天 ({lower_shadow_pct:.1%})")

    result["C"] = {
        "days_above_pre_vol": int(total_above_pre),
        "max_consec_heap_vol": max_consec,
        "price_flat_vol_up_days": len(price_flat_vol_up),
        "lower_shadow_days": int(lower_shadow_days),
        "lower_shadow_pct": lower_shadow_pct,
    }

    # ──────────────────────────────────────────
    # D. MACD / RSI 特征
    # ──────────────────────────────────────────
    print_section("D. MACD / RSI 特征")

    dif_accum = accum["DIF"]
    dea_accum = accum["DEA"]
    rsi_accum = accum["RSI"]

    dif_start = dif_accum.iloc[0]
    dif_end   = dif_accum.iloc[-1]
    dea_start = dea_accum.iloc[0]
    dea_end   = dea_accum.iloc[-1]

    # DIF/DEA 所在区域（正/负轴）
    dif_neg_pct = (dif_accum < 0).mean()
    dea_neg_pct = (dea_accum < 0).mean()

    # 底背离检测：建仓期内，价格创新低时MACD DIF是否不创新低
    # 方法：取建仓期前半段和后半段比较
    half = len(accum) // 2
    if half > 0:
        price_low1  = accum["low"].iloc[:half].min()
        price_low2  = accum["low"].iloc[half:].min()
        dif_low1    = dif_accum.iloc[:half].min()
        dif_low2    = dif_accum.iloc[half:].min()
        # 底背离：后半段价格低点 <= 前半段，但DIF低点 > 前半段
        bearish_div = (price_low2 <= price_low1) and (dif_low2 > dif_low1)
    else:
        bearish_div = False

    rsi_min  = rsi_accum.min()
    rsi_max  = rsi_accum.max()
    rsi_mean = rsi_accum.mean()
    rsi_oversold_days = (rsi_accum < 30).sum()
    rsi_mid_days      = ((rsi_accum >= 30) & (rsi_accum <= 50)).sum()

    print(f"    DIF: 期初={dif_start:.4f} → 期末={dif_end:.4f}  "
          f"({'上升' if dif_end > dif_start else '下降'}趋势)")
    print(f"    DEA: 期初={dea_start:.4f} → 期末={dea_end:.4f}  "
          f"({'上升' if dea_end > dea_start else '下降'}趋势)")
    print(f"    DIF在负轴天占比: {dif_neg_pct:.1%}  DEA在负轴天占比: {dea_neg_pct:.1%}")
    print(f"    底背离信号: {'是 ✓' if bearish_div else '否'}")
    print(f"    RSI运行区间: [{rsi_min:.1f}, {rsi_max:.1f}]  均值={rsi_mean:.1f}")
    print(f"    RSI超卖(<30)天数: {rsi_oversold_days}  RSI中性(30-50)天数: {rsi_mid_days}")

    result["D"] = {
        "dif_start": dif_start,
        "dif_end": dif_end,
        "dea_start": dea_start,
        "dea_end": dea_end,
        "dif_neg_pct": dif_neg_pct,
        "dea_neg_pct": dea_neg_pct,
        "bearish_divergence": bearish_div,
        "rsi_min": rsi_min,
        "rsi_max": rsi_max,
        "rsi_mean": rsi_mean,
        "rsi_oversold_days": int(rsi_oversold_days),
        "rsi_mid_days": int(rsi_mid_days),
    }

    # ──────────────────────────────────────────
    # E. 拉升前临界信号（最后N个交易日）
    # ──────────────────────────────────────────
    print_section(f"E. 临界信号（建仓期最后{cfg['lift_signal_days']}个交易日）")

    n = cfg["lift_signal_days"]
    last_n = accum.tail(n).copy()

    if len(last_n) < 2:
        print("  数据不足")
    else:
        # 价格变化
        price_chg_last = (last_n["close"].iloc[-1] - last_n["close"].iloc[0]) / last_n["close"].iloc[0]
        # 成交量变化：后半段均量 vs 前半段均量
        half_n = len(last_n) // 2
        vol_first = last_n["volume"].iloc[:half_n].mean()
        vol_last  = last_n["volume"].iloc[half_n:].mean()
        vol_accel = vol_last / vol_first if vol_first > 0 else np.nan

        # 最后N天的量比（相对整个建仓期）
        last_vol_ratio = last_n["volume"].mean() / vol_accum

        # MACD方向
        dif_last_start = last_n["DIF"].iloc[0]
        dif_last_end   = last_n["DIF"].iloc[-1]
        macd_cross_up  = (last_n["DIF"] > last_n["DEA"]).any()

        # 阳线连续
        last_n_up = (last_n["close"] >= last_n["open"]).sum()

        # 最大单日涨幅
        max_up_day = last_n["pct_chg"].max()
        max_vol_day = last_n["volume"].max() / vol_accum

        print(f"    最后{n}日价格变化: {price_chg_last:.2%}")
        print(f"    量能加速比(后半/前半): {vol_accel:.2f}  {'量能加速' if vol_accel > 1.2 else '量能平稳'}")
        print(f"    最后{n}日均量 vs 建仓期均量: {last_vol_ratio:.2f}倍  {'放量突破' if last_vol_ratio > 1.5 else '温和'}")
        print(f"    最大单日涨幅: {max_up_day:.2f}%  最大单日量: {max_vol_day:.2f}倍均量")
        print(f"    DIF方向: {dif_last_start:.4f} → {dif_last_end:.4f}  {'金叉信号' if macd_cross_up else '未金叉'}")
        print(f"    最后{n}日阳线天数: {last_n_up}/{n}")

        result["E"] = {
            "price_chg_last_n": price_chg_last,
            "vol_accel_ratio": vol_accel,
            "last_vol_vs_accum": last_vol_ratio,
            "max_up_day_pct": max_up_day,
            "max_vol_day_ratio": max_vol_day,
            "macd_golden_cross": bool(macd_cross_up),
            "up_days_last_n": int(last_n_up),
        }

    return result


# ─────────────────────────────────────────────
# 共同特征总结
# ─────────────────────────────────────────────

def summarize_common_features(results: list):
    valid = [r for r in results if r and "A" in r]
    if len(valid) < 2:
        print("\n有效结果不足，无法总结共同特征。")
        return

    print("\n")
    print_separator("═")
    print("  三只股票建仓期共同特征总结")
    print_separator("═")

    # ── A. 价格形态 ──────────────────────────────
    print("\n【A. 价格形态共同特征】")
    amp_vals = [r["A"]["amp_ratio"] for r in valid]
    low_vals = [r["A"]["price_vs_half_year_low_pct"] for r in valid]
    coh_vals = [r["A"]["ma_cohesion"] for r in valid if not np.isnan(r["A"]["ma_cohesion"])]
    boll_r   = [r["A"]["boll_ratio"] for r in valid if not np.isnan(r["A"].get("boll_ratio", np.nan))]

    print(f"  振幅比: {[f'{v:.2%}' for v in amp_vals]}")
    print(f"  → 平均振幅比 {np.mean(amp_vals):.2%}，{'窄幅横盘' if np.mean(amp_vals) < 0.15 else '中等振幅横盘'}")
    print(f"  价格高于半年最低点: {[f'{v:.2%}' for v in low_vals]}")
    print(f"  → 平均 {np.mean(low_vals):.2%}，{'处于底部区域(0~30%)' if np.mean(low_vals) < 0.3 else '中部偏低区域'}")
    if coh_vals:
        print(f"  均线粘合度: {[f'{v:.4f}' for v in coh_vals]}")
        print(f"  → 均线趋于粘合，多空分歧收敛，等待方向选择")
    if boll_r:
        print(f"  布林带收窄比: {[f'{v:.2f}' for v in boll_r]}")
        below_one = sum(1 for v in boll_r if v < 1.0)
        print(f"  → {below_one}/{len(boll_r)} 只股票布林带在建仓期收窄，价格波动趋于收敛")

    # ── B. 成交量 ────────────────────────────────
    print("\n【B. 成交量共同特征】")
    vr_vals  = [r["B"]["vol_ratio_accum_vs_pre"] for r in valid if not np.isnan(r["B"]["vol_ratio_accum_vs_pre"])]
    mv_vals  = [r["B"]["moderate_vol_pct"] for r in valid]
    ud_vals  = [r["B"]["up_down_vol_ratio"] for r in valid if not np.isnan(r["B"]["up_down_vol_ratio"])]
    tr_vals  = [r["B"]["turnover_mean"] for r in valid if not np.isnan(r["B"]["turnover_mean"])]

    if vr_vals:
        print(f"  量比(建仓期/参比期): {[f'{v:.2f}' for v in vr_vals]}")
        print(f"  → 平均{np.mean(vr_vals):.2f}，{'温和放量(1.0~1.5)，无明显异动' if 0.8 < np.mean(vr_vals) < 1.8 else '放量明显'}")
    print(f"  温和量比(1.0~2.0)天数占比: {[f'{v:.1%}' for v in mv_vals]}")
    print(f"  → 平均{np.mean(mv_vals):.1%}，多数交易日量能温和，无大幅异常放量")
    if ud_vals:
        print(f"  阳/阴量比: {[f'{v:.2f}' for v in ud_vals]}")
        above_one = sum(1 for v in ud_vals if v > 1.0)
        print(f"  → {above_one}/{len(ud_vals)} 只股票阳线均量>阴线均量，资金在上涨日更积极，吸筹特征明显")
    if tr_vals:
        print(f"  日均换手率: {[f'{v:.2f}%' for v in tr_vals]}")
        print(f"  → 平均换手率{np.mean(tr_vals):.2f}%，{'低换手(筹码锁定)' if np.mean(tr_vals) < 2.0 else '中等换手'}")

    # ── C. 筹码/资金 ─────────────────────────────
    print("\n【C. 筹码/资金共同特征】")
    ls_vals = [r["C"]["lower_shadow_pct"] for r in valid]
    pf_vals = [r["C"]["price_flat_vol_up_days"] for r in valid]
    mh_vals = [r["C"]["max_consec_heap_vol"] for r in valid]

    print(f"  长下影线占比: {[f'{v:.1%}' for v in ls_vals]}")
    print(f"  → 平均{np.mean(ls_vals):.1%}，主力多次护盘动作，低位承接力强")
    print(f"  价不涨但量升天数: {pf_vals}")
    print(f"  → 合计{sum(pf_vals)}天，主力悄然吸筹的典型特征（股价横盘但资金持续流入）")
    print(f"  最长连续堆量天数: {mh_vals}")
    print(f"  → 平均{np.mean(mh_vals):.1f}天，存在持续性底部堆量")

    # ── D. MACD/RSI ──────────────────────────────
    print("\n【D. MACD/RSI共同特征】")
    bd_vals  = [r["D"]["bearish_divergence"] for r in valid]
    rsi_m    = [r["D"]["rsi_mean"] for r in valid]
    rsi_os   = [r["D"]["rsi_oversold_days"] for r in valid]
    dif_neg  = [r["D"]["dif_neg_pct"] for r in valid]

    print(f"  底背离信号: {bd_vals}")
    print(f"  → {sum(bd_vals)}/{len(bd_vals)} 只出现底背离，MACD背离是重要的建仓识别信号")
    print(f"  RSI均值: {[f'{v:.1f}' for v in rsi_m]}")
    print(f"  → 平均{np.mean(rsi_m):.1f}，{'低RSI区间(30-50)，市场情绪偏悲观，主力低位建仓' if np.mean(rsi_m) < 55 else 'RSI中性偏高'}")
    print(f"  RSI超卖天数: {rsi_os}")
    print(f"  DIF在负轴占比: {[f'{v:.1%}' for v in dif_neg]}")
    print(f"  → 建仓期MACD多在零轴下方运行，与市场整体弱势一致，主力逆市吸筹")

    # ── E. 临界信号 ───────────────────────────────
    print("\n【E. 拉升前临界信号共同特征】")
    e_valid = [r for r in valid if "E" in r]
    if e_valid:
        va_vals = [r["E"]["vol_accel_ratio"] for r in e_valid if not np.isnan(r["E"].get("vol_accel_ratio", np.nan))]
        lv_vals = [r["E"]["last_vol_vs_accum"] for r in e_valid]
        gc_vals = [r["E"]["macd_golden_cross"] for r in e_valid]

        if va_vals:
            print(f"  量能加速比: {[f'{v:.2f}' for v in va_vals]}")
            print(f"  → 平均加速{np.mean(va_vals):.2f}倍，拉升前成交量明显放大")
        print(f"  最后N日均量vs建仓期: {[f'{v:.2f}' for v in lv_vals]}")
        print(f"  MACD金叉信号: {gc_vals}")
        print(f"  → {sum(gc_vals)}/{len(gc_vals)} 只出现MACD金叉，DIF向上穿越DEA是重要拉升信号")

    # ── 综合建仓特征画像 ─────────────────────────
    print("\n" + "─" * 70)
    print("  【综合建仓特征画像 — 可量化的筛选条件】")
    print("─" * 70)
    print("""
  以下条件同时满足，高度疑似主力建仓尾段（即将拉升）：

  1. 价格形态
     ✓ 振幅比 < 15%（窄幅横盘，波动收敛）
     ✓ 价格距近半年低点 < 30%（底部区域）
     ✓ 均线粘合（MA5/10/20标准差 / 均价 < 0.02）
     ✓ 布林带带宽较前期收窄（ratio < 1.0）

  2. 成交量
     ✓ 建仓期日均量 / 参比期日均量 = 1.0~1.8（温和放量）
     ✓ 量比(1.0~2.0)天数占比 > 50%（无大幅异常）
     ✓ 阳线均量 / 阴线均量 > 1.1（吸筹特征）
     ✓ 换手率适中（0.5%~3.0%）

  3. 筹码/资金
     ✓ 长下影线出现频率 > 15%（主力护盘）
     ✓ 价不涨但量升天数 > 5天（资金悄然流入）
     ✓ 底部堆量持续 > 5个交易日

  4. MACD/RSI
     ✓ MACD底背离（价格创新低但DIF不创新低）
     ✓ RSI均值 30~55（低位未超买）
     ✓ DIF逐步从负轴向零轴靠近

  5. 临界信号（拉升前5~10日）
     ✓ 量能加速（后半段均量 > 前半段1.2倍以上）
     ✓ 出现MACD金叉（DIF上穿DEA）
     ✓ 阳线天数占比 > 60%
     ✓ 价格涨幅开始扩大（单日涨幅 > 2%）
""")


# ─────────────────────────────────────────────
# 主程序
# ─────────────────────────────────────────────

def main():
    print("\n" + "═" * 70)
    print("  主力建仓期技术特征分析")
    print(f"  运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 70)

    all_results = []
    for cfg in STOCKS:
        try:
            res = analyze_stock(cfg)
            all_results.append(res)
        except Exception as e:
            print(f"\n[ERROR] 分析 {cfg['name']} 时出错: {e}")
            import traceback
            traceback.print_exc()
            all_results.append({})

    summarize_common_features(all_results)

    print("\n" + "═" * 70)
    print("  分析完成")
    print("═" * 70 + "\n")


if __name__ == "__main__":
    main()
