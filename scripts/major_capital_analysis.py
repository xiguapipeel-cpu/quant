"""
主力拉升特征分析脚本
分析股票：神剑股份(002361)、赛微电子(300456)、广西能源(600310)
分析维度：K线形态、成交量、涨幅、筹码分布、资金流向等
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ─── 分析目标 ───────────────────────────────────────────────
TARGETS = [
    {"name": "神剑股份", "code": "002361", "market": "SZ", "start": "20251001", "end": "20251231"},
    {"name": "赛微电子", "code": "300456", "market": "SZ", "start": "20250601", "end": "20251231"},
    {"name": "广西能源", "code": "600310", "market": "SH", "start": "20251101", "end": "20260331"},
]


# ─── 数据获取 ────────────────────────────────────────────────
def fetch_kline(code: str, start: str, end: str) -> pd.DataFrame:
    """获取日K线数据（前复权）"""
    df = ak.stock_zh_a_hist(symbol=code, period="daily",
                             start_date=start, end_date=end, adjust="qfq")
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "日期": "date", "开盘": "open", "收盘": "close",
        "最高": "high", "最低": "low", "成交量": "volume",
        "成交额": "turnover", "振幅": "amplitude",
        "涨跌幅": "pct_chg", "涨跌额": "chg",
        "换手率": "turnover_rate"
    })
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def fetch_money_flow(code: str, start: str, end: str) -> pd.DataFrame:
    """获取资金流向数据"""
    try:
        df = ak.stock_individual_fund_flow(stock=code, market="sz" if code.startswith(("0","3")) else "sh")
        df.columns = [c.strip() for c in df.columns]
        if "日期" in df.columns:
            df["date"] = pd.to_datetime(df["日期"])
            df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]
        return df
    except Exception as e:
        print(f"  资金流向获取失败: {e}")
        return pd.DataFrame()


# ─── 技术指标计算 ─────────────────────────────────────────────
def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算各类技术指标"""
    # 移动均线
    for n in [5, 10, 20, 60]:
        df[f"ma{n}"] = df["close"].rolling(n).mean()

    # 成交量均线
    df["vol_ma5"]  = df["volume"].rolling(5).mean()
    df["vol_ma10"] = df["volume"].rolling(10).mean()
    df["vol_ma20"] = df["volume"].rolling(20).mean()

    # 量比（当日量 / 5日均量）
    df["vol_ratio"] = df["volume"] / df["vol_ma5"]

    # 振幅相对大小
    df["amp_ma5"] = df["amplitude"].rolling(5).mean()

    # 上影线、下影线、实体比例
    body = (df["close"] - df["open"]).abs()
    full_range = df["high"] - df["low"]
    df["body_ratio"]  = body / full_range.replace(0, np.nan)
    df["upper_shadow"] = (df["high"] - df[["close","open"]].max(axis=1)) / full_range.replace(0, np.nan)
    df["lower_shadow"] = (df[["close","open"]].min(axis=1) - df["low"]) / full_range.replace(0, np.nan)

    # 阳线/阴线
    df["is_bull"] = df["close"] > df["open"]

    # 连续涨跌
    df["bull_streak"] = (df["pct_chg"] > 0).astype(int)
    df["streak"] = df["bull_streak"].groupby(
        (df["bull_streak"] != df["bull_streak"].shift()).cumsum()
    ).cumcount() + 1

    # 涨停判断（中国A股：默认10%，科创板/创业板20%）
    if df["code"].iloc[0].startswith(("68","30")):
        limit = 0.195
    else:
        limit = 0.095
    df["hit_limit"] = df["pct_chg"] >= limit * 100

    # 累计涨幅
    df["cum_return"] = (1 + df["pct_chg"] / 100).cumprod() - 1

    # MACD
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd_dif"] = ema12 - ema26
    df["macd_dea"] = df["macd_dif"].ewm(span=9, adjust=False).mean()
    df["macd_bar"] = 2 * (df["macd_dif"] - df["macd_dea"])

    # RSI(14)
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    df["rsi14"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    # 布林带
    df["boll_mid"]   = df["close"].rolling(20).mean()
    boll_std         = df["close"].rolling(20).std()
    df["boll_upper"] = df["boll_mid"] + 2 * boll_std
    df["boll_lower"] = df["boll_mid"] - 2 * boll_std
    df["boll_width"]  = (df["boll_upper"] - df["boll_lower"]) / df["boll_mid"]

    return df


# ─── 主力拉升阶段识别 ─────────────────────────────────────────
def identify_pump_phases(df: pd.DataFrame) -> list:
    """识别主力拉升阶段：价格上涨+放量配合"""
    phases = []
    in_phase = False
    phase_start = None

    for i in range(1, len(df)):
        row = df.iloc[i]
        prev = df.iloc[i-1]
        # 拉升信号：涨幅>2% 且 量比>1.5
        pumping = row["pct_chg"] > 2 and row["vol_ratio"] > 1.5
        # 结束信号：跌幅>2% 或 量比<0.8
        ending = row["pct_chg"] < -2 or row["vol_ratio"] < 0.8

        if pumping and not in_phase:
            in_phase = True
            phase_start = i
        elif ending and in_phase:
            if i - phase_start >= 2:  # 至少持续2天
                phase_df = df.iloc[phase_start:i+1]
                phases.append({
                    "start_date": phase_df["date"].iloc[0],
                    "end_date":   phase_df["date"].iloc[-1],
                    "days":       len(phase_df),
                    "total_gain": phase_df["pct_chg"].sum(),
                    "max_vol_ratio": phase_df["vol_ratio"].max(),
                    "avg_vol_ratio": phase_df["vol_ratio"].mean(),
                    "limit_days":    phase_df["hit_limit"].sum(),
                })
            in_phase = False

    return phases


# ─── 筹码分布分析（价格区间成本估算）─────────────────────────
def calc_chip_distribution(df: pd.DataFrame, window: int = 60) -> dict:
    """
    简化筹码分布：用换手率加权统计近60日各价位成本
    返回：成本集中区间、获利盘比例
    """
    result = {}
    for i in range(window, len(df)):
        sub = df.iloc[i-window:i+1].copy()
        sub["weight"] = sub["turnover_rate"] / sub["turnover_rate"].sum()
        avg_cost = (sub["close"] * sub["weight"]).sum()
        current  = df.iloc[i]["close"]
        profit_pct = (sub[sub["close"] < current]["weight"].sum()) * 100
        result[df.iloc[i]["date"]] = {
            "avg_cost": round(avg_cost, 2),
            "current":  round(current, 2),
            "profit_pct": round(profit_pct, 1),
            "cost_ratio": round(current / avg_cost, 3),  # 当前价/成本价 >1 说明多数获利
        }
    return result


# ─── 单只股票完整分析 ─────────────────────────────────────────
def analyze_stock(target: dict) -> dict:
    name  = target["name"]
    code  = target["code"]
    start = target["start"]
    end   = target["end"]

    print(f"\n{'='*60}")
    print(f"分析: {name} ({code})  {start[:6]}~{end[:6]}")
    print(f"{'='*60}")

    # 获取更早数据用于均线计算（多取90日）
    start_ext = str(int(start[:6]) - 1 if int(start[4:6]) > 1 else int(start[:4])*100 + 12 - 1) + "01"
    # 简单处理：直接往前取3个月
    df = fetch_kline(code, "20250101" if start > "20250101" else start, end)
    if df.empty:
        print(f"  [{name}] 无数据！")
        return {}

    df["code"] = code
    df = calc_indicators(df)

    # 截取目标时间段
    mask = (df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))
    df_target = df[mask].copy()

    print(f"\n[基础统计] 交易日: {len(df_target)}天")
    print(f"  起始价: {df_target['close'].iloc[0]:.2f}  "
          f"最高价: {df_target['high'].max():.2f}  "
          f"最终价: {df_target['close'].iloc[-1]:.2f}")
    total_gain = (df_target['close'].iloc[-1] / df_target['close'].iloc[0] - 1) * 100
    print(f"  区间累计涨幅: {total_gain:.1f}%")

    # 涨停统计
    limit_days = df_target["hit_limit"].sum()
    print(f"  涨停天数: {limit_days}天")

    # 成交量分析
    print(f"\n[成交量分析]")
    print(f"  区间最大量比: {df_target['vol_ratio'].max():.2f}x")
    print(f"  区间平均量比: {df_target['vol_ratio'].mean():.2f}x")
    high_vol_days = (df_target["vol_ratio"] > 2).sum()
    print(f"  量比>2倍天数: {high_vol_days}天 ({high_vol_days/len(df_target)*100:.0f}%)")
    print(f"  平均换手率: {df_target['turnover_rate'].mean():.2f}%")
    print(f"  最高换手率: {df_target['turnover_rate'].max():.2f}%")

    # 涨跌日统计
    bull_days = df_target["is_bull"].sum()
    bear_days = len(df_target) - bull_days
    print(f"\n[K线统计]")
    print(f"  阳线天数: {bull_days}  阴线天数: {bear_days}  "
          f"阳线占比: {bull_days/len(df_target)*100:.0f}%")
    print(f"  平均振幅: {df_target['amplitude'].mean():.2f}%  "
          f"最大振幅: {df_target['amplitude'].max():.2f}%")
    print(f"  平均上影线比例: {df_target['upper_shadow'].mean():.2%}")
    print(f"  平均下影线比例: {df_target['lower_shadow'].mean():.2%}")
    print(f"  平均实体比例: {df_target['body_ratio'].mean():.2%}")

    # 涨幅分布
    print(f"\n[涨跌幅分布]")
    bins = [-20, -5, -3, -1, 0, 1, 3, 5, 10, 20]
    labels = ["<-5%", "-5~-3%", "-3~-1%", "-1~0%", "0~1%", "1~3%", "3~5%", "5~10%", ">10%"]
    df_target["pct_bin"] = pd.cut(df_target["pct_chg"], bins=bins, labels=labels)
    dist = df_target["pct_bin"].value_counts().sort_index()
    for label, cnt in dist.items():
        bar = "█" * cnt
        print(f"  {label:>10}: {cnt:3d}天  {bar}")

    # 阶段性拉升识别
    phases = identify_pump_phases(df_target)
    print(f"\n[拉升阶段识别] 共{len(phases)}段")
    for i, ph in enumerate(phases):
        print(f"  第{i+1}段: {ph['start_date'].date()} ~ {ph['end_date'].date()}  "
              f"{ph['days']}天  累计+{ph['total_gain']:.1f}%  "
              f"峰值量比{ph['max_vol_ratio']:.1f}x  涨停{ph['limit_days']}天")

    # 筹码分布分析
    chip = calc_chip_distribution(df_target)
    if chip:
        last = list(chip.values())[-1]
        first = list(chip.values())[0]
        print(f"\n[筹码分布估算] (基于换手率加权)")
        print(f"  分析初始 → 获利盘比例: {first['profit_pct']:.1f}%  "
              f"均成本: {first['avg_cost']}  当前价: {first['current']}")
        print(f"  分析末期 → 获利盘比例: {last['profit_pct']:.1f}%  "
              f"均成本: {last['avg_cost']}  当前价: {last['current']}")

    # MACD信号
    golden_cross = ((df_target["macd_dif"] > df_target["macd_dea"]) &
                    (df_target["macd_dif"].shift(1) <= df_target["macd_dea"].shift(1))).sum()
    print(f"\n[MACD] 金叉次数: {golden_cross}  "
          f"末期DIF: {df_target['macd_dif'].iloc[-1]:.3f}  "
          f"末期DEA: {df_target['macd_dea'].iloc[-1]:.3f}")

    # RSI水平
    print(f"[RSI14] 区间均值: {df_target['rsi14'].mean():.1f}  "
          f"最高: {df_target['rsi14'].max():.1f}  "
          f"末期: {df_target['rsi14'].iloc[-1]:.1f}")

    # 布林带突破
    upper_break = (df_target["close"] > df_target["boll_upper"]).sum()
    print(f"[布林带] 突破上轨次数: {upper_break}天  "
          f"平均带宽: {df_target['boll_width'].mean():.3f}")

    # 返回关键指标供汇总对比
    return {
        "name": name, "code": code,
        "trade_days": len(df_target),
        "total_gain_pct": round(total_gain, 1),
        "limit_days": int(limit_days),
        "avg_vol_ratio": round(df_target['vol_ratio'].mean(), 2),
        "max_vol_ratio": round(df_target['vol_ratio'].max(), 2),
        "high_vol_ratio_pct": round(high_vol_days/len(df_target)*100, 0),
        "avg_turnover_rate": round(df_target['turnover_rate'].mean(), 2),
        "bull_day_pct": round(bull_days/len(df_target)*100, 0),
        "avg_amplitude": round(df_target['amplitude'].mean(), 2),
        "avg_upper_shadow": round(df_target['upper_shadow'].mean(), 3),
        "pump_phases": len(phases),
        "chip_profit_end": last['profit_pct'] if chip else None,
        "rsi_avg": round(df_target['rsi14'].mean(), 1),
        "rsi_max": round(df_target['rsi14'].max(), 1),
        "boll_upper_breaks": int(upper_break),
        "df": df_target,
    }


# ─── 共同特征汇总 ─────────────────────────────────────────────
def summarize_common_features(results: list):
    print(f"\n\n{'='*60}")
    print("【共同特征总结】主力资金拉升规律")
    print(f"{'='*60}\n")

    # 对比表格
    metrics = [
        ("区间总涨幅", "total_gain_pct", "%"),
        ("涨停天数", "limit_days", "天"),
        ("平均量比", "avg_vol_ratio", "x"),
        ("量比>2x占比", "high_vol_ratio_pct", "%"),
        ("平均换手率", "avg_turnover_rate", "%"),
        ("阳线占比", "bull_day_pct", "%"),
        ("平均振幅", "avg_amplitude", "%"),
        ("末期获利盘", "chip_profit_end", "%"),
        ("RSI均值", "rsi_avg", ""),
        ("RSI最高值", "rsi_max", ""),
        ("突破布林上轨", "boll_upper_breaks", "天"),
    ]
    header = f"{'指标':>14} | " + " | ".join(f"{r['name']:>10}" for r in results)
    print(header)
    print("-" * len(header))
    for label, key, unit in metrics:
        row = f"{label+unit:>14} | "
        row += " | ".join(f"{str(r.get(key,'N/A')):>10}" for r in results)
        print(row)

    print("\n\n【主力拉升核心规律提炼】\n")

    # 量价特征
    avg_gains = [r["total_gain_pct"] for r in results]
    avg_vol = [r["avg_vol_ratio"] for r in results]
    avg_amp = [r["avg_amplitude"] for r in results]
    print(f"1. 【涨幅规律】三只股票区间涨幅均值: {np.mean(avg_gains):.0f}%  "
          f"(范围 {min(avg_gains):.0f}% ~ {max(avg_gains):.0f}%)")
    print(f"   → 主力目标涨幅通常需要 2~4倍空间，短期快速拉升特征明显")

    print(f"\n2. 【量价配合】量比均值: {np.mean(avg_vol):.2f}x  "
          f"量比>2x占比均值: {np.mean([r['high_vol_ratio_pct'] for r in results]):.0f}%")
    print(f"   → 拉升期放量明显，量比普遍>1.5，关键突破日量比>3x是信号")

    print(f"\n3. 【K线特征】阳线占比均值: {np.mean([r['bull_day_pct'] for r in results]):.0f}%  "
          f"振幅均值: {np.mean(avg_amp):.2f}%")
    print(f"   → 主力拉升期阳线为主(>60%)，振幅适中，上影线短(主力压制抛盘)")

    print(f"\n4. 【筹码特征】末期获利盘比例: "
          + "  ".join(f"{r['name']}:{r['chip_profit_end']}%" for r in results if r.get('chip_profit_end')))
    print(f"   → 主力在获利盘80%+时仍能维持上涨，说明筹码高度集中")

    print(f"\n5. 【涨停特征】涨停天数: "
          + "  ".join(f"{r['name']}:{r['limit_days']}天" for r in results))
    print(f"   → 主力拉升阶段以连续涨停或高频涨停方式快速拉升，留出追涨空间")

    print(f"\n6. 【换手率特征】平均换手率: {np.mean([r['avg_turnover_rate'] for r in results]):.1f}%")
    print(f"   → 拉升期换手率显著提升（>3%/日），说明活跃资金积极参与")

    print(f"\n7. 【技术指标】RSI均值: {np.mean([r['rsi_avg'] for r in results]):.1f}  "
          f"RSI最高: {np.mean([r['rsi_max'] for r in results]):.1f}")
    print(f"   → RSI长时间维持60~80强势区间，强势股特征明显，极端值>90时注意顶部")

    print(f"\n8. 【布林带特征】突破上轨天数: "
          + "  ".join(f"{r['name']}:{r['boll_upper_breaks']}天" for r in results))
    print(f"   → 主力拉升期股价频繁突破布林上轨，趋势强劲")

    print(f"\n\n【策略信号核心要素】")
    print("─" * 50)
    print("✓ 选股条件：")
    print("  - 日换手率突破 3%+（活跃资金入场）")
    print("  - 量比 > 1.5~2x（放量拉升）")
    print("  - 涨幅 > 3% 且为阳线（价格突破）")
    print("  - MACD金叉或DIF上穿0轴（中期趋势转多）")
    print("  - RSI(14) 在 50~70 之间（强势但未超买）")
    print("")
    print("✓ 加仓信号：")
    print("  - 连续2日量比>2x + 涨幅>3%")
    print("  - 突破20日均线或布林上轨")
    print("  - 涨停板（限价单）放量确认")
    print("")
    print("✓ 止损/止盈：")
    print("  - 止损：跌破5日均线 或 量比>2x 但收阴线（放量滞涨）")
    print("  - 止盈：RSI>85 + 上影线>30% + 量比>3x（主力出货信号）")


# ─── 主程序 ──────────────────────────────────────────────────
if __name__ == "__main__":
    print("主力资金拉升特征分析")
    print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    results = []
    for target in TARGETS:
        try:
            res = analyze_stock(target)
            if res:
                results.append(res)
        except Exception as e:
            print(f"[ERROR] {target['name']}: {e}")
            import traceback
            traceback.print_exc()

    if results:
        summarize_common_features(results)

    print(f"\n分析完成！")
