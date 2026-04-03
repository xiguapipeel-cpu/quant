#!/usr/bin/env python3
"""
冠农股份 (600251) 技术面分析脚本
获取最近120个交易日日线数据（前复权），计算各项技术指标
"""

import warnings
warnings.filterwarnings('ignore')

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# 1. 拉取数据
# ─────────────────────────────────────────────
print("=" * 65)
print("  冠农股份 (600251) 技术面分析报告")
print(f"  数据获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 65)

print("\n[1] 正在获取 600251 日线数据（前复权）...")

end_date = datetime.now().strftime('%Y%m%d')
# 取180天前确保能拿到120个交易日
start_date = (datetime.now() - timedelta(days=250)).strftime('%Y%m%d')

df = ak.stock_zh_a_hist(
    symbol="600251",
    period="daily",
    start_date=start_date,
    end_date=end_date,
    adjust="qfq"   # 前复权
)

df.columns = [c.strip() for c in df.columns]
df = df.rename(columns={
    '日期': 'date',
    '开盘': 'open',
    '收盘': 'close',
    '最高': 'high',
    '最低': 'low',
    '成交量': 'volume',
    '成交额': 'amount',
    '振幅': 'amplitude',
    '涨跌幅': 'pct_chg',
    '涨跌额': 'price_chg',
    '换手率': 'turnover'
})
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values('date').reset_index(drop=True)

# 只保留最近120个交易日
df = df.tail(120).reset_index(drop=True)
print(f"   数据期间: {df['date'].iloc[0].date()} ~ {df['date'].iloc[-1].date()}")
print(f"   共 {len(df)} 个交易日")

# ─────────────────────────────────────────────
# 2. 计算技术指标
# ─────────────────────────────────────────────

# --- 均线 ---
for n in [5, 10, 20, 60]:
    df[f'ma{n}'] = df['close'].rolling(n).mean()

# --- MACD ---
ema12 = df['close'].ewm(span=12, adjust=False).mean()
ema26 = df['close'].ewm(span=26, adjust=False).mean()
df['dif'] = ema12 - ema26
df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
df['macd_bar'] = (df['dif'] - df['dea']) * 2

# --- RSI(14) ---
delta = df['close'].diff()
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)
avg_gain = gain.ewm(com=13, adjust=False).mean()
avg_loss = loss.ewm(com=13, adjust=False).mean()
rs = avg_gain / avg_loss.replace(0, np.nan)
df['rsi14'] = 100 - (100 / (1 + rs))

# --- 布林带 (20, 2) ---
df['boll_mid'] = df['close'].rolling(20).mean()
boll_std = df['close'].rolling(20).std()
df['boll_upper'] = df['boll_mid'] + 2 * boll_std
df['boll_lower'] = df['boll_mid'] - 2 * boll_std

# --- 量比：最近5日均量 vs 前20日均量 ---
df['vol_ma5']  = df['volume'].rolling(5).mean()
df['vol_ma20'] = df['volume'].rolling(20).mean()

# --- 近5日涨跌幅 (累计) ---
df['pct5d'] = df['close'].pct_change(5) * 100

# ─────────────────────────────────────────────
# 3. 输出最近5个交易日数据
# ─────────────────────────────────────────────
last5 = df.tail(5).copy()

print("\n" + "=" * 65)
print("  【基础行情 & 均线】最近5个交易日")
print("=" * 65)

header = f"{'日期':^12} {'收盘':>7} {'成交量(万手)':>12} {'成交额(亿)':>10} {'MA5':>7} {'MA10':>7} {'MA20':>7} {'MA60':>7}"
print(header)
print("-" * 70)
for _, row in last5.iterrows():
    print(
        f"{str(row['date'].date()):^12} "
        f"{row['close']:>7.2f} "
        f"{row['volume']/10000:>12.2f} "
        f"{row['amount']/1e8:>10.4f} "
        f"{row['ma5']:>7.2f} "
        f"{row['ma10']:>7.2f} "
        f"{row['ma20']:>7.2f} "
        f"{row['ma60']:>7.2f}"
    )

print("\n" + "=" * 65)
print("  【MACD】最近5个交易日")
print("=" * 65)
print(f"{'日期':^12} {'DIF':>9} {'DEA':>9} {'MACD柱':>9}")
print("-" * 44)
for _, row in last5.iterrows():
    bar_sign = "▲" if row['macd_bar'] > 0 else "▼"
    print(
        f"{str(row['date'].date()):^12} "
        f"{row['dif']:>9.4f} "
        f"{row['dea']:>9.4f} "
        f"{row['macd_bar']:>8.4f}{bar_sign}"
    )

print("\n" + "=" * 65)
print("  【RSI(14) & 布林带】最近5个交易日")
print("=" * 65)
print(f"{'日期':^12} {'RSI14':>7} {'上轨':>8} {'中轨':>8} {'下轨':>8} {'带宽%':>7}")
print("-" * 58)
for _, row in last5.iterrows():
    bw = (row['boll_upper'] - row['boll_lower']) / row['boll_mid'] * 100
    print(
        f"{str(row['date'].date()):^12} "
        f"{row['rsi14']:>7.2f} "
        f"{row['boll_upper']:>8.3f} "
        f"{row['boll_mid']:>8.3f} "
        f"{row['boll_lower']:>8.3f} "
        f"{bw:>7.2f}"
    )

print("\n" + "=" * 65)
print("  【量比 & 涨跌幅】最近5个交易日")
print("=" * 65)
print(f"{'日期':^12} {'量比(5/20)':>11} {'日涨跌%':>9} {'5日累涨跌%':>11}")
print("-" * 48)
for _, row in last5.iterrows():
    vol_ratio = row['vol_ma5'] / row['vol_ma20'] if row['vol_ma20'] > 0 else float('nan')
    print(
        f"{str(row['date'].date()):^12} "
        f"{vol_ratio:>11.3f} "
        f"{row['pct_chg']:>+9.2f} "
        f"{row['pct5d']:>+11.2f}"
    )

# ─────────────────────────────────────────────
# 4. K线形态描述（最近5日）
# ─────────────────────────────────────────────
print("\n" + "=" * 65)
print("  【K线形态】最近5个交易日")
print("=" * 65)
print(f"{'日期':^12} {'形态':^6} {'实体幅%':>8} {'上影幅%':>8} {'下影幅%':>8} {'描述'}")
print("-" * 75)
for _, row in last5.iterrows():
    o, c, h, l = row['open'], row['close'], row['high'], row['low']
    total_range = h - l if h != l else 0.001
    body = abs(c - o)
    body_pct = body / total_range * 100

    upper_shadow = h - max(o, c)
    lower_shadow = min(o, c) - l
    upper_pct = upper_shadow / total_range * 100
    lower_pct = lower_shadow / total_range * 100

    candle_type = "阳线" if c >= o else "阴线"

    # 形态描述
    if body_pct >= 70:
        shape = "大实体" + candle_type
    elif body_pct >= 40:
        shape = "中实体" + candle_type
    elif body_pct >= 15:
        if upper_pct > 30:
            shape = "上影线" + candle_type
        elif lower_pct > 30:
            shape = "下影线" + candle_type
        else:
            shape = "小实体" + candle_type
    else:
        if upper_pct > 35 and lower_pct > 35:
            shape = "十字星"
        elif upper_pct > 35:
            shape = "墓碑线"
        elif lower_pct > 35:
            shape = "蜻蜓线"
        else:
            shape = "纺锤线"

    print(
        f"{str(row['date'].date()):^12} "
        f"{candle_type:^6} "
        f"{body_pct:>8.1f} "
        f"{upper_pct:>8.1f} "
        f"{lower_pct:>8.1f}  "
        f"{shape}"
    )

# ─────────────────────────────────────────────
# 5. 综合判断
# ─────────────────────────────────────────────
latest = df.iloc[-1]
prev   = df.iloc[-2]

print("\n" + "=" * 65)
print("  【综合技术判断】（基于最新收盘日）")
print("=" * 65)

close = latest['close']
print(f"\n  最新收盘价: {close:.2f}")

# --- 价格 vs 均线 ---
print("\n  ▶ 价格相对均线位置:")
for n, label in [(5, 'MA5'), (10, 'MA10'), (20, 'MA20'), (60, 'MA60')]:
    ma_val = latest[f'ma{n}']
    diff_pct = (close - ma_val) / ma_val * 100
    pos = "上方 ↑" if close > ma_val else "下方 ↓"
    print(f"     {label}: {ma_val:.2f}  →  价格在{pos}  ({diff_pct:+.2f}%)")

# 均线多头/空头排列
ma5  = latest['ma5']
ma10 = latest['ma10']
ma20 = latest['ma20']
ma60 = latest['ma60']
if ma5 > ma10 > ma20 > ma60:
    align = "多头排列 (看涨)"
elif ma5 < ma10 < ma20 < ma60:
    align = "空头排列 (看跌)"
else:
    align = "混乱排列 (震荡)"
print(f"     均线排列: {align}")

# --- MACD 判断 ---
print("\n  ▶ MACD 趋势状态:")
dif = latest['dif']
dea = latest['dea']
bar = latest['macd_bar']
prev_bar = prev['macd_bar']

if dif > 0 and dea > 0:
    zone = "零轴上方 (强势区)"
elif dif < 0 and dea < 0:
    zone = "零轴下方 (弱势区)"
else:
    zone = "零轴附近 (中性)"

if dif > dea:
    cross_state = "DIF > DEA (金叉区域)"
else:
    cross_state = "DIF < DEA (死叉区域)"

if bar > 0 and bar > prev_bar:
    bar_trend = "MACD柱扩大 (多头动能增强)"
elif bar > 0 and bar < prev_bar:
    bar_trend = "MACD柱缩小 (多头动能减弱)"
elif bar < 0 and abs(bar) > abs(prev_bar):
    bar_trend = "MACD柱扩大 (空头动能增强)"
else:
    bar_trend = "MACD柱缩小 (空头动能减弱)"

print(f"     DIF={dif:.4f}  DEA={dea:.4f}  MACD柱={bar:.4f}")
print(f"     位置: {zone}")
print(f"     状态: {cross_state}")
print(f"     趋势: {bar_trend}")

# --- RSI 判断 ---
print("\n  ▶ RSI(14) 位置:")
rsi = latest['rsi14']
if rsi >= 80:
    rsi_desc = "严重超买 (高风险)"
elif rsi >= 70:
    rsi_desc = "超买区间 (注意回调)"
elif rsi >= 60:
    rsi_desc = "偏强区间"
elif rsi >= 50:
    rsi_desc = "中性偏强"
elif rsi >= 40:
    rsi_desc = "中性偏弱"
elif rsi >= 30:
    rsi_desc = "偏弱区间"
else:
    rsi_desc = "超卖区间 (关注反弹)"
print(f"     RSI(14)={rsi:.2f}  →  {rsi_desc}")

# --- 成交量趋势 ---
print("\n  ▶ 成交量趋势:")
vol_ratio = latest['vol_ma5'] / latest['vol_ma20']
print(f"     最近5日均量: {latest['vol_ma5']/10000:.2f} 万手")
print(f"     前20日均量:  {latest['vol_ma20']/10000:.2f} 万手")
print(f"     量比(5/20):  {vol_ratio:.3f}", end="  →  ")
if vol_ratio >= 1.5:
    print("明显放量 (活跃)")
elif vol_ratio >= 1.1:
    print("温和放量")
elif vol_ratio >= 0.9:
    print("量能持平")
elif vol_ratio >= 0.6:
    print("温和缩量")
else:
    print("明显缩量 (萎靡)")

# --- 布林带 判断 ---
print("\n  ▶ 布林带位置:")
bu = latest['boll_upper']
bm = latest['boll_mid']
bl = latest['boll_lower']
bw_pct = (bu - bl) / bm * 100
if close > bu:
    boll_pos = f"价格突破上轨 ({bu:.2f}) (超买警示)"
elif close > bm:
    boll_pos = f"价格在中轨 ({bm:.2f}) 与上轨 ({bu:.2f}) 之间 (偏强)"
elif close > bl:
    boll_pos = f"价格在下轨 ({bl:.2f}) 与中轨 ({bm:.2f}) 之间 (偏弱)"
else:
    boll_pos = f"价格跌破下轨 ({bl:.2f}) (超卖警示)"
print(f"     上轨={bu:.2f}  中轨={bm:.2f}  下轨={bl:.2f}")
print(f"     布林带宽={bw_pct:.2f}%  →  {'收窄(蓄势)' if bw_pct < 10 else '扩张(波动加大)' if bw_pct > 20 else '正常宽度'}")
print(f"     位置: {boll_pos}")

# --- 关键支撑/压力位 ---
print("\n  ▶ 关键支撑 & 压力位:")
# 近60日高低点
hi60 = df.tail(60)['high'].max()
lo60 = df.tail(60)['low'].min()
hi20 = df.tail(20)['high'].max()
lo20 = df.tail(20)['low'].min()
print(f"     近60日高点: {hi60:.2f}  (压力)")
print(f"     近20日高点: {hi20:.2f}  (压力)")
print(f"     MA20: {ma20:.2f}")
print(f"     MA60: {ma60:.2f}")
print(f"     近20日低点: {lo20:.2f}  (支撑)")
print(f"     近60日低点: {lo60:.2f}  (支撑)")

# ─────────────────────────────────────────────
# 6. 综合结论
# ─────────────────────────────────────────────
print("\n" + "=" * 65)
print("  【综合结论】")
print("=" * 65)

# 多空得分简单评估
score = 0
score_details = []

# 均线
if close > ma5: score += 1; score_details.append("+1 价格>MA5")
if close > ma10: score += 1; score_details.append("+1 价格>MA10")
if close > ma20: score += 1; score_details.append("+1 价格>MA20")
if close > ma60: score += 1; score_details.append("+1 价格>MA60")
if ma5 > ma10 > ma20: score += 1; score_details.append("+1 短期均线多头排列")

# MACD
if dif > dea: score += 1; score_details.append("+1 MACD金叉")
if dif > 0: score += 1; score_details.append("+1 DIF>0")
if bar > prev_bar and bar > 0: score += 1; score_details.append("+1 MACD柱扩张")

# RSI
if 50 < rsi < 70: score += 1; score_details.append("+1 RSI健康强势区")
elif rsi > 70: score -= 1; score_details.append("-1 RSI超买")
elif rsi < 30: score -= 1; score_details.append("-1 RSI超卖")

# 量价
if vol_ratio >= 1.1 and close > prev['close']: score += 1; score_details.append("+1 量价配合上涨")

for d in score_details:
    print(f"     {d}")

print(f"\n  综合得分: {score} / 10")
if score >= 7:
    verdict = "技术面偏强，多头占优，可关注做多机会"
elif score >= 5:
    verdict = "技术面中性偏强，震荡偏多，需结合基本面判断"
elif score >= 3:
    verdict = "技术面中性偏弱，震荡格局，建议观望"
else:
    verdict = "技术面偏弱，空头占优，注意控制风险"

print(f"  结论: {verdict}")
print("\n" + "=" * 65)
print("  * 本报告仅供技术分析参考，不构成投资建议")
print("=" * 65)
