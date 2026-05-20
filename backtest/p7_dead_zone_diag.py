"""
P7：Dead Zone 诊断 — 找出 2023-07~2025-03 共 21 个月 0 trades 的真正元凶

分层过滤通过率：
  L1 大盘过滤 : idx_sh MA20 > MA60                    （每日）
  L2 个股趋势 : stock MA20 > MA60                     （每日每股）
  L3 RSI≥55  : rsi >= 55
  L4 突破 30d 高 : close > max(high[-1..-30])

输出每日通过率 + 串联条件概率 + 同样诊断对照组（活跃区 2025-01~2026-03）。
"""
import asyncio
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.mysql_pool import get_pool, close_pool


async def fetch_idx_sh(start, end):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT trade_date, close FROM stock_daily
                WHERE code='idx_sh' AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date
            """, (start, end))
            rows = await cur.fetchall()
    df = pd.DataFrame(rows, columns=['trade_date', 'close'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df['close'] = df['close'].astype(float)
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    df['pass_L1'] = df['ma20'] > df['ma60']
    return df


async def fetch_stocks(start, end):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            cutoff = (pd.to_datetime(start) - pd.Timedelta(days=90)).date()
            await cur.execute("""
                SELECT sd.trade_date, sd.code, sd.close, sd.high
                FROM stock_daily sd
                JOIN stock_basic sb ON sb.code = sd.code
                WHERE sd.trade_date BETWEEN %s AND %s
                  AND sd.code != 'idx_sh'
                  AND sb.list_date IS NOT NULL
                  AND sb.list_date <= %s
                ORDER BY sd.code, sd.trade_date
            """, (start, end, cutoff))
            rows = await cur.fetchall()
    df = pd.DataFrame(rows, columns=['trade_date', 'code', 'close', 'high'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    return df


def compute_indicators(df):
    """逐股票计算 MA20/MA60/RSI/30 日 high（前一天起，不含今日）"""
    df = df.sort_values(['code', 'trade_date']).reset_index(drop=True)
    df['ma20'] = df.groupby('code')['close'].transform(lambda s: s.rolling(20).mean())
    df['ma60'] = df.groupby('code')['close'].transform(lambda s: s.rolling(60).mean())

    # RSI 14（简化版：14 日移动平均 of gain/loss）
    df['_delta'] = df.groupby('code')['close'].diff()
    df['_gain'] = df['_delta'].where(df['_delta'] > 0, 0.0)
    df['_loss'] = (-df['_delta']).where(df['_delta'] < 0, 0.0)
    df['_avg_gain'] = df.groupby('code')['_gain'].transform(lambda s: s.rolling(14).mean())
    df['_avg_loss'] = df.groupby('code')['_loss'].transform(lambda s: s.rolling(14).mean())
    df['rsi'] = 100 - 100 / (1 + df['_avg_gain'] / df['_avg_loss'].replace(0, 1e-9))

    # 30 日 high（前一天起，不含今日）
    df['high30_prev'] = df.groupby('code')['high'].transform(
        lambda s: s.shift(1).rolling(30).max())

    return df.drop(columns=['_delta', '_gain', '_loss', '_avg_gain', '_avg_loss'])


def evaluate_and_report(label, df, idx_df, start, end):
    df = df[(df['trade_date'] >= start) & (df['trade_date'] <= end)]
    idx_df = idx_df[(idx_df['trade_date'] >= start) & (idx_df['trade_date'] <= end)]

    total_days = idx_df['trade_date'].nunique()
    l1_pass = idx_df['pass_L1'].fillna(False).sum()
    print(f"\n{'═'*70}")
    print(f"  {label}  ({start} ~ {end})")
    print(f"{'═'*70}")
    print(f"交易日: {total_days}")
    print(f"\n【L1 大盘 idx_sh MA20 > MA60】")
    print(f"  通过 {l1_pass}/{total_days} = {l1_pass/total_days*100:.1f}%")

    # 合并 L1 标记到个股
    df = df.merge(idx_df[['trade_date', 'pass_L1']], on='trade_date', how='left')
    df['pass_L2'] = df['ma20'] > df['ma60']
    df['pass_L3'] = df['rsi'] >= 55
    df['pass_L4'] = df['close'] > df['high30_prev']

    # 独立通过率
    print(f"\n【独立通过率（不考虑串联）】")
    print(f"  L1 (idx_sh MA20>MA60) : {l1_pass/total_days*100:5.1f}% (交易日)")
    n_l2_total = df['pass_L2'].notna().sum()
    n_l2_pass = df['pass_L2'].fillna(False).sum()
    print(f"  L2 (stock MA20>MA60)  : {n_l2_pass/n_l2_total*100:5.1f}% ({n_l2_pass:,}/{n_l2_total:,})")
    n_l3_total = df['pass_L3'].notna().sum()
    n_l3_pass = df['pass_L3'].fillna(False).sum()
    print(f"  L3 (rsi ≥ 55)         : {n_l3_pass/n_l3_total*100:5.1f}% ({n_l3_pass:,}/{n_l3_total:,})")
    n_l4_total = df['pass_L4'].notna().sum()
    n_l4_pass = df['pass_L4'].fillna(False).sum()
    print(f"  L4 (close > 30d high) : {n_l4_pass/n_l4_total*100:5.1f}% ({n_l4_pass:,}/{n_l4_total:,})")

    # 链式通过率（按策略实际过滤顺序）
    print(f"\n【链式通过率（按 L1→L2→L3→L4 串联）】")
    s1 = df[df['pass_L1'].fillna(False)]
    n_s1 = len(s1)
    print(f"  L1 后剩 (股票,日)              : {n_s1:,}")
    if n_s1:
        s2 = s1[s1['pass_L2'].fillna(False)]
        n_s2 = len(s2)
        print(f"  + L2 (stock MA)                : {n_s2:,}  通过率 {n_s2/n_s1*100:.1f}%")
        if n_s2:
            s3 = s2[s2['pass_L3'].fillna(False)]
            n_s3 = len(s3)
            print(f"  + L3 (rsi≥55)                  : {n_s3:,}  通过率 {n_s3/n_s2*100:.1f}%")
            if n_s3:
                s4 = s3[s3['pass_L4'].fillna(False)]
                n_s4 = len(s4)
                print(f"  + L4 (close>30d high)          : {n_s4:,}  通过率 {n_s4/n_s3*100:.1f}%")


async def main():
    print("[1/2] DEAD ZONE 诊断 (2023-07 ~ 2025-03)")
    print("加载 idx_sh + 个股数据...")
    idx_df = await fetch_idx_sh('2023-01-01', '2025-03-31')
    print(f"  idx_sh {len(idx_df)} 行")
    df = await fetch_stocks('2023-01-01', '2025-03-31')
    print(f"  stocks {len(df):,} 行 ({df['code'].nunique()} 只)")
    print("计算指标...")
    df = compute_indicators(df)
    evaluate_and_report('DEAD ZONE (Fold 1-6)', df, idx_df, '2023-07-01', '2025-03-31')

    print("\n\n[2/2] 对照：活跃区 (2025-01 ~ 2026-03)")
    print("加载 idx_sh + 个股数据...")
    idx_df2 = await fetch_idx_sh('2024-07-01', '2026-03-31')
    df2 = await fetch_stocks('2024-07-01', '2026-03-31')
    print(f"  stocks {len(df2):,} 行")
    df2 = compute_indicators(df2)
    evaluate_and_report('ACTIVE ZONE (Fold 7-10)', df2, idx_df2, '2025-01-01', '2026-03-31')

    await close_pool()


if __name__ == '__main__':
    asyncio.run(main())
