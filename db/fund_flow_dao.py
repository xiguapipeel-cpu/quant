"""
个股资金流向 DAO — 从 stock_fund_flow 读取并组织成策略所需结构

主要消费者：
  backtest/bt_major_capital.py 的 fund_flow_data 参数

返回结构：
  {code: pd.DataFrame indexed by date, columns=[main_net_amount, main_net_pct, ...]}
"""

from datetime import date as _date
from typing import Iterable, Optional

import pandas as pd

from db.mysql_pool import get_pool


# ──────────────────────────────────────────────────────────────
# 批量读取
# ──────────────────────────────────────────────────────────────

async def load_fund_flow_range(
    codes: Iterable[str],
    start: str | _date,
    end: str | _date,
) -> dict[str, pd.DataFrame]:
    """
    批量读取多只股票在 [start, end] 区间的资金流向数据。

    返回 {code: DataFrame(indexed by date)}。
    某代码无数据则不在返回字典中。
    """
    codes = list(codes)
    if not codes:
        return {}

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(codes))
            await cur.execute(f"""
                SELECT code, trade_date, close_price, pct_change,
                       main_net_amount, main_net_pct,
                       super_large_net, super_large_pct,
                       large_net, large_pct,
                       medium_net, medium_pct,
                       small_net, small_pct
                FROM stock_fund_flow
                WHERE code IN ({placeholders})
                  AND trade_date >= %s
                  AND trade_date <= %s
                ORDER BY code, trade_date
            """, (*codes, start, end))
            rows = await cur.fetchall()

    if not rows:
        return {}

    cols = [
        "code", "trade_date", "close_price", "pct_change",
        "main_net_amount", "main_net_pct",
        "super_large_net", "super_large_pct",
        "large_net", "large_pct",
        "medium_net", "medium_pct",
        "small_net", "small_pct",
    ]
    df_all = pd.DataFrame(rows, columns=cols)
    # 数值列转 float（DECIMAL → Decimal，需要转）
    for c in cols[2:]:
        df_all[c] = pd.to_numeric(df_all[c], errors="coerce")

    df_all["trade_date"] = pd.to_datetime(df_all["trade_date"])

    result: dict[str, pd.DataFrame] = {}
    for code, grp in df_all.groupby("code"):
        df = grp.drop(columns=["code"]).set_index("trade_date").sort_index()
        result[str(code)] = df
    return result


# ──────────────────────────────────────────────────────────────
# 覆盖率诊断（用于报告 + 选 fold 时检查可用性）
# ──────────────────────────────────────────────────────────────

async def coverage_summary(start: str | _date, end: str | _date) -> dict:
    """
    返回 {start, end} 区间内 stock_fund_flow 的覆盖统计：
      - n_codes:        有数据的代码数
      - earliest_date:  最早 trade_date
      - latest_date:    最晚 trade_date
      - total_rows:     总行数
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT COUNT(DISTINCT code), MIN(trade_date), MAX(trade_date), COUNT(*)
                FROM stock_fund_flow
                WHERE trade_date >= %s AND trade_date <= %s
            """, (start, end))
            row = await cur.fetchone()
    return {
        "n_codes":       row[0] or 0,
        "earliest_date": row[1].isoformat() if row[1] else None,
        "latest_date":   row[2].isoformat() if row[2] else None,
        "total_rows":    row[3] or 0,
    }
